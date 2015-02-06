package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

public class EpaxosKeyStateManagerTest extends AbstractEpaxosTest
{
    private static final InetAddress ADDRESS;
    static
    {
        try
        {
            ADDRESS = InetAddress.getByAddress(new byte[]{(byte)192, (byte) 168, (byte) 1, (byte) 1});
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError();
        }
    }
    private static final ReplayPosition REPLAY_POS = new ReplayPosition(1, 2);

    /**
     * Loads/saves a key state, persisting an empty one to disk
     */
    private static void createKeyState(KeyStateManager ksm, ByteBuffer key, UUID cfId)
    {
        KeyState ks = ksm.loadKeyState(key, cfId);
        ksm.saveKeyState(key, cfId, ks);
    }

    private static List<ByteBuffer> getKeyList(int size)
    {
        List <ByteBuffer> bufferList = new ArrayList<>(size);
        for (int i=0; i<size; i++)
        {
            bufferList.add(ByteBufferUtil.bytes(i));
        }
        return bufferList;
    }

    private static List<UUID> getUUIDList(int size)
    {
        List <UUID> uuidList = new ArrayList<>(size);
        for (int i=0; i<size; i++)
        {
            uuidList.add(UUIDGen.getTimeUUID());
        }
        return uuidList;
    }

    private static List<CfKey> getCfKeyList(int numKeys, int numCf)
    {
        assert numKeys >= numCf;
        List<CfKey> cfKeyList = new ArrayList<>(numKeys);

        List<ByteBuffer> keys = getKeyList(numKeys);
        List<UUID> cfIds = getUUIDList(numCf);

        for (int i=0; i<numKeys; i++)
        {
            cfKeyList.add(new CfKey(keys.get(i), cfIds.get(i%numCf)));
        }

        return cfKeyList;
    }

    private void clearKeyStates()
    {
        String select = String.format("SELECT row_key FROM %s.%s", Keyspace.SYSTEM_KS, SystemKeyspace.EPAXOS_KEY_STATE);
        String delete = String.format("DELETE FROM %s.%s WHERE row_key=?", Keyspace.SYSTEM_KS, SystemKeyspace.EPAXOS_KEY_STATE);
        UntypedResultSet result = QueryProcessor.executeInternal(select);

        while (result.size() > 0)
        {
            for (UntypedResultSet.Row row: result)
            {
                QueryProcessor.executeInternal(delete, row.getBlob("row_key"));
            }
            result = QueryProcessor.executeInternal(select);
        }

        Assert.assertEquals(0, QueryProcessor.executeInternal(select).size());
    }

    private void clearTokenStates()
    {
        String select = String.format("SELECT cf_id FROM %s.%s", Keyspace.SYSTEM_KS, SystemKeyspace.EPAXOS_TOKEN_STATE);
        String delete = String.format("DELETE FROM %s.%s WHERE cf_id=?", Keyspace.SYSTEM_KS, SystemKeyspace.EPAXOS_TOKEN_STATE);
        UntypedResultSet result = QueryProcessor.executeInternal(select);

        while (result.size() > 0)
        {
            for (UntypedResultSet.Row row: result)
            {
                QueryProcessor.executeInternal(delete, row.getBlob("cf_id"));
            }
            result = QueryProcessor.executeInternal(select);
        }

        Assert.assertEquals(0, QueryProcessor.executeInternal(select).size());
    }

    @Before
    public void setUp() throws Exception
    {
        clearKeyStates();
        clearTokenStates();
    }

    @Test
    public void getCurrentQueryDependencies() throws Exception
    {
        TokenStateManager tsm = new MockTokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);

        SerializedRequest request1 = getSerializedCQLRequest(1, 1);
        SerializedRequest request2 = getSerializedCQLRequest(2, 2);

        List<CfKey> cfKeys = Lists.newArrayList(request1.getCfKey(), request2.getCfKey());
        Map<CfKey, Set<UUID>> keyDeps = new HashMap<>();
        for (CfKey cfKey: cfKeys)
        {
            Set<UUID> deps = Sets.newHashSet(getUUIDList(3));
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            for (UUID id: deps)
            {
                ks.recordInstance(id);
            }
            keyDeps.put(cfKey, deps);
            Assert.assertEquals(deps, ks.getActiveInstanceIds());

            ksm.saveKeyState(cfKey.key, cfKey.cfId, ks);
        }

        Set<UUID> expectedDeps = keyDeps.get(request1.getCfKey());
        QueryInstance instance = new QueryInstance(request1, ADDRESS);

        Set<UUID> actualDeps = ksm.getCurrentDependencies(instance);
        Assert.assertEquals(expectedDeps, actualDeps);

        // check that the instance has been added to it's own key state, but not the other
        KeyState ks1 = ksm.loadKeyState(request1.getKey(), request1.getCfKey().cfId);
        Assert.assertTrue(ks1.getActiveInstanceIds().contains(instance.getId()));
        KeyState ks2 = ksm.loadKeyState(request2.getKey(), request2.getCfKey().cfId);
        Assert.assertFalse(ks2.getActiveInstanceIds().contains(instance.getId()));
    }

    @Test
    public void getCurrentTokenDependencies() throws Exception
    {
        MockTokenStateManager tsm = new MockTokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);

        List<CfKey> cfKeys = getCfKeyList(9, 3);
        Token token = DatabaseDescriptor.getPartitioner().getToken(cfKeys.get(0).key);
        UUID cfId = cfKeys.get(0).cfId;

        Map<CfKey, Set<UUID>> keyDeps = new HashMap<>();
        Set<UUID> expectedDeps = Sets.newHashSet();
        for (CfKey cfKey: cfKeys)
        {
            Set<UUID> deps = Sets.newHashSet(getUUIDList(3));
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            for (UUID id: deps)
            {
                ks.recordInstance(id);
            }
            keyDeps.put(cfKey, deps);

            if (cfKey.cfId.equals(cfId))
            {
                expectedDeps.addAll(deps);
            }

            Assert.assertEquals(deps, ks.getActiveInstanceIds());

            ksm.saveKeyState(cfKey.key, cfKey.cfId, ks);
        }

        EpochInstance instance = new EpochInstance(ADDRESS, token, cfId, 0);

        Set<UUID> actualDeps = ksm.getCurrentDependencies(instance);
        Assert.assertEquals(expectedDeps, actualDeps);

        // check that the token instance has been added to each of the individual key states
        for (CfKey cfKey: cfKeys)
        {
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            if (cfKey.cfId.equals(cfId))
            {
                Assert.assertTrue(ks.getActiveInstanceIds().contains(instance.getId()));
            }
            else
            {
                Assert.assertFalse(ks.getActiveInstanceIds().contains(instance.getId()));
            }
        }
    }

    @Test
    public void recordMissingQueryInstance() throws Exception
    {
        TokenStateManager tsm = new MockTokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);

        SerializedRequest request1 = getSerializedCQLRequest(1, 1);
        SerializedRequest request2 = getSerializedCQLRequest(2, 2);

        QueryInstance instance = new QueryInstance(request1, ADDRESS);

        createKeyState(ksm, request1.getKey(), request1.getCfKey().cfId);
        createKeyState(ksm, request2.getKey(), request2.getCfKey().cfId);

        KeyState ks1 = ksm.loadKeyState(request1.getKey(), request1.getCfKey().cfId);
        KeyState ks2 = ksm.loadKeyState(request2.getKey(), request2.getCfKey().cfId);

        Assert.assertEquals(0, ks1.getActiveInstanceIds().size());
        Assert.assertEquals(0, ks2.getActiveInstanceIds().size());

        ksm.recordMissingInstance(instance);

        Assert.assertEquals(1, ks1.getActiveInstanceIds().size());
        Assert.assertEquals(0, ks2.getActiveInstanceIds().size());

        Assert.assertTrue(ks1.getActiveInstanceIds().contains(instance.getId()));
    }

    @Test
    public void recordMissingEpochInstance() throws Exception
    {
        TokenStateManager tsm = new MockTokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);

        List<CfKey> cfKeys = getCfKeyList(9, 3);
        for (CfKey cfKey: cfKeys)
        {
            createKeyState(ksm, cfKey.key, cfKey.cfId);
        }

        Token token = DatabaseDescriptor.getPartitioner().getToken(cfKeys.get(0).key);
        UUID cfId = cfKeys.get(0).cfId;

        EpochInstance instance = new EpochInstance(ADDRESS, token, cfId, 0);
        ksm.recordMissingInstance(instance);

        // check that the token instance has been added to each of the individual key states
        for (CfKey cfKey: cfKeys)
        {
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);

            if (cfKey.cfId.equals(cfId))
            {
                Assert.assertTrue(ks.getActiveInstanceIds().contains(instance.getId()));
            }
            else
            {
                Assert.assertFalse(ks.getActiveInstanceIds().contains(instance.getId()));
            }
        }
    }

    @Test
    public void recordAcknowledgedQueryDeps() throws Exception
    {
        TokenStateManager tsm = new MockTokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);

        SerializedRequest request1 = getSerializedCQLRequest(1, 1);
        SerializedRequest request2 = getSerializedCQLRequest(2, 2);

        // populate key states with fake dependencies
        List<CfKey> cfKeys = Lists.newArrayList(request1.getCfKey(), request2.getCfKey());
        Map<CfKey, Set<UUID>> keyDeps = new HashMap<>();
        for (CfKey cfKey: cfKeys)
        {
            Set<UUID> deps = Sets.newHashSet(getUUIDList(3));
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            for (UUID id: deps)
            {
                ks.recordInstance(id);
            }
            keyDeps.put(cfKey, deps);
            Assert.assertEquals(deps, ks.getActiveInstanceIds());

            ksm.saveKeyState(cfKey.key, cfKey.cfId, ks);
        }

        //
        QueryInstance instance = new QueryInstance(request1, ADDRESS);
        Set<UUID> deps = ksm.getCurrentDependencies(instance);
        instance.preaccept(deps);

        // check that we visit all deps
        Set<UUID> expected = new HashSet<>(deps);

        // check that none of the deps are ack'd
        for (CfKey cfKey: cfKeys)
        {
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            for (UUID id: ks.getActiveInstanceIds())
            {
                KeyState.Entry dep = ks.get(id);
                Assert.assertEquals(0, dep.acknowledged.size());
                expected.remove(id);
            }
        }

        Assert.assertEquals(0, expected.size());

        ksm.recordAcknowledgedDeps(instance);

        // check that only the expected dependencies have been ack'd
        expected = new HashSet<>(deps);
        for (CfKey cfKey: cfKeys)
        {
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            for (UUID id: ks.getActiveInstanceIds())
            {
                KeyState.Entry dep = ks.get(id);
                Assert.assertEquals(expected.contains(id), dep.acknowledged.size() > 0);
                expected.remove(id);
            }
        }
        Assert.assertEquals(0, expected.size());
    }

    @Test
    public void recordAcknowledgedTokenDeps() throws Exception
    {
        TokenStateManager tsm = new MockTokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);

        List<CfKey> cfKeys = getCfKeyList(9, 3);
        for (CfKey cfKey: cfKeys)
        {
            Set<UUID> deps = Sets.newHashSet(getUUIDList(3));
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            for (UUID id: deps)
            {
                ks.recordInstance(id);
            }
            Assert.assertEquals(deps, ks.getActiveInstanceIds());

            ksm.saveKeyState(cfKey.key, cfKey.cfId, ks);
        }

        Token token = DatabaseDescriptor.getPartitioner().getToken(cfKeys.get(0).key);
        UUID cfId = cfKeys.get(0).cfId;
        EpochInstance instance = new EpochInstance(ADDRESS, token, cfId, 0);

        Set<UUID> deps = ksm.getCurrentDependencies(instance);
        instance.preaccept(deps);

        // check that we visit all deps
        Set<UUID> expected = new HashSet<>(deps);

        // check that none of the deps are ack'd
        for (CfKey cfKey: cfKeys)
        {
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            for (UUID id: ks.getActiveInstanceIds())
            {
                KeyState.Entry dep = ks.get(id);
                Assert.assertEquals(0, dep.acknowledged.size());
                expected.remove(id);
            }
        }

        Assert.assertEquals(0, expected.size());
        ksm.recordAcknowledgedDeps(instance);

        // check that only the expected dependencies have been ack'd
        expected = new HashSet<>(deps);
        for (CfKey cfKey: cfKeys)
        {
            // FIXME: only keystates with the same cfid should be affected
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            for (UUID id: ks.getActiveInstanceIds())
            {
                KeyState.Entry dep = ks.get(id);
                Assert.assertEquals(expected.contains(id), dep.acknowledged.size() > 0);
                expected.remove(id);
            }
        }
        Assert.assertEquals(0, expected.size());
    }

    @Test
    public void recordExecutedQuery() throws Exception
    {
        TokenStateManager tsm = new MockTokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);

        SerializedRequest request1 = getSerializedCQLRequest(1, 1);
        SerializedRequest request2 = getSerializedCQLRequest(2, 2);

        // populate key states with fake dependencies
        List<CfKey> cfKeys = Lists.newArrayList(request1.getCfKey(), request2.getCfKey());
        Map<CfKey, Set<UUID>> keyDeps = new HashMap<>();
        for (CfKey cfKey: cfKeys)
        {
            Set<UUID> deps = Sets.newHashSet(getUUIDList(3));
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            for (UUID id: deps)
            {
                ks.recordInstance(id);
            }
            keyDeps.put(cfKey, deps);
            Assert.assertEquals(deps, ks.getActiveInstanceIds());

            ksm.saveKeyState(cfKey.key, cfKey.cfId, ks);
        }

        //
        QueryInstance instance = new QueryInstance(request1, ADDRESS);
        Set<UUID> deps = ksm.getCurrentDependencies(instance);
        instance.preaccept(deps);

        // check that none of the deps are ack'd
        for (CfKey cfKey: cfKeys)
        {
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            KeyState.Entry dep = ks.get(instance.getId());
            if (!cfKey.equals(request1.getCfKey()))
            {
                Assert.assertNull(dep);

            }
            else
            {
                Assert.assertFalse(dep.executed);
            }
        }

        ksm.recordExecuted(instance, REPLAY_POS);

        for (CfKey cfKey: cfKeys)
        {
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            KeyState.Entry dep = ks.get(instance.getId());
            if (!cfKey.equals(request1.getCfKey()))
            {
                Assert.assertNull(dep);

            }
            else
            {
                Assert.assertTrue(dep.executed);
            }
        }
    }

    @Test
    public void recordExecutedToken() throws Exception
    {
        TokenStateManager tsm = new MockTokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);

        List<CfKey> cfKeys = getCfKeyList(9, 3);
        for (CfKey cfKey: cfKeys)
        {
            Set<UUID> deps = Sets.newHashSet(getUUIDList(3));
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            for (UUID id: deps)
            {
                ks.recordInstance(id);
            }
            Assert.assertEquals(deps, ks.getActiveInstanceIds());

            ksm.saveKeyState(cfKey.key, cfKey.cfId, ks);
        }

        Token token = DatabaseDescriptor.getPartitioner().getToken(cfKeys.get(0).key);
        UUID cfId = cfKeys.get(0).cfId;
        EpochInstance instance = new EpochInstance(ADDRESS, token, cfId, 0);

        Set<UUID> deps = ksm.getCurrentDependencies(instance);
        instance.preaccept(deps);

        // check that none of the deps are ack'd
        for (CfKey cfKey: cfKeys)
        {
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            KeyState.Entry dep = ks.get(instance.getId());
            if (cfKey.cfId.equals(cfId))
            {
                Assert.assertFalse(dep.executed);
            }
            else
            {
                Assert.assertNull(dep);
            }
        }

        ksm.recordExecuted(instance, REPLAY_POS);

        for (CfKey cfKey: cfKeys)
        {
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            KeyState.Entry dep = ks.get(instance.getId());
            if (cfKey.cfId.equals(cfId))
            {
                Assert.assertTrue(dep.executed);
            }
            else
            {
                Assert.assertNull(dep);
            }
        }
    }

    @Test
    public void updateEpoch() throws Exception
    {
        TokenStateManager tsm = new MockTokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);

        List<CfKey> cfKeys = getCfKeyList(9, 3);
        for (CfKey cfKey: cfKeys)
        {
            createKeyState(ksm, cfKey.key, cfKey.cfId);
        }

        // check that all key states are on epoch 0
        for (CfKey cfKey: cfKeys)
        {
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            Assert.assertEquals(0, ks.getEpoch());
        }

        UUID cfId = cfKeys.get(0).cfId;
        TokenState tokenState = tsm.get(ByteBufferUtil.bytes(1), cfId);
        Assert.assertEquals((long) 0, tokenState.getEpoch());
        tokenState.setEpoch(1);

        ksm.updateEpoch(tokenState);

        for (CfKey cfKey: cfKeys)
        {
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            if (cfKey.cfId.equals(cfId))
            {
                Assert.assertEquals((long) 1, ks.getEpoch());
            }
            else
            {
                Assert.assertEquals((long) 0, ks.getEpoch());
            }
        }
    }

    @Test
    public void canIncrementEpochTrue() throws Exception
    {
        long targetEpoch = 4;
        long currentEpoch = targetEpoch - 1;

        TokenStateManager tsm = new MockTokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);
        ByteBuffer key = ByteBufferUtil.bytes(1234);
        UUID cfId = UUIDGen.getTimeUUID();
        KeyState keyState = ksm.loadKeyState(key, cfId);

        for (long i=0; i<targetEpoch; i++)
        {
            keyState.setEpoch(i);
            assert keyState.getEpoch() == i;
            for (UUID id: Lists.newArrayList(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID()))
            {
                keyState.markExecuted(id, new HashSet<UUID>(), REPLAY_POS);
                if (i == currentEpoch)
                {
                    // if this is the 'current' or previous epoch, set the dependency as active
                    keyState.recordInstance(id);
                }
            }
            assert keyState.getExecutionCount() == 2;
        }

        Assert.assertTrue(keyState.canIncrementToEpoch(targetEpoch));
        TokenState tokenState = tsm.get(key, cfId);
        Assert.assertTrue(ksm.canIncrementToEpoch(tokenState, targetEpoch));
    }

    @Test
    public void canIncrementEpochFalse() throws Exception
    {
        long targetEpoch = 4;
        long currentEpoch = targetEpoch - 1;

        TokenStateManager tsm = new MockTokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);
        ByteBuffer key = ByteBufferUtil.bytes(1234);
        UUID cfId = UUIDGen.getTimeUUID();
        KeyState keyState = ksm.loadKeyState(key, cfId);

        for (long i=0; i<targetEpoch; i++)
        {
            keyState.setEpoch(i);
            for (UUID id: Lists.newArrayList(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID()))
            {
                keyState.markExecuted(id, new HashSet<UUID>(), REPLAY_POS);
                if (i == currentEpoch - 1)
                {
                    // if this is the 'current' epoch, set the dependency as active
                    keyState.recordInstance(id);
                }
            }
        }

        Assert.assertFalse(keyState.canIncrementToEpoch(targetEpoch));
        TokenState tokenState = tsm.get(key, cfId);
        Assert.assertFalse(ksm.canIncrementToEpoch(tokenState, targetEpoch));
    }

    @Test
    public void tokenRangeIteration() throws Exception
    {
        TokenStateManager tsm = new MockTokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);

        int size = 4;
        UUID cfId = UUIDGen.getTimeUUID();

        List<ByteBuffer> keys = new ArrayList<>(size);
        List<Token> tokens = new ArrayList<>(size);
        Map<Token, ByteBuffer> tokenMap = new HashMap<>(size);
        for (int i=0; i<size; i++)
        {
            ByteBuffer key = ByteBufferUtil.bytes(i);
            Token token = DatabaseDescriptor.getPartitioner().getToken(key);

            keys.add(key);
            tokens.add(token);
            Assert.assertFalse(tokenMap.containsKey(token));
            tokenMap.put(token, key);

            KeyState ks = ksm.loadKeyState(key, cfId);
            ks.markExecuted(UUIDGen.getTimeUUID(), null, null);
            ksm.saveKeyState(key, cfId, ks);
        }

        Collections.sort(tokens);

        Token start = tokens.get(1);
        Token stop = tokens.get(2);

        Iterator<Pair<ByteBuffer, ExecutionInfo>> iter = ksm.getRangeExecutionInfo(cfId,
                                                                                   new Range<>(start, stop),
                                                                                   new ReplayPosition(0, 0));

        List<Pair<ByteBuffer, ExecutionInfo>> infos = Lists.newArrayList(iter);
        Assert.assertEquals(2, infos.size());
        Assert.assertEquals(tokenMap.get(tokens.get(1)), infos.get(0).left);
        Assert.assertEquals(tokenMap.get(tokens.get(2)), infos.get(1).left);
    }

    @Test
    public void cfKeyIterator()
    {
        final BytesToken token1 = new BytesToken(ByteBufferUtil.bytes(100));
        final BytesToken token2 = new BytesToken(ByteBufferUtil.bytes(200));
        TokenStateManager tsm = new TokenStateManager() {
            @Override
            protected Set<Token> getReplicatedTokensForCf(UUID cfId)
            {
                return Sets.<Token>newHashSet(token1, token2);
            }
            {
                setStarted();
            }
        };

        KeyStateManager ksm = new KeyStateManager(tsm);

        UUID cfid = UUIDGen.getTimeUUID();
        UUID otherCfId = UUIDGen.getTimeUUID();
        tsm.getOrInitManagedCf(cfid);

        for (int i=50; i<=250; i+=10)
        {
            ksm.loadKeyState(ByteBufferUtil.bytes(i), cfid);
            ksm.loadKeyState(ByteBufferUtil.bytes(i), otherCfId);
        }

        Iterator<CfKey> iterator = ksm.getCfKeyIterator(tsm.getExact(token2, cfid), 3);
        int numReturned = 0;
        List<Integer> keysReturned = new LinkedList<>();
        while (iterator.hasNext())
        {
            CfKey cfKey = iterator.next();
            int key = ByteBufferUtil.toInt(cfKey.key);
            Assert.assertEquals(cfid, cfKey.cfId);
            Assert.assertTrue(String.format("%s not > 100", key), key > 100);
            Assert.assertTrue(String.format("%s not <= 200", key), key <= 200);
            numReturned++;
            keysReturned.add(key);
        }
        Assert.assertEquals(keysReturned.toString(), 10, numReturned);
    }

    @Test
    public void cfKeyIteratorWrapAround()
    {
        final BytesToken token1 = new BytesToken(ByteBufferUtil.bytes(100));
        final BytesToken token2 = new BytesToken(ByteBufferUtil.bytes(200));
        TokenStateManager tsm = new TokenStateManager() {
            @Override
            protected Set<Token> getReplicatedTokensForCf(UUID cfId)
            {
                return Sets.<Token>newHashSet(token1, token2);
            }
            {
                setStarted();
            }
        };

        KeyStateManager ksm = new KeyStateManager(tsm);

        UUID cfid = UUIDGen.getTimeUUID();
        tsm.getOrInitManagedCf(cfid);

        int keysCreated = 0;
        for (int i=50; i<=250; i+=10)
        {
            ksm.loadKeyState(ByteBufferUtil.bytes(i), cfid);
            keysCreated++;
        }

        Iterator<CfKey> iterator = ksm.getCfKeyIterator(tsm.getExact(token1, cfid), 5);
        int numReturned = 0;
        List<Integer> keysReturned = new LinkedList<>();
        while (iterator.hasNext())
        {
            CfKey cfKey = iterator.next();
            int key = ByteBufferUtil.toInt(cfKey.key);
            Assert.assertTrue(String.format("%s not > 100", key), key <= 100 || key > 200);
            numReturned++;
            keysReturned.add(key);
        }
        Assert.assertEquals(11, keysCreated - 10);
        Assert.assertEquals(keysReturned.toString(), 11, numReturned);
    }

    @Test
    public void cfKeyIteratorSingleToken()
    {
        final BytesToken token1 = new BytesToken(ByteBufferUtil.bytes(100));
        TokenStateManager tsm = new TokenStateManager() {
            @Override
            protected Set<Token> getReplicatedTokensForCf(UUID cfId)
            {
                return Sets.<Token>newHashSet(token1);
            }
            {
                setStarted();
            }
        };

        KeyStateManager ksm = new KeyStateManager(tsm);

        UUID cfid = UUIDGen.getTimeUUID();
        tsm.getOrInitManagedCf(cfid);

        int keysCreated = 0;
        for (int i=50; i<=250; i+=10)
        {
            ksm.loadKeyState(ByteBufferUtil.bytes(i), cfid);
            keysCreated++;
        }

        Iterator<CfKey> iterator = ksm.getCfKeyIterator(tsm.getExact(token1, cfid), 5);
        int numReturned = 0;
        List<Integer> keysReturned = new LinkedList<>();
        while (iterator.hasNext())
        {
            CfKey cfKey = iterator.next();
            int key = ByteBufferUtil.toInt(cfKey.key);
            numReturned++;
            keysReturned.add(key);
        }
        Assert.assertEquals(21, keysCreated);
        Assert.assertEquals(keysReturned.toString(), 21, numReturned);
    }
}
