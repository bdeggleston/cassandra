package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
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

    @Before
    public void setUp() throws Exception
    {
        clearKeyStates();
        clearTokenStates();
        Assert.assertTrue(DatabaseDescriptor.getPartitioner() instanceof ByteOrderedPartitioner);
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
        QueryInstance instance = new QueryInstance(request1, ADDRESS, "DC1");

        Set<UUID> actualDeps = ksm.getCurrentDependencies(instance).left;
        Assert.assertEquals(expectedDeps, actualDeps);

        // check that the instance has been added to it's own key state, but not the other
        KeyState ks1 = ksm.loadKeyState(request1.getKey(), request1.getCfKey().cfId);
        Assert.assertTrue(ks1.getActiveInstanceIds().contains(instance.getId()));
        KeyState ks2 = ksm.loadKeyState(request2.getKey(), request2.getCfKey().cfId);
        Assert.assertFalse(ks2.getActiveInstanceIds().contains(instance.getId()));
    }

    @Test
    public void getCurrentEpochDependencies() throws Exception
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

        EpochInstance instance = new EpochInstance(ADDRESS, "DC1", token, cfId, 0, false);

        Set<UUID> actualDeps = ksm.getCurrentDependencies(instance).left;
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

        QueryInstance instance = new QueryInstance(request1, ADDRESS, "DC1");

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

        EpochInstance instance = new EpochInstance(ADDRESS, "DC1", token, cfId, 0, false);
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
        QueryInstance instance = new QueryInstance(request1, ADDRESS, "DC1");
        Set<UUID> deps = ksm.getCurrentDependencies(instance).left;
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
    public void recordAcknowledgedEpochDeps() throws Exception
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
        EpochInstance instance = new EpochInstance(ADDRESS, "DC1", token, cfId, 0, false);

        Set<UUID> deps = ksm.getCurrentDependencies(instance).left;
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
        MockTokenStateManager tsm = new MockTokenStateManager();

        Token t100 = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(100));
        Token t200 = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(200));
        tsm.setTokens(TOKEN0, t200);

        KeyStateManager ksm = new KeyStateManager(tsm);

        ByteBuffer k50 = ByteBufferUtil.bytes(50);
        ByteBuffer k150 = ByteBufferUtil.bytes(150);

        Set<UUID> expectedDeps = Sets.newHashSet();

        // record keys on either side of the to-be-added token
        KeyState ks;
        ks = ksm.loadKeyState(k50, CFID);
        UUID id50 = UUIDGen.getTimeUUID();

        ks.recordInstance(id50);
        expectedDeps.add(id50);
        ksm.saveKeyState(k50, CFID, ks);
        Assert.assertEquals(1, ks.getActiveInstanceIds().size());

        ks = ksm.loadKeyState(k150, CFID);
        UUID id150 = UUIDGen.getTimeUUID();

        ks.recordInstance(id150);
        expectedDeps.add(id150);
        ksm.saveKeyState(k150, CFID, ks);
        Assert.assertEquals(1, ks.getActiveInstanceIds().size());

        TokenInstance instance = new TokenInstance(ADDRESS, "DC1", CFID, t100, range(TOKEN0, t200), false);
        Set<UUID> deps = ksm.getCurrentDependencies(instance).left;
        Assert.assertEquals(expectedDeps, deps);
        instance.preaccept(deps);

        // add token state at 100, otherwise we won't
        // excercise the range modification code
        tsm.setTokens(TOKEN0, t100, t200);
        tsm.get(t100, CFID);
        TokenState ts = new TokenState(range(TOKEN0, t100), CFID, 0, 0);
        ts.setCreatorToken(t200);
        ts.recordTokenInstance(t100, instance.getId());
        tsm.putState(ts);
        Assert.assertEquals(2, tsm.getManagedTokensForCf(CFID).size());

        // acknowledge instance
        Assert.assertEquals(0, ts.getCurrentEpochInstances().size());
        ksm.recordAcknowledgedDeps(instance);
        Assert.assertEquals(1, ts.getCurrentEpochInstances().size());

        KeyState.Entry entry;
        ks = ksm.loadKeyState(k50, CFID);
        entry = ks.get(id50);
        Assert.assertTrue(entry.acknowledged.contains(instance.getId()));

        ks = ksm.loadKeyState(k150, CFID);
        entry = ks.get(id150);
        Assert.assertTrue(entry.acknowledged.contains(instance.getId()));
    }

    @Test
    public void recordExecutedQueryInstance() throws Exception
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
        QueryInstance instance = new QueryInstance(request1, ADDRESS, "DC1");
        Set<UUID> deps = ksm.getCurrentDependencies(instance).left;
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

        Assert.assertEquals(KeyState.MIN_TIMESTAMP, ksm.getMaxTimestamp(instance.getQuery().getCfKey()));
        ksm.recordExecuted(instance, REPLAY_POS, 5);
        Assert.assertEquals(5, ksm.getMaxTimestamp(instance.getQuery().getCfKey()));

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
    public void recordExecutedEpochInstance() throws Exception
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
        EpochInstance instance = new EpochInstance(ADDRESS, "DC1", token, cfId, 0, false);

        Set<UUID> deps = ksm.getCurrentDependencies(instance).left;
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

        ksm.recordExecuted(instance, REPLAY_POS, KeyState.MIN_TIMESTAMP);

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
    public void recordExecutedTokenInstance() throws Exception
    {
        MockTokenStateManager tsm = new MockTokenStateManager();

        Token t100 = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(100));
        Token t200 = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(200));
        tsm.setTokens(TOKEN0, t200);

        KeyStateManager ksm = new KeyStateManager(tsm);

        ByteBuffer k50 = ByteBufferUtil.bytes(50);
        ByteBuffer k150 = ByteBufferUtil.bytes(150);

        Set<UUID> expectedDeps = Sets.newHashSet();

        // record keys on either side of the to-be-added token
        KeyState ks;
        ks = ksm.loadKeyState(k50, CFID);
        UUID id50 = UUIDGen.getTimeUUID();

        ks.recordInstance(id50);
        expectedDeps.add(id50);
        ksm.saveKeyState(k50, CFID, ks);
        Assert.assertEquals(1, ks.getActiveInstanceIds().size());

        ks = ksm.loadKeyState(k150, CFID);
        UUID id150 = UUIDGen.getTimeUUID();

        ks.recordInstance(id150);
        expectedDeps.add(id150);
        ksm.saveKeyState(k150, CFID, ks);
        Assert.assertEquals(1, ks.getActiveInstanceIds().size());

        TokenInstance instance = new TokenInstance(ADDRESS, "DC1", CFID, t100, range(TOKEN0, t200), false);
        Set<UUID> deps = ksm.getCurrentDependencies(instance).left;
        Assert.assertEquals(expectedDeps, deps);
        instance.preaccept(deps);

        // add token state at 100, otherwise we won't
        // excercise the range modification code
        tsm.setTokens(TOKEN0, t100, t200);
        tsm.get(t100, CFID);
        TokenState ts = new TokenState(range(TOKEN0, t100), CFID, 0, 0);
        ts.setCreatorToken(t200);
        ts.recordTokenInstance(t100, instance.getId());
        tsm.putState(ts);
        Assert.assertEquals(2, tsm.getManagedTokensForCf(CFID).size());

        // acknowledge instance
        Assert.assertEquals(0, ts.getCurrentEpochInstances().size());
        ksm.recordExecuted(instance, null, KeyState.MIN_TIMESTAMP);
        Assert.assertEquals(1, ts.getCurrentEpochInstances().size());

        KeyState.Entry entry;
        ks = ksm.loadKeyState(k50, CFID);
        Assert.assertTrue(ks.get(instance.getId()).executed);

        ks = ksm.loadKeyState(k150, CFID);
        Assert.assertTrue(ks.get(instance.getId()).executed);
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
                keyState.markExecuted(id, new HashSet<UUID>(), REPLAY_POS, KeyState.MIN_TIMESTAMP);
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
                keyState.markExecuted(id, new HashSet<UUID>(), REPLAY_POS, KeyState.MIN_TIMESTAMP);
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
            ks.markExecuted(UUIDGen.getTimeUUID(), null, null, KeyState.MIN_TIMESTAMP);
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
            protected Set<Range<Token>> getReplicatedRangesForCf(UUID cfId)
            {
                Set<Range<Token>> ranges = new HashSet<>();
                ranges.add(range(TOKEN0, token1));
                ranges.add(range(token1, token2));
                return ranges;
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
            protected Set<Range<Token>> getReplicatedRangesForCf(UUID cfId)
            {
                Set<Range<Token>> ranges = new HashSet<>();
                ranges.add(range(token2, token1));
                return ranges;
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
            protected Set<Range<Token>> getReplicatedRangesForCf(UUID cfId)
            {
                Set<Range<Token>> ranges = new HashSet<>();
                ranges.add(range(token1, token1));
                return ranges;
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

    /**
     * Tests that if a key state's epoch is behind the token manager
     * on key state load. Either because of processing time on the epoch
     * increment, or because of a failure during the increment process,
     * that it's incremented before being returned.
     */
    @Test
    public void catchUpKeyStateEpoch()
    {
        TokenStateManager tsm = new MockTokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);

        ByteBuffer key = ByteBufferUtil.bytes(0);

        TokenState ts = tsm.get(key, CFID);
        ts.setEpoch(5);
        tsm.save(ts);

        KeyState ks = ksm.loadKeyState(key, CFID);
        Assert.assertEquals(5, ks.getEpoch());

        ts.setEpoch(6);
        ks = ksm.loadKeyState(key, CFID);
        Assert.assertEquals(6, ks.getEpoch());
    }
}
