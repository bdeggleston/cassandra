package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.utils.ByteBufferUtil;
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
    }

    @Test
    public void getCurrentQueryDependencies() throws Exception
    {
        TokenStateManager tsm = new TokenStateManager();
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
                ks.create(id);
            }
            keyDeps.put(cfKey, deps);
            Assert.assertEquals(deps, ks.getDeps());

            ksm.saveKeyState(cfKey.key, cfKey.cfId, ks);
        }

        Set<UUID> expectedDeps = keyDeps.get(request1.getCfKey());
        QueryInstance instance = new QueryInstance(request1, ADDRESS);

        Set<UUID> actualDeps = ksm.getCurrentDependencies(instance);
        Assert.assertEquals(expectedDeps, actualDeps);

        // check that the instance has been added to it's own key state, but not the other
        KeyState ks1 = ksm.loadKeyState(request1.getKey(), request1.getCfKey().cfId);
        Assert.assertTrue(ks1.getDeps().contains(instance.getId()));
        KeyState ks2 = ksm.loadKeyState(request2.getKey(), request2.getCfKey().cfId);
        Assert.assertFalse(ks2.getDeps().contains(instance.getId()));

        // TODO: check token bounds
    }

    @Test
    public void getCurrentTokenDependencies() throws Exception
    {
        TokenStateManager tsm = new TokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);

        List<CfKey> cfKeys = getCfKeyList(9, 3);
        Map<CfKey, Set<UUID>> keyDeps = new HashMap<>();
        for (CfKey cfKey: cfKeys)
        {
            Set<UUID> deps = Sets.newHashSet(getUUIDList(3));
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            for (UUID id: deps)
            {
                ks.create(id);
            }
            keyDeps.put(cfKey, deps);
            Assert.assertEquals(deps, ks.getDeps());

            ksm.saveKeyState(cfKey.key, cfKey.cfId, ks);
        }

        Set<UUID> expectedDeps = Sets.newHashSet(Iterables.concat(keyDeps.values()));
        TokenInstance instance = new TokenInstance(ADDRESS, new LongToken((long)0), 0);

        Set<UUID> actualDeps = ksm.getCurrentDependencies(instance);
        Assert.assertEquals(expectedDeps, actualDeps);

        // check that the token instance has been added to each of the individual key states
        for (CfKey cfKey: cfKeys)
        {
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            Assert.assertTrue(ks.getDeps().contains(instance.getId()));
        }

        // TODO: check token bounds
    }

    @Test
    public void recordMissingQueryInstance() throws Exception
    {
        TokenStateManager tsm = new TokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);

        SerializedRequest request1 = getSerializedCQLRequest(1, 1);
        SerializedRequest request2 = getSerializedCQLRequest(2, 2);

        QueryInstance instance = new QueryInstance(request1, ADDRESS);

        createKeyState(ksm, request1.getKey(), request1.getCfKey().cfId);
        createKeyState(ksm, request2.getKey(), request2.getCfKey().cfId);

        KeyState ks1 = ksm.loadKeyState(request1.getKey(), request1.getCfKey().cfId);
        KeyState ks2 = ksm.loadKeyState(request2.getKey(), request2.getCfKey().cfId);

        Assert.assertEquals(0, ks1.getDeps().size());
        Assert.assertEquals(0, ks2.getDeps().size());

        ksm.recordMissingInstance(instance);

        Assert.assertEquals(1, ks1.getDeps().size());
        Assert.assertEquals(0, ks2.getDeps().size());

        Assert.assertTrue(ks1.getDeps().contains(instance.getId()));
    }

    @Test
    public void recordMissingTokenInstance() throws Exception
    {
        TokenStateManager tsm = new TokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);

        List<CfKey> cfKeys = getCfKeyList(9, 3);
        for (CfKey cfKey: cfKeys)
        {
            createKeyState(ksm, cfKey.key, cfKey.cfId);
        }
        TokenInstance instance = new TokenInstance(ADDRESS, new LongToken((long)0), 0);
        ksm.recordMissingInstance(instance);

        // check that the token instance has been added to each of the individual key states
        for (CfKey cfKey: cfKeys)
        {
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            Assert.assertTrue(ks.getDeps().contains(instance.getId()));
        }
    }

    @Test
    public void recordAcknowledgedQueryDeps() throws Exception
    {
        TokenStateManager tsm = new TokenStateManager();
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
                ks.create(id);
            }
            keyDeps.put(cfKey, deps);
            Assert.assertEquals(deps, ks.getDeps());

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
            for (UUID id: ks.getDeps())
            {
                KeyState.Entry dep = ks.get(id);
                Assert.assertFalse(dep.acknowledged);
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
            for (UUID id: ks.getDeps())
            {
                KeyState.Entry dep = ks.get(id);
                Assert.assertEquals(expected.contains(id), dep.acknowledged);
                expected.remove(id);
            }
        }
        Assert.assertEquals(0, expected.size());
    }

    @Test
    public void recordAcknowledgedTokenDeps() throws Exception
    {
        TokenStateManager tsm = new TokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);

        List<CfKey> cfKeys = getCfKeyList(9, 3);
        for (CfKey cfKey: cfKeys)
        {
            Set<UUID> deps = Sets.newHashSet(getUUIDList(3));
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            for (UUID id: deps)
            {
                ks.create(id);
            }
            Assert.assertEquals(deps, ks.getDeps());

            ksm.saveKeyState(cfKey.key, cfKey.cfId, ks);
        }

        TokenInstance instance = new TokenInstance(ADDRESS, new LongToken((long)0), 0);

        Set<UUID> deps = ksm.getCurrentDependencies(instance);
        instance.preaccept(deps);

        // check that we visit all deps
        Set<UUID> expected = new HashSet<>(deps);

        // check that none of the deps are ack'd
        for (CfKey cfKey: cfKeys)
        {
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            for (UUID id: ks.getDeps())
            {
                KeyState.Entry dep = ks.get(id);
                Assert.assertFalse(dep.acknowledged);
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
            for (UUID id: ks.getDeps())
            {
                KeyState.Entry dep = ks.get(id);
                Assert.assertEquals(expected.contains(id), dep.acknowledged);
                expected.remove(id);
            }
        }
        Assert.assertEquals(0, expected.size());
    }

    @Test
    public void recordExecutedQuery() throws Exception
    {
        TokenStateManager tsm = new TokenStateManager();
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
                ks.create(id);
            }
            keyDeps.put(cfKey, deps);
            Assert.assertEquals(deps, ks.getDeps());

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

        ksm.recordExecuted(instance);

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
        TokenStateManager tsm = new TokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);

        List<CfKey> cfKeys = getCfKeyList(9, 3);
        for (CfKey cfKey: cfKeys)
        {
            Set<UUID> deps = Sets.newHashSet(getUUIDList(3));
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            for (UUID id: deps)
            {
                ks.create(id);
            }
            Assert.assertEquals(deps, ks.getDeps());

            ksm.saveKeyState(cfKey.key, cfKey.cfId, ks);
        }

        TokenInstance instance = new TokenInstance(ADDRESS, new LongToken((long)0), 0);

        Set<UUID> deps = ksm.getCurrentDependencies(instance);
        instance.preaccept(deps);

        // check that none of the deps are ack'd
        for (CfKey cfKey: cfKeys)
        {
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            KeyState.Entry dep = ks.get(instance.getId());
            Assert.assertFalse(dep.executed);
        }

        ksm.recordExecuted(instance);

        for (CfKey cfKey: cfKeys)
        {
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            KeyState.Entry dep = ks.get(instance.getId());
            Assert.assertTrue(dep.executed);
        }
    }

    @Test
    public void updateEpoch() throws Exception
    {
        TokenStateManager tsm = new TokenStateManager();
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

        TokenState tokenState = tsm.get(ByteBufferUtil.bytes(1));
        Assert.assertEquals((long) 0, tokenState.getEpoch());
        tokenState.setEpoch(1);

        ksm.updateEpoch(tokenState);

        for (CfKey cfKey: cfKeys)
        {
            KeyState ks = ksm.loadKeyState(cfKey.key, cfKey.cfId);
            Assert.assertEquals((long) 1, ks.getEpoch());
        }

        // TODO: check token bounds
    }

    @Test
    public void canIncrementEpochTrue() throws Exception
    {
        long targetEpoch = 4;
        long currentEpoch = targetEpoch - 1;

        TokenStateManager tsm = new TokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);
        ByteBuffer key = ByteBufferUtil.bytes(1234);
        KeyState keyState = ksm.loadKeyState(key, UUIDGen.getTimeUUID());

        for (long i=0; i<targetEpoch; i++)
        {
            keyState.setEpoch(i);
            assert keyState.getEpoch() == i;
            for (UUID id: Lists.newArrayList(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID()))
            {
                keyState.markExecuted(id);
                if (i == currentEpoch)
                {
                    // if this is the 'current' or previous epoch, set the dependency as active
                    keyState.create(id);
                }
            }
            assert keyState.getExecutionCount() == 2;
        }

        Assert.assertTrue(keyState.canIncrementToEpoch(targetEpoch));
        TokenState tokenState = tsm.get(key);
        Assert.assertTrue(ksm.canIncrementToEpoch(tokenState, targetEpoch));
    }

    @Test
    public void canIncrementEpochFalse() throws Exception
    {
        long targetEpoch = 4;
        long currentEpoch = targetEpoch - 1;

        TokenStateManager tsm = new TokenStateManager();
        KeyStateManager ksm = new KeyStateManager(tsm);
        ByteBuffer key = ByteBufferUtil.bytes(1234);
        KeyState keyState = ksm.loadKeyState(key, UUIDGen.getTimeUUID());

        for (long i=0; i<targetEpoch; i++)
        {
            keyState.setEpoch(i);
            for (UUID id: Lists.newArrayList(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID()))
            {
                keyState.markExecuted(id);
                if (i == currentEpoch - 1)
                {
                    // if this is the 'current' epoch, set the dependency as active
                    keyState.create(id);
                }
            }
        }

        Assert.assertFalse(keyState.canIncrementToEpoch(targetEpoch));
        TokenState tokenState = tsm.get(key);
        Assert.assertTrue(ksm.canIncrementToEpoch(tokenState, targetEpoch));
    }
}
