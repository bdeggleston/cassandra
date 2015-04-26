package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;

public class EpaxosFailureRecoveryTest extends AbstractEpaxosTest
{
    @Before
    public void setUp()
    {
        clearKeyStates();
        clearTokenStates();
    }

    @Test
    public void preRecover() throws Exception
    {
        final Set<UUID> deleted = Sets.newHashSet();

        EpaxosState state = new MockVerbHandlerState(){

            @Override
            void deleteInstance(UUID id)
            {
                deleted.add(id);
            }
        };

        final TokenState tokenState = new TokenState(TOKEN, CFID, 2, 5);
        Assert.assertEquals(TokenState.State.NORMAL, tokenState.getState());
        Assert.assertFalse(state.managesCfId(CFID));

        // make a bunch of keys & instance ids
        final Set<ByteBuffer> keys = Sets.newHashSet();
        Set<UUID> expectedIds = Sets.newHashSet();

        for (int i=0; i<10; i++)
        {
            ByteBuffer key = ByteBufferUtil.bytes(i);
            keys.add(key);

            KeyState ks = state.keyStateManager.loadKeyState(key, CFID);
            UUID prev = null;
            for (int j=0; j<9; j++)
            {
                UUID id = UUIDGen.getTimeUUID();
                expectedIds.add(id);
                ks.getDepsAndAdd(id);
                switch (j%3)
                {
                    case 2:
                        ks.markExecuted(id, null, new ReplayPosition(0, 0), 0);
                    case 1:
                        ks.markAcknowledged(Sets.newHashSet(id), prev);
                }
                prev = id;
            }

            Assert.assertTrue(ks.getActiveInstanceIds().size() > 0);
            Assert.assertTrue(ks.getEpochExecutions().size() > 0);
        }

        FailureRecoveryTask task = new FailureRecoveryTask(state, TOKEN, CFID, 3)
        {
            @Override
            protected TokenState getTokenState()
            {
                return tokenState;
            }
        };

        Assert.assertTrue(state.managesCfId(CFID));
        Assert.assertTrue(deleted.isEmpty());
        for (ByteBuffer key: keys)
        {
            Assert.assertTrue(state.keyStateManager.managesKey(key, CFID));
        }

        task.preRecover();

        // all of the instances and key states should have been deleted
        Assert.assertEquals(TokenState.State.PRE_RECOVERY, tokenState.getState());
        Assert.assertEquals(expectedIds, deleted);
        for (ByteBuffer key: keys)
        {
            Assert.assertFalse(state.keyStateManager.managesKey(key, CFID));
        }
    }

    @Test
    public void preRecoverBailsIfNotBehindRemoteEpoch()
    {
        EpaxosState state = new MockVerbHandlerState();
        TokenState tokenState = state.tokenStateManager.get(TOKEN, CFID);
        tokenState.setEpoch(2);
        state.tokenStateManager.save(tokenState);
        Assert.assertEquals(TokenState.State.NORMAL, tokenState.getState());
        FailureRecoveryTask task = new FailureRecoveryTask(state, TOKEN, CFID, 2);

        task.preRecover();
        Assert.assertEquals(TokenState.State.NORMAL, tokenState.getState());
    }


    @Test
    public void preRecoverAlwaysContinuesIfTokenStateIsNotNormal()
    {

        EpaxosState state = new MockVerbHandlerState();
        TokenState tokenState = state.tokenStateManager.get(TOKEN, CFID);
        tokenState.setEpoch(2);
        tokenState.setState(TokenState.State.RECOVERY_REQUIRED);
        FailureRecoveryTask task = new FailureRecoveryTask(state, TOKEN, CFID, 0);

        task.preRecover();
        Assert.assertEquals(TokenState.State.PRE_RECOVERY, tokenState.getState());
    }
}
