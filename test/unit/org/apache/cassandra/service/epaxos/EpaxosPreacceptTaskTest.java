package org.apache.cassandra.service.epaxos;

import com.google.common.base.Predicate;
import org.apache.cassandra.exceptions.UnavailableException;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Collections;
import java.util.UUID;

public class EpaxosPreacceptTaskTest extends AbstractEpaxosTest
{

    private static class FailureCallback implements Runnable
    {
        public boolean called = false;

        @Override
        public void run()
        {
            called = true;
        }
    }

    @Test
    public void normalCase() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);
        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));

        FailureCallback failureCallback = new FailureCallback();

        PreacceptTask task = new PreacceptTask.Leader(state, instance, failureCallback);

        Assert.assertEquals(0, instance.getBallot());
        Assert.assertEquals(Instance.State.INITIALIZED, instance.getState());

        task.run();

        Assert.assertEquals(1, instance.getBallot());
        Assert.assertEquals(Instance.State.PREACCEPTED, instance.getState());
        Assert.assertEquals(2, state.sentMessages.size());
        Assert.assertFalse(failureCallback.called);
    }

    @Test
    public void quorumFailure() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0) {
            protected Predicate<InetAddress> livePredicate()
            {
                return new Predicate<InetAddress>()
                {
                    public boolean apply(InetAddress address)
                    {
                        return false;
                    }
                };
            }
        };
        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));

        FailureCallback failureCallback = new FailureCallback();

        PreacceptTask task = new PreacceptTask.Leader(state, instance, failureCallback);

        Assert.assertEquals(0, instance.getBallot());
        Assert.assertEquals(Instance.State.INITIALIZED, instance.getState());

        try
        {
            task.run();
            Assert.fail("Expecting UnavailableException");
        }
        catch (RuntimeException e)
        {
            Assert.assertEquals(UnavailableException.class, e.getCause().getClass());
        }

        // instance should not have been preaccepted if a quorum couldn't be reached
        Assert.assertEquals(0, instance.getBallot());
        Assert.assertEquals(Instance.State.INITIALIZED, instance.getState());
        Assert.assertEquals(0, state.sentMessages.size());
        Assert.assertTrue(failureCallback.called);
    }

    @Test
    public void unexpectedInstanceState() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);
        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        instance.accept(Collections.<UUID>emptySet());
        instance.checkBallot(1);
        state.saveInstance(instance);

        FailureCallback failureCallback = new FailureCallback();

        PreacceptTask task = new PreacceptTask.Prepare(state, instance.getId(), false, failureCallback);

        Assert.assertEquals(1, instance.getBallot());
        Assert.assertEquals(Instance.State.ACCEPTED, instance.getState());

        task.run();

        Assert.assertEquals(1, instance.getBallot());
        Assert.assertEquals(Instance.State.ACCEPTED, instance.getState());
        Assert.assertEquals(0, state.sentMessages.size());
        Assert.assertTrue(failureCallback.called);
    }

    @Test
    public void noopPrepare() throws Exception
    {
        MockCallbackState state = new MockCallbackState(3, 0);
        Instance instance = state.createQueryInstance(getSerializedCQLRequest(0, 0));
        instance.preaccept(Collections.<UUID>emptySet());
        state.saveInstance(instance);

        FailureCallback failureCallback = new FailureCallback();

        PreacceptTask task = new PreacceptTask.Prepare(state, instance.getId(), true, failureCallback);

        Assert.assertEquals(0, instance.getBallot());
        Assert.assertEquals(Instance.State.PREACCEPTED, instance.getState());
        Assert.assertFalse(instance.isNoop());

        task.run();

        Assert.assertEquals(1, instance.getBallot());
        Assert.assertEquals(Instance.State.PREACCEPTED, instance.getState());
        Assert.assertTrue(instance.isNoop());
        Assert.assertEquals(2, state.sentMessages.size());
        Assert.assertFalse(failureCallback.called);
    }
}
