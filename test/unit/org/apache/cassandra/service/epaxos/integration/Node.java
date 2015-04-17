package org.apache.cassandra.service.epaxos.integration;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.TracingAwareExecutorService;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.epaxos.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;

public class Node extends EpaxosState
{
    private final InetAddress endpoint;
    private final Messenger messenger;
    private volatile State state;

    private final Map<MessagingService.Verb, IVerbHandler> verbHandlerMap = Maps.newEnumMap(MessagingService.Verb.class);

    public static enum State {UP, NORESPONSE, DOWN}

    private volatile Instance lastCreatedInstance = null;
    private static final List<InetAddress> NO_ENDPOINTS = ImmutableList.of();

    public final List<UUID> executionOrder = Lists.newLinkedList();

    public volatile Runnable preAcceptHook = null;
    public volatile Runnable preCommitHook = null;
    public final Set<UUID> accepted = Sets.newConcurrentHashSet();

    public final int number;

    public Node(int number, Messenger messenger)
    {
        this.number = number;
        try
        {
            endpoint = InetAddress.getByAddress(ByteBufferUtil.bytes(number).array());
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
        this.messenger = messenger;
        state = State.UP;

        verbHandlerMap.put(MessagingService.Verb.EPAXOS_PREACCEPT, getPreacceptVerbHandler());
        verbHandlerMap.put(MessagingService.Verb.EPAXOS_ACCEPT, getAcceptVerbHandler());
        verbHandlerMap.put(MessagingService.Verb.EPAXOS_COMMIT, getCommitVerbHandler());
        verbHandlerMap.put(MessagingService.Verb.EPAXOS_PREPARE, getPrepareVerbHandler());
        verbHandlerMap.put(MessagingService.Verb.EPAXOS_TRYPREACCEPT, getTryPreacceptVerbHandler());
    }

    @Override
    protected TokenStateManager createTokenStateManager()
    {
        return new MockTokenStateManager(keyspace(), tokenStateTable());
    }

    public State getState()
    {
        return state;
    }

    public void setState(State state)
    {
        this.state = state;
    }

    @Override
    public InetAddress getEndpoint()
    {
        return endpoint;
    }

    @Override
    protected QueryInstance createQueryInstance(SerializedRequest request)
    {
        QueryInstance instance = super.createQueryInstance(request);
        lastCreatedInstance = instance;
        return instance;
    }

    @Override
    public EpochInstance createEpochInstance(Token token, UUID cfId, long epoch)
    {
        EpochInstance instance = super.createEpochInstance(token, cfId, epoch);
        lastCreatedInstance = instance;
        return instance;
    }

    public Instance getLastCreatedInstance()
    {
        return lastCreatedInstance;
    }

    public Instance getInstance(UUID iid)
    {
        return loadInstance(iid);
    }

    public KeyState getKeyState(Instance instance)
    {
        if (instance instanceof QueryInstance)
        {
            SerializedRequest request = ((QueryInstance) instance).getQuery();
            return keyStateManager.loadKeyState(request.getKey(), Schema.instance.getId(request.getKeyspaceName(), request.getCfName()));
        }
        else if (instance instanceof EpochInstance)
        {
            throw new AssertionError();
        }
        else
        {
            throw new IllegalArgumentException("Unsupported instance type: " + instance.getClass().getName());
        }
    }

    @Override
    protected String keyspace()
    {
        throw new UnsupportedOperationException("override in concrete implementation");
    }

    @Override
    protected String instanceTable()
    {
        throw new UnsupportedOperationException("override in concrete implementation");
    }

    @Override
    protected String keyStateTable()
    {
        throw new UnsupportedOperationException("override in concrete implementation");
    }

    @Override
    protected String tokenStateTable()
    {
        return String.format("%s_%s", super.tokenStateTable(), number);
    }

    @Override
    public void accept(UUID iid, AcceptDecision decision, Runnable failureCallback)
    {
        if (preAcceptHook != null)
            preAcceptHook.run();
        accepted.add(iid);
        super.accept(iid, decision, failureCallback);
    }

    @Override
    public void commit(UUID iid, Set<UUID> deps)
    {
        if (preCommitHook != null)
            preCommitHook.run();
        super.commit(iid, deps);
    }

    @Override
    public ReplayPosition executeInstance(Instance instance) throws InvalidRequestException, ReadTimeoutException, WriteTimeoutException
    {
        ReplayPosition rp = super.executeInstance(instance);
        executionOrder.add(instance.getId());
        return rp;
    }

    @Override
    protected ParticipantInfo getParticipants(Instance instance)
    {
        return new ParticipantInfo(messenger.getEndpoints(getEndpoint()), NO_ENDPOINTS, instance.getConsistencyLevel());
    }

    @Override
    protected void sendOneWay(MessageOut message, InetAddress to)
    {
        messenger.sendOneWay(message, endpoint, to);
    }

    @Override
    protected int sendRR(MessageOut message, InetAddress to, IAsyncCallback cb)
    {
        return messenger.sendRR(message, endpoint, to, cb);
    }

    @Override
    protected void sendReply(MessageOut message, int id, InetAddress to)
    {
        messenger.sendReply(message, id, endpoint, to);
    }

    @Override
    protected Predicate<InetAddress> livePredicate()
    {
        return new Predicate<InetAddress>()
        {
            @Override
            public boolean apply(InetAddress inetAddress)
            {
                return true;
            }
        };
    }

    @Override
    public String toString()
    {
        return "Node{" +
                "endpoint=" + endpoint +
                ", state=" + state +
                ", number=" + number +
                '}';
    }

    public TokenStateMaintenanceTask newTokenStateMaintenanceTask()
    {
        return new TokenStateMaintenanceTask(this, tokenStateManager) {
            // TODO: de-override for token splitting tests
            @Override
            protected boolean replicatesTokenForKeyspace(Token token, UUID cfId)
            {
                return true;
            }

            @Override
            protected boolean shouldRun()
            {
                return true;
            }
        };
    }

    public void setEpochIncrementThreshold(int threshold)
    {
        ((MockTokenStateManager) tokenStateManager).epochIncrementThreshold = threshold;
    }

    /**
     * runs tasks in the order they're received
     */
    public static TracingAwareExecutorService queuedExecutor = new AbstractExecutorService()
    {
        private Queue<Runnable> queue = new LinkedTransferQueue<>();

        private synchronized void maybeRun(Runnable runnable)
        {
            boolean wasEmpty = queue.isEmpty();
            queue.add(runnable);
            if (wasEmpty)
            {
                while (!queue.isEmpty())
                {
                    queue.peek().run();  // prevents the next added task thinking it should run
                    queue.remove();
                }
            }
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result)
        {
            maybeRun(task);
            return null;
        }

        @Override
        public Future<?> submit(Runnable task)
        {
            maybeRun(task);
            return null;
        }
    };

    public static class SingleThreaded extends Node
    {
        public SingleThreaded(int number, Messenger messenger)
        {
            super(number, messenger);
        }

        @Override
        protected long getQueryTimeout(long start)
        {
            return 0;
        }

        @Override
        protected long getPrepareWaitTime(long lastUpdate)
        {
            return 0;
        }

        @Override
        public TracingAwareExecutorService getStage(Stage stage)
        {
            return queuedExecutor;
        }
    }
}
