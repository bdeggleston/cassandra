package org.apache.cassandra.service.epaxos.integration;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.TracingAwareExecutorService;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
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
    public final String ksName;

    public Node(int number, String ksName, Messenger messenger)
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
        this.ksName = ksName;
        this.messenger = messenger;
        state = State.UP;

        verbHandlerMap.put(MessagingService.Verb.EPAXOS_PREACCEPT, getPreacceptVerbHandler());
        verbHandlerMap.put(MessagingService.Verb.EPAXOS_ACCEPT, getAcceptVerbHandler());
        verbHandlerMap.put(MessagingService.Verb.EPAXOS_COMMIT, getCommitVerbHandler());
        verbHandlerMap.put(MessagingService.Verb.EPAXOS_PREPARE, getPrepareVerbHandler());
        verbHandlerMap.put(MessagingService.Verb.EPAXOS_TRYPREACCEPT, getTryPreacceptVerbHandler());
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
    protected Instance createInstance(SerializedRequest request)
    {
        Instance instance = super.createInstance(request);
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

    public DependencyManager getDependencyManager(Instance instance)
    {
        SerializedRequest request = instance.getQuery();
        return loadDependencyManager(request.getKey(), Schema.instance.getId(request.getKeyspaceName(), request.getCfName()));
    }

    @Override
    protected String keyspace()
    {
        return ksName != null ? ksName : super.keyspace();
    }

    @Override
    protected String instanceTable()
    {
        return String.format("%s_%s", super.instanceTable(), number);
    }

    @Override
    protected String dependencyTable()
    {
        return String.format("%s_%s", super.dependencyTable(), number);
    }

    @Override
    protected String stateTable()
    {
        return String.format("%s_%s", super.stateTable(), number);
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
    protected void executeInstance(Instance instance) throws InvalidRequestException, ReadTimeoutException, WriteTimeoutException
    {
        super.executeInstance(instance);
        executionOrder.add(instance.getId());
    }

    @Override
    protected ParticipantInfo getParticipants(Instance instance) throws UnavailableException
    {
        return new ParticipantInfo(messenger.getEndpoints(getEndpoint()), NO_ENDPOINTS, instance.getQuery().getConsistencyLevel());
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
        public SingleThreaded(int number, String ksName, Messenger messenger)
        {
            super(number, ksName, messenger);
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
