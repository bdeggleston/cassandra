package org.apache.cassandra.service.epaxos.integration;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.epaxos.*;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class Node extends EpaxosManager
{
    private final InetAddress endpoint;
    private final Messenger messenger;
    private volatile State state;

    private final Map<MessagingService.Verb, IVerbHandler> verbHandlerMap = Maps.newEnumMap(MessagingService.Verb.class);

    public static enum State {UP, NORESPONSE, DOWN;}

    private volatile Instance lastCreatedInstance = null;
    private static final List<InetAddress> NO_ENDPOINTS = ImmutableList.of();

    public final List<UUID> executionOrder = Lists.newLinkedList();

    public volatile Runnable postPreacceptHook = null;
    public volatile Runnable postAcceptHook = null;
    public volatile Runnable postCommitHook = null;
    public final Set<UUID> accepted = Sets.newConcurrentHashSet();

    public Node(InetAddress endpoint, Messenger messenger)
    {
        this.endpoint = endpoint;
        this.messenger = messenger;
        state = State.UP;

        verbHandlerMap.put(MessagingService.Verb.PREACCEPT_REQUEST, getPreacceptVerbHandler());
        verbHandlerMap.put(MessagingService.Verb.ACCEPT_REQUEST, getAcceptVerbHandler());
        verbHandlerMap.put(MessagingService.Verb.COMMIT_REQUEST, getCommitVerbHandler());
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

    @Override
    public AcceptDecision preaccept(Instance instance) throws UnavailableException, InvalidInstanceStateChange, WriteTimeoutException, BallotException
    {
        AcceptDecision decision = super.preaccept(instance);
        if (postPreacceptHook != null)
            postPreacceptHook.run();
        return decision;
    }

    @Override
    public void accept(Instance instance, AcceptDecision decision) throws InvalidInstanceStateChange, UnavailableException, WriteTimeoutException, BallotException
    {
        super.accept(instance, decision);
        accepted.add(instance.getId());
        if (postAcceptHook != null)
            postAcceptHook.run();
    }

    @Override
    public void commit(Instance instance) throws InvalidInstanceStateChange, UnavailableException
    {
        super.commit(instance);
        if (postCommitHook != null)
            postCommitHook.run();
    }

    @Override
    protected void executeInstance(Instance instance)
    {
        super.executeInstance(instance);
        executionOrder.add(instance.getId());
    }

    @Override
    protected ParticipantInfo getParticipants(Instance instance) throws UnavailableException
    {
        return new ParticipantInfo(messenger.getEndpoints(), NO_ENDPOINTS, instance.getQuery().getConsistencyLevel());
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

    public static class SingleThreaded extends Node
    {
        public SingleThreaded(InetAddress endpoint, Messenger messenger)
        {
            super(endpoint, messenger);
        }

        @Override
        protected PreacceptCallback getPreacceptCallback(Instance instance, final ParticipantInfo participantInfo)
        {
            return new PreacceptCallback(instance, participantInfo) {

                @Override
                public void await() throws WriteTimeoutException
                {
                    if (getResponseCount() < participantInfo.quorumSize)
                        throw new WriteTimeoutException(WriteType.CAS, participantInfo.consistencyLevel, getResponseCount(), targets);
                }

            };
        }

        @Override
        protected AcceptCallback getAcceptCallback(Instance instance, final ParticipantInfo participantInfo)
        {
            return new AcceptCallback(instance, participantInfo) {

                @Override
                public void await() throws WriteTimeoutException
                {
                    if (getResponseCount() < participantInfo.quorumSize)
                        throw new WriteTimeoutException(WriteType.CAS, participantInfo.consistencyLevel, getResponseCount(), targets);
                }

            };
        }

        @Override
        protected PrepareCallback getPrepareCallback(Instance instance, ParticipantInfo participantInfo)
        {
            return new PrepareCallback(instance, participantInfo){

                @Override
                public void await() throws WriteTimeoutException
                {
                    if (getResponseCount() < participantInfo.quorumSize)
                        throw new WriteTimeoutException(WriteType.CAS, participantInfo.consistencyLevel, getResponseCount(), targets);
                }

            };
        }

        @Override
        protected TryPreacceptCallback getTryPreacceptCallback(Instance instance, TryPreacceptAttempt attempt, final ParticipantInfo participantInfo)
        {
            return new TryPreacceptCallback(instance, attempt, participantInfo)
            {

                @Override
                public void await() throws WriteTimeoutException
                {
                    if (getResponseCount() < targets)
                        throw new WriteTimeoutException(WriteType.CAS, participantInfo.consistencyLevel, getResponseCount(), targets);
                }

            };
        }
    }
}
