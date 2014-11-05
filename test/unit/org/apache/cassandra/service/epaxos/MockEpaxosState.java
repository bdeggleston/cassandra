package org.apache.cassandra.service.epaxos;

import com.google.common.base.Predicate;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class MockEpaxosState extends EpaxosState
{
    private final InetAddress endpoint;
    public final List<InetAddress> localReplicas;
    public final List<InetAddress> localEndpoints;
    public final List<InetAddress> remoteEndpoints;

    public MockEpaxosState(int numLocal, int numRemote)
    {
        numLocal = Math.max(1, numLocal);
        numRemote = Math.max(0, numRemote);

        try
        {
            endpoint = InetAddress.getByAddress(ByteBufferUtil.bytes(1).array());
            localReplicas = new ArrayList<>(numLocal - 1);
            localEndpoints = new ArrayList<>(numLocal);
            localEndpoints.add(endpoint);
            for (int i=1; i<numLocal; i++)
            {
                InetAddress replicaEndpoint = InetAddress.getByAddress(ByteBufferUtil.bytes(i + 1).array());
                localReplicas.add(replicaEndpoint);
                localEndpoints.add(replicaEndpoint);
            }

            remoteEndpoints = new ArrayList<>(numRemote);
            for (int i=0; i<numRemote; i++)
            {
                remoteEndpoints.add(InetAddress.getByAddress(ByteBufferUtil.bytes(i + 1 + numLocal).array()));
            }
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError();
        }
    }

    @Override
    protected InetAddress getEndpoint()
    {
        return endpoint;
    }

    @Override
    protected ParticipantInfo getParticipants(Instance instance) throws UnavailableException
    {
        return new ParticipantInfo(localEndpoints, remoteEndpoints, instance.getQuery().getConsistencyLevel());
    }

    public final List<UUID> preaccepts = new LinkedList<>();

    @Override
    public void preaccept(Instance instance)
    {
        preaccepts.add(instance.getId());
    }

    public static class AcceptCall
    {
        public final UUID id;
        public final AcceptDecision decision;
        public final Runnable failureCallback;

        public AcceptCall(UUID id, AcceptDecision decision, Runnable failureCallback)
        {
            this.id = id;
            this.decision = decision;
            this.failureCallback = failureCallback;
        }
    }

    public final List<AcceptCall> accepts = new LinkedList<>();

    @Override
    public void accept(UUID iid, AcceptDecision decision, Runnable failureCallback)
    {
        accepts.add(new AcceptCall(iid, decision, failureCallback));
    }

    public static class CommitCall
    {
        public final UUID id;
        public final Set<UUID> dependencies;

        public CommitCall(UUID id, Set<UUID> dependencies)
        {
            this.id = id;
            this.dependencies = dependencies;
        }
    }

    public final List<CommitCall> commits = new LinkedList<>();

    @Override
    public void commit(UUID iid, Set<UUID> dependencies)
    {
        commits.add(new CommitCall(iid, dependencies));
    }

    public final List<UUID> executes = new LinkedList<>();

    @Override
    public void execute(UUID instanceId)
    {
        executes.add(instanceId);
    }

    public final List<UUID> prepares = new LinkedList<>();

    @Override
    public PrepareTask prepare(UUID id, PrepareGroup group)
    {
        prepares.add(id);
        return null;
    }

    public static class UpdateBallotCall
    {
        public final UUID id;
        public final int ballot;
        public final Runnable callback;

        public UpdateBallotCall(UUID id, int ballot, Runnable callback)
        {
            this.id = id;
            this.ballot = ballot;
            this.callback = callback;
        }
    }

    public final List<UpdateBallotCall> ballotUpdates = new LinkedList<>();

    @Override
    public void updateBallot(UUID id, int ballot, Runnable callback)
    {
        ballotUpdates.add(new UpdateBallotCall(id, ballot, callback));
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
}
