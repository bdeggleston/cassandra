package org.apache.cassandra.service.epaxos;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Striped;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.apache.cassandra.utils.FBUtilities;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;

public class EpaxosManager
{
    private static final List<InetAddress> NO_ENDPOINTS = ImmutableList.of();

    private final ConcurrentMap<UUID, Instance> instances = Maps.newConcurrentMap();
    private final Striped<ReadWriteLock> locks = Striped.readWriteLock(DatabaseDescriptor.getConcurrentWriters() * 1024);

    public class ParticipantInfo
    {

        // endpoints that are actively involved in this tx
        public final List<InetAddress> endpoints;
        public final List<InetAddress> liveEndpoints;
        public final ConsistencyLevel consistencyLevel;

        // endpoints that are not involved in the tx, but need to
        // be notified of it's commit
        public final List<InetAddress> remoteEndpoints;
        public final int N;  // total number of nodes
        public final int F;  // number tolerable failures
        public final int quorumSize;
        public final int fastQuorumSize;

        ParticipantInfo(List<InetAddress> endpoints, List<InetAddress> remoteEndpoints, ConsistencyLevel cl) throws UnavailableException
        {
            this.endpoints = endpoints;
            this.liveEndpoints = ImmutableList.copyOf(Iterables.filter(endpoints, livePredicate()));
            this.consistencyLevel = cl;

            if (cl == ConsistencyLevel.SERIAL && remoteEndpoints != null)
                throw new AssertionError("SERIAL consistency must include all endpoints");
            this.remoteEndpoints = remoteEndpoints != null ? remoteEndpoints : NO_ENDPOINTS;

            N = endpoints.size();
            F = N / 2;
            quorumSize = F + 1;
            fastQuorumSize = F + ((F + 1) / 2);

            if (liveEndpoints.size() < quorumSize)
                throw new UnavailableException(cl, quorumSize, liveEndpoints.size());

        }
    }

    public ColumnFamily query(SerializedRequest query)
            throws InvalidRequestException, UnavailableException, InvalidInstanceStateChange
    {
        query.getConsistencyLevel().validateForCas();

        Instance instance = new Instance(query);
        instances.put(instance.getId(), instance);

        preaccept(instance);



        return null;
    }

    public Set<UUID> getCurrentDependencies(SerializedRequest query)
    {
        return new HashSet<>();
    }

    public boolean preaccept(Instance instance) throws UnavailableException, InvalidInstanceStateChange, WriteTimeoutException
    {
        ReadWriteLock lock = locks.get(instance.getId());
        lock.writeLock().lock();
        try
        {
            instance.preaccept(getCurrentDependencies(instance.getQuery()));
            instance.incrementBallot();
            persistInstance(instance);
        }
        finally
        {
            lock.writeLock().unlock();
        }

        // don't hold a write lock while sending and receiving messages
        lock.readLock().lock();
        PreacceptCallback callback;
        try
        {
            ParticipantInfo participantInfo = getParticipants(instance);
            MessageOut<Instance> message = new MessageOut<Instance>(MessagingService.Verb.PREACCEPT_REQUEST,
                                                                    instance,
                                                                    Instance.serializer);
            callback = new PreacceptCallback(instance, participantInfo);
            for (InetAddress endpoint: participantInfo.liveEndpoints)
                sendRR(message, endpoint, callback);
        }
        finally
        {
            lock.readLock().unlock();
        }

        callback.await();


        return false;
    }

    public IVerbHandler<Instance> getPreacceptVerbHandler()
    {
        return new IVerbHandler<Instance>()
        {
            @Override
            public void doVerb(MessageIn<Instance> message, int id)
            {
                ReadWriteLock lock = locks.get(message.payload.getId());
                lock.writeLock().lock();
                try
                {
                    Instance remoteInstance = message.payload;
                    Instance instance = instances.get(remoteInstance.getId());
                    try
                    {
                        if (instance == null)
                        {
                            Instance previous = instances.putIfAbsent(remoteInstance.getId(), remoteInstance);
                            instance = previous == null ? remoteInstance : previous;
                        }
                        else
                        {
                            instance.checkBallot(remoteInstance.getBallot());
                        }
                        instance.preaccept(getCurrentDependencies(instance.getQuery()), remoteInstance.getDependencies());

                        PreacceptResponse response;
                        if (instance.getLeaderDepsMatch())
                            response = PreacceptResponse.success(instance);
                        else
                            response = PreacceptResponse.failure(instance, new ArrayList<Instance>());  // TODO: return missing instances
                        MessageOut<PreacceptResponse> reply = new MessageOut<>(MessagingService.Verb.PREACCEPT_RESPONSE,
                                                                                     response,
                                                                                     PreacceptResponse.serializer);
                        sendReply(reply, id, message.from);
                    }
                    catch (BallotException e)
                    {
                        BallotRejectionResponse response = new BallotRejectionResponse(e.instanceId, e.localBallot);
                        MessageOut<BallotRejectionResponse> reply = new MessageOut<>(MessagingService.Verb.EPAXOS_BALLOT_REJECT_RESPONSE,
                                                                                     response,
                                                                                     BallotRejectionResponse.serializer);
                        sendReply(reply, id, message.from);
                    }
                    catch (InvalidInstanceStateChange invalidInstanceStateChange)
                    {
                        // a BallotException should always be thrown before getting here
                        throw new AssertionError(invalidInstanceStateChange.getMessage());
                    }
                }
                finally
                {
                    lock.writeLock().unlock();
                }

            }
        };
    }


    protected Instance getInstance(UUID instanceId)
    {
        // read from table
        return instances.get(instanceId);
    }

    protected void persistInstance(Instance instance)
    {
        // actually write to table
    }

    // wrapped for testing
    protected void sendReply(MessageOut message, int id, InetAddress to)
    {
        MessagingService.instance().sendReply(message, id, to);
    }

    public int sendRR(MessageOut message, InetAddress to, IAsyncCallback cb)
    {
        return MessagingService.instance().sendRR(message, to, cb);
    }

    protected ParticipantInfo getParticipants(Instance instance) throws UnavailableException
    {
        SerializedRequest query = instance.getQuery();

        Token tk = StorageService.getPartitioner().getToken(query.getKey());
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(query.getKeyspaceName(), tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, query.getKeyspaceName());

        List<InetAddress> endpoints = ImmutableList.copyOf(Iterables.concat(naturalEndpoints, pendingEndpoints));
        List<InetAddress> remoteEndpoints = null;
        if (query.getConsistencyLevel() == ConsistencyLevel.LOCAL_SERIAL)
        {
            // Restrict naturalEndpoints and pendingEndpoints to node in the local DC only
            String localDc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
            Predicate<InetAddress> isLocalDc = dcPredicateFor(localDc, false);
            Predicate<InetAddress> notLocalDc = dcPredicateFor(localDc, true);

            remoteEndpoints = ImmutableList.copyOf(Iterables.filter(endpoints, notLocalDc));
            endpoints = ImmutableList.copyOf(Iterables.filter(endpoints, isLocalDc));
        }
        return new ParticipantInfo(endpoints, remoteEndpoints, query.getConsistencyLevel());
    }

    protected Predicate<InetAddress> dcPredicateFor(final String dc, final boolean equals)
    {
        final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        return new Predicate<InetAddress>()
        {
            public boolean apply(InetAddress host)
            {
                boolean equal = dc.equals(snitch.getDatacenter(host));
                return equals ? equal : !equal;
            }
        };
    }

    protected static Predicate<InetAddress> livePredicate()
    {
        return IAsyncCallback.isAlive;
    }

}
