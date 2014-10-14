package org.apache.cassandra.service.epaxos;

import com.google.common.base.Predicate;
import com.google.common.collect.*;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;

public class EpaxosManager
{
    public static final EpaxosManager instance = new EpaxosManager();

    private static final Logger logger = LoggerFactory.getLogger(EpaxosManager.class);

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

        public ParticipantInfo(List<InetAddress> endpoints, List<InetAddress> remoteEndpoints, ConsistencyLevel cl) throws UnavailableException
        {
            this.endpoints = endpoints;
            this.liveEndpoints = ImmutableList.copyOf(Iterables.filter(endpoints, livePredicate()));
            this.consistencyLevel = cl;

            if (cl == ConsistencyLevel.SERIAL && (remoteEndpoints != null && remoteEndpoints.size() > 0))
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

    public static interface IAccessor
    {
        public Instance loadInstance(UUID iid);

        public void saveInstance(Instance instance);

        public ReadWriteLock getLock(Object key);
    }

    private final IAccessor accessor = new IAccessor()
    {
        @Override
        public Instance loadInstance(UUID iid)
        {
            return EpaxosManager.this.loadInstance(iid);
        }

        @Override
        public void saveInstance(Instance instance)
        {
            EpaxosManager.this.saveInstance(instance);
        }

        @Override
        public ReadWriteLock getLock(Object key)
        {
            return locks.get(key);
        }
    };

    public ColumnFamily query(SerializedRequest query)
            throws InvalidRequestException, UnavailableException, InvalidInstanceStateChange, WriteTimeoutException, BallotException
    {
        query.getConsistencyLevel().validateForCas();

        Instance instance = createInstance(query);

        AcceptDecision acceptDecision = preaccept(instance);

        if (acceptDecision.acceptNeeded)
            accept(instance, acceptDecision.acceptDeps);

        commit(instance);

        execute(instance);
        return null;
    }

    protected Instance createInstance(SerializedRequest request)
    {
        return new Instance(request, getEndpoint());
    }

    public Set<UUID> getCurrentDependencies(SerializedRequest query)
    {
        return instances.keySet();
    }

    protected PreacceptCallback getPreacceptCallback(Instance instance, ParticipantInfo participantInfo)
    {
        return new PreacceptCallback(instance, participantInfo);
    }

    public AcceptDecision preaccept(Instance instance) throws UnavailableException, InvalidInstanceStateChange, BallotException, WriteTimeoutException
    {
        ReadWriteLock lock = locks.get(instance.getId());
        lock.writeLock().lock();
        try
        {
            instance.preaccept(getCurrentDependencies(instance.getQuery()));
            // TODO: select successors
            instance.incrementBallot();
            saveInstance(instance);
        }
        finally
        {
            lock.writeLock().unlock();
        }

        PreacceptCallback callback;

        // don't hold a write lock while sending
        lock.readLock().lock();
        try
        {
            ParticipantInfo participantInfo = getParticipants(instance);
            MessageOut<Instance> message = new MessageOut<Instance>(MessagingService.Verb.PREACCEPT_REQUEST,
                                                                    instance,
                                                                    Instance.serializer);
            callback = getPreacceptCallback(instance, participantInfo);
            for (InetAddress endpoint : participantInfo.liveEndpoints)
                if (!endpoint.equals(getEndpoint()))
                    sendRR(message, endpoint, callback);
                else
                    callback.countLocal();
        }
        finally
        {
            lock.readLock().unlock();
        }

        try
        {
            callback.await();
        }
        catch (WriteTimeoutException e)
        {
            setFastPathImpossible(instance);
            throw e;
        }

        AcceptDecision decision = callback.getAcceptDecision();
        if (decision.acceptNeeded)
            setFastPathImpossible(instance);

        return decision;
    }

    private void setFastPathImpossible(Instance instance)
    {
        ReadWriteLock lock = locks.get(instance.getId());
        lock.writeLock().lock();
        try
        {
            instance.setFastPathImpossible(true);
            saveInstance(instance);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    protected class PreacceptVerbHandler implements IVerbHandler<Instance>
    {
        @Override
        public void doVerb(MessageIn<Instance> message, int id)
        {
            ReadWriteLock lock = locks.get(message.payload.getId());
            lock.writeLock().lock();
            try
            {
                Instance remoteInstance = message.payload;
                Instance instance = loadInstance(remoteInstance.getId());
                try
                {
                    if (instance == null)
                    {
                        instance = remoteInstance.copyRemote();
                    } else
                    {
                        instance.checkBallot(remoteInstance.getBallot());
                    }
                    instance.preaccept(getCurrentDependencies(instance.getQuery()), remoteInstance.getDependencies());
                    saveInstance(instance);

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
                    PreacceptResponse response = PreacceptResponse.ballotFailure(e.localBallot);
                    MessageOut<PreacceptResponse> reply = new MessageOut<>(MessagingService.Verb.PREACCEPT_RESPONSE,
                                                                           response,
                                                                           PreacceptResponse.serializer);
                    sendReply(reply, id, message.from);
                }
                catch (InvalidInstanceStateChange e)
                {
                    // a BallotException should always be thrown before getting here
                    throw new AssertionError(e.getMessage());
                }
            }
            finally
            {
                lock.writeLock().unlock();
            }
        }
    }

    public IVerbHandler<Instance> getPreacceptVerbHandler()
    {
        return new PreacceptVerbHandler();
    }

    protected AcceptCallback getAcceptCallback(Instance instance, ParticipantInfo participantInfo)
    {
        return new AcceptCallback(instance, participantInfo);
    }

    public void accept(Instance instance, Set<UUID> deps) throws InvalidInstanceStateChange, UnavailableException, WriteTimeoutException, BallotException
    {
        ReadWriteLock lock = locks.get(instance.getId());
        lock.writeLock().lock();
        try
        {
            instance.accept(deps);
            instance.incrementBallot();
            saveInstance(instance);
        }
        finally
        {
            lock.writeLock().unlock();
        }

        AcceptCallback callback;
        // don't hold a write lock while sending
        lock.readLock().lock();
        try
        {
            ParticipantInfo participantInfo = getParticipants(instance);
            MessageOut<Instance> message = new MessageOut<Instance>(MessagingService.Verb.ACCEPT_REQUEST,
                                                                    instance,
                                                                    Instance.serializer);
            callback = getAcceptCallback(instance, participantInfo);
            for (InetAddress endpoint : participantInfo.liveEndpoints)
                if (!endpoint.equals(getEndpoint()))
                    sendRR(message, endpoint, callback);
                else
                    callback.countLocal();
        }
        finally
        {
            lock.readLock().unlock();
        }

        callback.await();
        callback.checkSuccess();
    }

    protected class AcceptVerbHandler implements IVerbHandler<Instance>
    {
        @Override
        public void doVerb(MessageIn<Instance> message, int id)
        {
            ReadWriteLock lock = locks.get(message.payload.getId());
            lock.writeLock().lock();
            try
            {
                Instance remoteInstance = message.payload;
                Instance instance = loadInstance(remoteInstance.getId());
                if (instance == null)
                {
                    instance = remoteInstance.copyRemote();
                } else
                {
                    instance.checkBallot(remoteInstance.getBallot());
                    instance.accept(remoteInstance.getDependencies());
                }
                saveInstance(instance);

                AcceptResponse response = new AcceptResponse(true, 0);
                MessageOut<AcceptResponse> reply = new MessageOut<>(MessagingService.Verb.ACCEPT_RESPONSE,
                                                                    response,
                                                                    AcceptResponse.serializer);
                sendReply(reply, id, message.from);
            }
            catch (BallotException e)
            {
                AcceptResponse response = new AcceptResponse(false, e.localBallot);
                MessageOut<AcceptResponse> reply = new MessageOut<>(MessagingService.Verb.ACCEPT_RESPONSE,
                                                                    response,
                                                                    AcceptResponse.serializer);
                sendReply(reply, id, message.from);
            }
            catch (InvalidInstanceStateChange e)
            {
                // a BallotException should always be thrown before getting here
                throw new AssertionError(e.getMessage());
            }
            finally
            {
                lock.writeLock().unlock();
            }
        }
    }

    public IVerbHandler<Instance> getAcceptVerbHandler()
    {
        return new AcceptVerbHandler();
    }

    public void commit(Instance instance) throws InvalidInstanceStateChange, UnavailableException
    {
        ReadWriteLock lock = locks.get(instance.getId());
        lock.writeLock().lock();
        try
        {
            instance.commit();
            instance.incrementBallot();
            saveInstance(instance);
        }
        finally
        {
            lock.writeLock().unlock();
        }

        lock.readLock().lock();
        try
        {
            ParticipantInfo participantInfo = getParticipants(instance);
            MessageOut<Instance> message = new MessageOut<Instance>(MessagingService.Verb.COMMIT_REQUEST,
                                                                    instance,
                                                                    Instance.serializer);
            for (InetAddress endpoint : participantInfo.liveEndpoints)
                if (!endpoint.equals(getEndpoint()))
                    sendOneWay(message, endpoint);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    protected class CommitVerbHandler implements IVerbHandler<Instance>
    {
        @Override
        public void doVerb(MessageIn<Instance> message, int id)
        {
            Instance instance;
            ReadWriteLock lock = locks.get(message.payload.getId());
            lock.writeLock().lock();
            try
            {
                Instance remoteInstance = message.payload;
                instance = loadInstance(remoteInstance.getId());
                if (instance == null)
                {
                    instance = remoteInstance.copyRemote();
                } else
                {
                    instance.commit(remoteInstance.getDependencies());
                }
                saveInstance(instance);
            }
            catch (InvalidInstanceStateChange e)
            {
                // got a duplicate commit message, no big deal
                logger.debug("Duplicate commit message received", e);
                return;
            }
            finally
            {
                lock.writeLock().unlock();
            }

            try
            {
                execute(instance);
            }
            catch (UnavailableException | WriteTimeoutException e)
            {
                // TODO: maybe do something besides just logging?
                logger.error("Error executing instance", e);
            }
        }
    }

    public IVerbHandler<Instance> getCommitVerbHandler()
    {
        return new CommitVerbHandler();
    }

    public void execute(Instance instance) throws UnavailableException, WriteTimeoutException
    {
        while (true)
        {
            ExecutionSorter executionSorter = new ExecutionSorter(instance, accessor);
            executionSorter.buildGraph();

            if (executionSorter.uncommitted.size() > 0)
            {
                for (UUID iid : executionSorter.uncommitted)
                    prepare(loadInstance(iid));
            } else
            {
                for (UUID iid : executionSorter.getOrder())
                {
                    ReadWriteLock lock = locks.get(iid);
                    lock.writeLock().lock();
                    Instance toExecute = loadInstance(iid);
                    try
                    {
                        if (toExecute.getState() == Instance.State.EXECUTED)
                            continue;

                        assert toExecute.getState() == Instance.State.COMMITTED;

                        executeInstance(toExecute);
                        toExecute.setExecuted();
                        saveInstance(toExecute);

                        if (toExecute.getId().equals(instance.getId()))
                            return;

                    }
                    finally
                    {
                        lock.writeLock().unlock();
                    }
                }
            }
        }
    }

    protected void executeInstance(Instance instance)
    {
        // TODO: databasey read/write stuff
    }

    protected PrepareCallback getPrepareCallback(Instance instance, ParticipantInfo participantInfo)
    {
        return new PrepareCallback(instance, participantInfo);
    }

    public void prepare(Instance instance) throws UnavailableException, WriteTimeoutException
    {
        if (shouldPrepare(instance))
        {
            PrepareCallback callback;
            ParticipantInfo participantInfo;

            ReadWriteLock lock = locks.get(instance.getId());
            lock.writeLock().lock();
            try
            {
                instance.incrementBallot();
                saveInstance(instance);

                participantInfo = getParticipants(instance);
                PrepareRequest request = new PrepareRequest(instance);
                MessageOut<PrepareRequest> message = new MessageOut<>(MessagingService.Verb.PREACCEPT_REQUEST,
                                                                      request,
                                                                      PrepareRequest.serializer);
                callback = getPrepareCallback(instance, participantInfo);
                for (InetAddress endpoint : participantInfo.liveEndpoints)
                    if (!endpoint.equals(getEndpoint()))
                        sendRR(message, endpoint, callback);
                    else
                        callback.countLocal(getEndpoint(), instance);
            }
            finally
            {
                lock.writeLock().unlock();
            }

            callback.await();

        }
    }

    protected boolean shouldPrepare(Instance instance)
    {
        // TODO: wait for successors and grace periods
        return true;
    }

    protected class PrepareVerbHandler implements IVerbHandler<PrepareRequest>
    {
        @Override
        public void doVerb(MessageIn<PrepareRequest> message, int id)
        {
            ReadWriteLock lock = locks.get(message.payload.iid);
            lock.writeLock().lock();
            try
            {
                Instance instance = loadInstance(message.payload.iid);
                if (instance != null)
                    try
                    {
                        instance.checkBallot(message.payload.ballot);
                        saveInstance(instance);
                    }
                    catch (BallotException e)
                    {
                        // don't die if the message has an old ballot value, just don't
                        // update the instance. This instance will still be useful to the requestor
                    }

                MessageOut<Instance> reply = new MessageOut<>(MessagingService.Verb.PREPARE_RESPONSE,
                                                              instance,
                                                              Instance.serializer);
                sendReply(reply, id, message.from);
            }
            finally
            {
                lock.writeLock().unlock();
            }
        }
    }

    public IVerbHandler<PrepareRequest> getPrepareVerbHandler()
    {
        return new PrepareVerbHandler();
    }

    protected Instance loadInstance(UUID instanceId)
    {
        // read from table
        return instances.get(instanceId);
    }

    protected void saveInstance(Instance instance)
    {
        // actually write to table
        instances.put(instance.getId(), instance);
    }

    // wrapped for testing
    protected void sendReply(MessageOut message, int id, InetAddress to)
    {
        MessagingService.instance().sendReply(message, id, to);
    }

    protected int sendRR(MessageOut message, InetAddress to, IAsyncCallback cb)
    {
        return MessagingService.instance().sendRR(message, to, cb);
    }

    protected void sendOneWay(MessageOut message, InetAddress to)
    {
        MessagingService.instance().sendOneWay(message, to);
    }

    protected InetAddress getEndpoint()
    {
        return FBUtilities.getBroadcastAddress();
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

    protected Predicate<InetAddress> livePredicate()
    {
        return IAsyncCallback.isAlive;
    }

}
