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

    private class Accessor
    {
        public Instance loadInstance(UUID iid)
        {
            return EpaxosManager.this.loadInstance(iid);
        }

        public void saveInstance(Instance instance)
        {
            EpaxosManager.this.saveInstance(instance);
        }

        public ReadWriteLock getLock(Object key)
        {
            return locks.get(key);
        }
    }

    public ColumnFamily query(SerializedRequest query)
            throws InvalidRequestException, UnavailableException, InvalidInstanceStateChange, WriteTimeoutException, BallotException
    {
        query.getConsistencyLevel().validateForCas();

        Instance instance = new Instance(query);

        Set<UUID> acceptDeps = preaccept(instance);

        if (acceptDeps != null)
            accept(instance, acceptDeps);

        commit(instance);

        execute(instance);
        return null;
    }

    protected Instance createInstance(SerializedRequest request)
    {
        return new Instance(request);
    }

    public Set<UUID> getCurrentDependencies(SerializedRequest query)
    {
        return new HashSet<>();
    }

    protected PreacceptCallback getPreacceptCallback(Instance instance, ParticipantInfo participantInfo)
    {
        return new PreacceptCallback(instance, participantInfo);
    }

    public Set<UUID> preaccept(Instance instance) throws UnavailableException, InvalidInstanceStateChange, WriteTimeoutException, BallotException
    {
        ReadWriteLock lock = locks.get(instance.getId());
        lock.writeLock().lock();
        try
        {
            instance.preaccept(getCurrentDependencies(instance.getQuery()));
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
                sendRR(message, endpoint, callback);
        }
        finally
        {
            lock.readLock().unlock();
        }

        callback.await();


        return callback.getAcceptDeps();
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
                        instance = remoteInstance;
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
                sendRR(message, endpoint, callback);
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
                    instance = remoteInstance;
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
                    instance = remoteInstance;
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

            execute(instance);
        }
    }

    public IVerbHandler<Instance> getCommitVerbHandler()
    {
        return new CommitVerbHandler();
    }

    private class ExecutionOrdering
    {
        private final DependencyGraph dependencyGraph = new DependencyGraph();
        public final Set<UUID> uncommitted = new HashSet<>();
        private final Set<UUID> requiredInstances = new HashSet<>();

        private final Instance target;
        private final Set<UUID> targetDeps;

        private ExecutionOrdering(Instance target)
        {
            this.target = target;
            targetDeps = target.getDependencies();
        }

        private void addInstance(Instance instance)
        {
            Set<UUID> deps;
            Instance.State state;
            Set<UUID> stronglyConnected;
            ReadWriteLock lock = locks.get(instance);
            lock.readLock().lock();
            try
            {
                deps = instance.getDependencies();
                state = instance.getState();
                stronglyConnected = instance.getStronglyConnected();
            }
            finally
            {
                lock.readLock().lock();
            }

            // if the instance is already executed, and it's not a dependency
            // of the target execution instance, only add it to the dep graph
            // if it's connected to an uncommitted instance, since that will
            // make it part of a strongly connected component of at least one
            // unexecuted instance, and will therefore affect the execution
            // ordering
            if (state == Instance.State.EXECUTED)
            {
                if (!targetDeps.contains(instance.getId()))
                {
                    boolean connected = false;
                    for (UUID dep: deps)
                    {
                        boolean notExecuted = loadInstance(dep).getState() != Instance.State.EXECUTED;
                        boolean targetDep = targetDeps.contains(dep);
                        boolean required = requiredInstances.contains(dep);
                        connected |= notExecuted || targetDep || required;
                    }
                    if (!connected)
                        return;
                }

            }
            else if (state != Instance.State.COMMITTED)
            {
                uncommitted.add(instance.getId());
            }

            if (stronglyConnected != null)
                requiredInstances.addAll(stronglyConnected);

            dependencyGraph.addVertex(instance.getId(), deps);
            for (UUID dep: deps)
            {
                if (dependencyGraph.contains(dep))
                    continue;

                Instance depInst = loadInstance(dep);
                assert depInst != null;

                addInstance(depInst);
            }
        }

        public void buildGraph()
        {
            addInstance(target);
        }

        public List<UUID> getOrder()
        {
            List<List<UUID>> scc = dependencyGraph.getExecutionOrder();

            // record the strongly connected components on the instances.
            // As instances are executed, they will stop being added to the depGraph for sorting.
            // However, if an instance that's not added to the dep graph is part of a strongly
            // connected component, it will affect the execution order by breaking the component.
            if (uncommitted.size() > 0)
            {
                for (List<UUID> component: scc)
                {
                    if (component.size() > 1)
                    {
                        // we're not using a lock, or persisting because:
                        // 1) the strongly connected ids will always be the same, and will always be computed
                        //    so writing the same value multiple times isn't a big deal.
                        // 2) the instance will be persisted when it's marked as executed. If it doesn't make
                        //    it that far, the strongly connected set will be computed again
                        Set<UUID> componentSet = ImmutableSet.copyOf(component);
                        for (UUID iid: component)
                            loadInstance(iid).setStronglyConnected(componentSet);
                    }

                }
            }

            return Lists.newArrayList(Iterables.concat(scc));
        }
    }

    public void execute(Instance instance)
    {
        while (true)
        {
            ExecutionOrdering executionOrdering = new ExecutionOrdering(instance);
            executionOrdering.buildGraph();

            if (executionOrdering.uncommitted.size() > 0)
            {
                for (UUID iid: executionOrdering.uncommitted)
                    prepare(loadInstance(iid));
            }
            else
            {
                for (UUID iid: executionOrdering.getOrder())
                {
                    ReadWriteLock lock = locks.get(iid);
                    lock.writeLock().lock();
                    Instance toExecute = loadInstance(iid);
                    try
                    {
                        if (toExecute.getState() == Instance.State.EXECUTED)
                            return;

                        assert toExecute.getState() == Instance.State.COMMITTED;

                        executeInstance(toExecute);
                        toExecute.setExecuted();
                        saveInstance(toExecute);

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

    public void prepare(Instance instance)
    {

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
