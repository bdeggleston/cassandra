package org.apache.cassandra.service.epaxos;

import com.google.common.base.Predicate;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.*;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Striped;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;
import org.apache.cassandra.service.epaxos.exceptions.InstanceNotFoundException;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.apache.cassandra.service.epaxos.exceptions.PrepareAbortException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public class EpaxosService
{
    public static final EpaxosService instance = new EpaxosService();

    private static final Logger logger = LoggerFactory.getLogger(EpaxosService.class);

    private static final List<InetAddress> NO_ENDPOINTS = ImmutableList.of();

    // TODO: put these in DatabaseDescriptor

    // the number of times a prepare phase will try to gain control of an instance before giving up
    protected static int PREPARE_BALLOT_FAILURE_RETRIES = 5;

    // the amount of time the prepare phase will wait for the leader to commit an instance before
    // attempting a prepare phase. This is multiplied by a replica's position in the successor list
    protected static int PREPARE_GRACE_MILLIS = 2000;

    private final Striped<ReadWriteLock> locks = Striped.readWriteLock(DatabaseDescriptor.getConcurrentWriters() * 1024);
    private final Cache<UUID, Instance> instanceCache;

    // prevents multiple threads from attempting to prepare the same instance. Aquire this before an instance lock
    private final Striped<Lock> prepareLocks = Striped.lock(DatabaseDescriptor.getConcurrentWriters() * 1024);

    // prevents multiple threads modifying the dependency manager for a given key. Aquire this after locking an instance
    private final Striped<Lock> depsLocks = Striped.lock(DatabaseDescriptor.getConcurrentWriters() * 1024);
    private final Cache<Pair<ByteBuffer, UUID>, DependencyManager> dependencyManagers;

    // aborts prepare phases on commit
    private final ConcurrentMap<UUID, CountDownLatch> commitLatches = Maps.newConcurrentMap();

    private final Random random = new Random();

    private final Predicate<InetAddress> nonLocalPredicate = new Predicate<InetAddress>()
    {
        @Override
        public boolean apply(InetAddress inetAddress)
        {
            return !getEndpoint().equals(inetAddress);
        }
    };

    private final Map<UUID, SettableFuture> resultFutures = Maps.newConcurrentMap();

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
            return EpaxosService.this.loadInstance(iid);
        }

        @Override
        public void saveInstance(Instance instance)
        {
            EpaxosService.this.saveInstance(instance);
        }

        @Override
        public ReadWriteLock getLock(Object key)
        {
            return locks.get(key);
        }
    };

    public EpaxosService()
    {
        instanceCache = CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.MINUTES).maximumSize(10000).build();
        dependencyManagers = CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.MINUTES).maximumSize(1000).build();
    }

    protected String keyspace()
    {
        return Keyspace.SYSTEM_KS;
    }

    protected String instanceTable()
    {
        return SystemKeyspace.EPAXOS_INSTANCE;
    }

    protected String dependencyTable()
    {
        return SystemKeyspace.EPAXOS_DEPENDENCIES;
    }

    protected Random getRandom()
    {
        return random;
    }

    protected long getTimeout(long start)
    {
        return Math.max(1, DatabaseDescriptor.getRpcTimeout() - (System.currentTimeMillis() - start));
    }

    // TODO: eventually, the type parameter will let this take read and cas requests
    public <T> T query(SerializedRequest query)
            throws UnavailableException, WriteTimeoutException, ReadTimeoutException, InvalidRequestException
    {
        long start = System.currentTimeMillis();

        query.getConsistencyLevel().validateForCas();

        Instance instance = createInstance(query);
        SettableFuture<T> resultFuture = SettableFuture.create();
        resultFutures.put(instance.getId(), resultFuture);
        try
        {
            AcceptDecision acceptDecision = preaccept(instance);

            if (acceptDecision.acceptNeeded)
                accept(instance.getId(), acceptDecision);

            commit(instance.getId(), acceptDecision.acceptDeps);

            execute(instance.getId());

            return resultFuture.get(getTimeout(start), TimeUnit.MILLISECONDS);
        }
        catch (RequestTimeoutException | InvalidInstanceStateChange | BallotException
                | UnavailableException | ExecutionException | TimeoutException e1)
        {
            try
            {
                // it's possible that another node will take control of this instance and commit it
                // before the timeout is up, so we'll wait for it to be committed here before returning
                return resultFuture.get(getTimeout(start), TimeUnit.MILLISECONDS);
            }
            catch (TimeoutException | ExecutionException e2)
            {
                if (e1 instanceof ReadTimeoutException)
                    throw (ReadTimeoutException) e1;
                if (e1 instanceof WriteTimeoutException)
                    throw (WriteTimeoutException) e1;

                throw new WriteTimeoutException(WriteType.CAS, query.getConsistencyLevel(), 0, 1);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        finally
        {
            resultFutures.remove(instance.getId());
        }
    }

    protected Instance createInstance(SerializedRequest request)
    {
        return new Instance(request, getEndpoint());
    }

    protected Set<UUID> getCurrentDependencies(Instance instance)
    {
        SerializedRequest request = instance.getQuery();

        Pair<ByteBuffer, UUID> keyPair = Pair.create(request.getKey(), Schema.instance.getId(request.getKeyspaceName(), request.getCfName()));

        Lock lock = depsLocks.get(keyPair);
        lock.lock();
        try
        {
            DependencyManager dm = loadDependencyManager(keyPair.left, keyPair.right);
            Set<UUID> deps = dm.getDepsAndAdd(instance.getId());
            saveDependencyManager(keyPair.left, keyPair.right, dm);
            return deps;
        }
        finally
        {
            lock.unlock();
        }
    }

    protected void recordMissingInstance(Instance instance)
    {
        SerializedRequest request = instance.getQuery();

        Pair<ByteBuffer, UUID> keyPair = Pair.create(request.getKey(), Schema.instance.getId(request.getKeyspaceName(), request.getCfName()));

        Lock lock = depsLocks.get(keyPair);
        lock.lock();
        try
        {
            DependencyManager dm = loadDependencyManager(keyPair.left, keyPair.right);
            dm.recordInstance(instance.getId());

            if (instance.getState().atLeast(Instance.State.ACCEPTED))
            {
                dm.markAcknowledged(Sets.newHashSet(instance.getId()));
            }

            if (instance.getState() == Instance.State.EXECUTED)
            {
                dm.markExecuted(instance.getId());
            }

            saveDependencyManager(keyPair.left, keyPair.right, dm);
        }
        finally
        {
            lock.unlock();
        }
    }

    protected void recordAcknowledgedDeps(Instance instance)
    {
        SerializedRequest request = instance.getQuery();

        Pair<ByteBuffer, UUID> keyPair = Pair.create(request.getKey(), Schema.instance.getId(request.getKeyspaceName(), request.getCfName()));

        Lock lock = depsLocks.get(keyPair);
        lock.lock();
        try
        {
            DependencyManager dm = loadDependencyManager(keyPair.left, keyPair.right);
            dm.markAcknowledged(instance.getDependencies(), instance.getId());
            saveDependencyManager(keyPair.left, keyPair.right, dm);
        }
        finally
        {
            lock.unlock();
        }
    }

    protected void recordExecuted(Instance instance)
    {
        SerializedRequest request = instance.getQuery();

        Pair<ByteBuffer, UUID> keyPair = Pair.create(request.getKey(), Schema.instance.getId(request.getKeyspaceName(), request.getCfName()));

        Lock lock = depsLocks.get(keyPair);
        lock.lock();
        try
        {
            DependencyManager dm = loadDependencyManager(keyPair.left, keyPair.right);
            dm.markExecuted(instance.getId());
            saveDependencyManager(keyPair.left, keyPair.right, dm);
        }
        finally
        {
            lock.unlock();
        }
    }

    protected PreacceptCallback getPreacceptCallback(Instance instance, ParticipantInfo participantInfo)
    {
        return new PreacceptCallback(instance, participantInfo);
    }

    public AcceptDecision preaccept(Instance instance) throws UnavailableException, InvalidInstanceStateChange, BallotException, WriteTimeoutException
    {
        logger.debug("preaccepting instance {}", instance.getId());
        PreacceptCallback callback;
        ReadWriteLock lock = locks.get(instance.getId());
        lock.writeLock().lock();

        try
        {
            ParticipantInfo participantInfo = getParticipants(instance);
            instance.preaccept(getCurrentDependencies(instance));
            List<InetAddress> successors = Lists.newArrayList(Iterables.filter(participantInfo.endpoints, nonLocalPredicate));
            Collections.shuffle(successors, getRandom());
            instance.setSuccessors(successors);
            instance.incrementBallot();
            saveInstance(instance);

            MessageOut<Instance> message = new MessageOut<Instance>(MessagingService.Verb.EPAXOS_PREACCEPT,
                                                                    instance,
                                                                    Instance.serializer);
            callback = getPreacceptCallback(instance, participantInfo);
            for (InetAddress endpoint : participantInfo.liveEndpoints)
            {
                if (!endpoint.equals(getEndpoint()))
                {
                    logger.debug("sending preaccept request to {} for instance {}", endpoint, instance.getId());
                    sendRR(message, endpoint, callback);
                }
                else
                {
                    logger.debug("counting self in preaccept quorum for instance {}", instance.getId());
                    callback.countLocal();
                }
            }
        }
        finally
        {
            lock.writeLock().unlock();
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
        {
            setFastPathImpossible(instance);
        }

        for (Instance missingInstance: callback.getMissingInstances())
        {
            addMissingInstance(missingInstance);
        }

        return decision;
    }

    private void setFastPathImpossible(Instance instance)
    {
        logger.debug("Setting fast path impossible for {}", instance.getId());
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
            logger.debug("Preaccept request received from {} for {}", message.from, message.payload.getId());

            PreacceptResponse response;
            Set<UUID> missingInstanceIds = null;

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
                        // TODO: add to deps
                        instance = remoteInstance.copyRemote();
                    }
                    else
                    {
                        instance.checkBallot(remoteInstance.getBallot());
                        instance.applyRemote(remoteInstance);
                    }
                    instance.preaccept(getCurrentDependencies(instance), remoteInstance.getDependencies());
                    saveInstance(instance);

                    if (instance.getLeaderDepsMatch())
                    {
                        logger.debug("Preaccept dependencies agree for {}", instance.getId());
                        response = PreacceptResponse.success(instance);
                    }
                    else
                    {
                        logger.debug("Preaccept dependencies disagree for {}", instance.getId());
                        missingInstanceIds = Sets.difference(instance.getDependencies(), remoteInstance.getDependencies());
                        missingInstanceIds.remove(instance.getId());
                        response = PreacceptResponse.failure(instance);
                    }
                }
                catch (BallotException e)
                {
                    response = PreacceptResponse.ballotFailure(e.localBallot);
                }
                catch (InvalidInstanceStateChange e)
                {
                    // another node is working on a prepare phase that this node wasn't involved in.
                    // as long as the dependencies are the same, reply with an ok, otherwise, something
                    // has gone wrong
                    assert instance.getDependencies().equals(message.payload.getDependencies());

                    response = PreacceptResponse.success(instance);
                }
            }
            finally
            {
                lock.writeLock().unlock();
            }
            response.missingInstances = getInstanceCopies(missingInstanceIds);
            MessageOut<PreacceptResponse> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE,
                                                                   response,
                                                                   PreacceptResponse.serializer);
            sendReply(reply, id, message.from);
        }
    }

    private List<Instance> getInstanceCopies(Set<UUID> iids)
    {
        if (iids == null || iids.size() == 0)
            return Collections.EMPTY_LIST;

        List<Instance> instances = Lists.newArrayListWithCapacity(iids.size());
        for (UUID iid: iids)
        {
            ReadWriteLock lock = locks.get(iid);
            lock.readLock().lock();
            try
            {
                Instance missingInstance = loadInstance(iid);
                if (missingInstance != null)
                {
                    instances.add(missingInstance.copy());
                }
            }
            finally
            {
                lock.readLock().unlock();
            }

        }
        return instances;
    }

    public IVerbHandler<Instance> getPreacceptVerbHandler()
    {
        return new PreacceptVerbHandler();
    }

    protected AcceptCallback getAcceptCallback(Instance instance, ParticipantInfo participantInfo)
    {
        return new AcceptCallback(instance, participantInfo);
    }

    public void accept(UUID iid, AcceptDecision decision) throws InvalidInstanceStateChange, UnavailableException, WriteTimeoutException, BallotException
    {
        logger.debug("accepting instance {}", iid);

        // get missing instances
        Map<InetAddress, List<Instance>> missingInstances = new HashMap<>();
        for (Map.Entry<InetAddress, Set<UUID>> entry: decision.missingInstances.entrySet())
        {
            missingInstances.put(entry.getKey(), getInstanceCopies(entry.getValue()));
        }

        ReadWriteLock lock = locks.get(iid);
        lock.writeLock().lock();
        AcceptCallback callback;
        Instance instance;
        try
        {
            instance = loadInstance(iid);

            instance.accept(decision.acceptDeps);
            instance.incrementBallot();
            saveInstance(instance);

            ParticipantInfo participantInfo = getParticipants(instance);

            callback = getAcceptCallback(instance, participantInfo);
            for (InetAddress endpoint : participantInfo.liveEndpoints)
            {
                if (!endpoint.equals(getEndpoint()))
                {
                    List<Instance> missing = missingInstances.get(endpoint);
                    missing = missing != null ? missing : Collections.EMPTY_LIST;
                    AcceptRequest request = new AcceptRequest(instance, missing);  // FIXME: deadlocks
                    MessageOut<AcceptRequest> message = new MessageOut<>(MessagingService.Verb.EPAXOS_ACCEPT,
                                                                         request,
                                                                         AcceptRequest.serializer);
                    logger.debug("sending accept request to {} for instance {}", endpoint, instance.getId());
                    sendRR(message, endpoint, callback);
                }
                else
                {
                    logger.debug("counting self in accept quorum for instance {}", instance.getId());
                    callback.countLocal();
                }
            }

            // send to remote datacenters for LOCAL_SERIAL queries
            for (InetAddress endpoint: participantInfo.remoteEndpoints)
            {
                AcceptRequest request = new AcceptRequest(instance, Collections.EMPTY_LIST);
                MessageOut<AcceptRequest> message = new MessageOut<>(MessagingService.Verb.EPAXOS_ACCEPT,
                                                                     request,
                                                                     AcceptRequest.serializer);
                logger.debug("sending accept request to non-local dc {} for instance {}", endpoint, instance.getId());
                sendOneWay(message, endpoint);
            }

            recordAcknowledgedDeps(instance);
        }
        finally
        {
            lock.writeLock().unlock();
        }

        callback.await();
        callback.checkSuccess();
        logger.debug("accept phase successful for {}", iid);
    }

    protected class AcceptVerbHandler implements IVerbHandler<AcceptRequest>
    {
        @Override
        public void doVerb(MessageIn<AcceptRequest> message, int id)
        {
            Instance remoteInstance = message.payload.instance;
            logger.debug("Accept request received from {} for {}", message.from, remoteInstance.getId());

            for (Instance missing: message.payload.missingInstances)
            {
                if (!missing.getId().equals(message.payload.instance.getId()))
                {
                    addMissingInstance(missing);
                }
            }

            ReadWriteLock lock = locks.get(remoteInstance.getId());
            lock.writeLock().lock();
            Instance instance = null;
            try
            {
                instance = loadInstance(remoteInstance.getId());
                if (instance == null)
                {
                    instance = remoteInstance.copyRemote();
                    recordMissingInstance(instance);
                } else
                {
                    instance.checkBallot(remoteInstance.getBallot());
                    instance.applyRemote(remoteInstance);
                }
                instance.accept(remoteInstance.getDependencies());
                saveInstance(instance);

                AcceptResponse response = new AcceptResponse(true, 0);
                MessageOut<AcceptResponse> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE,
                                                                    response,
                                                                    AcceptResponse.serializer);
                logger.debug("Accept request from {} successful for {}", message.from, remoteInstance.getId());
                sendReply(reply, id, message.from);

                recordAcknowledgedDeps(instance);
            }
            catch (BallotException e)
            {
                logger.debug("Accept request from {} for {}, rejected. Old ballot", message.from, remoteInstance.getId());
                AcceptResponse response = new AcceptResponse(false, e.localBallot);
                MessageOut<AcceptResponse> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE,
                                                                    response,
                                                                    AcceptResponse.serializer);
                sendReply(reply, id, message.from);
            }
            catch (InvalidInstanceStateChange e)
            {
                // another node is working on a prepare phase that this node wasn't involved in.
                // as long as the dependencies are the same, reply with an ok, otherwise, something
                // has gone wrong
                assert instance.getDependencies().equals(remoteInstance.getDependencies());

                logger.debug("Accept request from {} for {}, rejected. State demotion", message.from, remoteInstance.getId());
                AcceptResponse response = new AcceptResponse(true, 0);
                MessageOut<AcceptResponse> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE,
                                                                    response,
                                                                    AcceptResponse.serializer);
                sendReply(reply, id, message.from);
            }
            finally
            {
                lock.writeLock().unlock();
            }
        }
    }

    public IVerbHandler<AcceptRequest> getAcceptVerbHandler()
    {
        return new AcceptVerbHandler();
    }

    /**
     * notifies threads waiting on this commit
     *
     * must be called with this instances's write lock held
     */
    private void notifyCommit(UUID iid)
    {
        CountDownLatch commitLatch = commitLatches.get(iid);
        if (commitLatch != null)
        {
            commitLatch.countDown();
            commitLatches.remove(iid);
        }
    }

    /**
     * Returns a commit latch for an instance
     *
     * must be called with the instance's lock held, and only
     * if the instance was not committed
     */
    private CountDownLatch getCommitLatch(UUID iid)
    {
        CountDownLatch commitLatch = commitLatches.get(iid);
        if (commitLatch == null)
        {
            commitLatch = new CountDownLatch(1);
            CountDownLatch previous = commitLatches.putIfAbsent(iid, commitLatch);
            if (previous != null)
                commitLatch = previous;
        }

        return commitLatch;
    }

    public void commit(UUID iid, Set<UUID> dependencies) throws InvalidInstanceStateChange, UnavailableException
    {
        logger.debug("committing instance {}", iid);
        ReadWriteLock lock = locks.get(iid);
        lock.writeLock().lock();
        Instance instance;
        try
        {
            instance = loadInstance(iid);
            instance.commit(dependencies);
            instance.incrementBallot();
            saveInstance(instance);

            ParticipantInfo participantInfo = getParticipants(instance);
            MessageOut<Instance> message = new MessageOut<Instance>(MessagingService.Verb.EPAXOS_COMMIT,
                                                                    instance,
                                                                    Instance.serializer);
            for (InetAddress endpoint : participantInfo.liveEndpoints)
            {
                if (!endpoint.equals(getEndpoint()))
                {
                    logger.debug("sending commit request to {} for instance {}", endpoint, instance.getId());
                    sendOneWay(message, endpoint);
                }
            }

            // send to remote datacenters for LOCAL_SERIAL queries
            for (InetAddress endpoint: participantInfo.remoteEndpoints)
            {
                logger.debug("sending commit request to non-local dc {} for instance {}", endpoint, instance.getId());
                sendOneWay(message, endpoint);
            }

            recordAcknowledgedDeps(instance);

            notifyCommit(iid);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    protected class CommitVerbHandler implements IVerbHandler<Instance>
    {
        @Override
        public void doVerb(MessageIn<Instance> message, int id)
        {
            logger.debug("Commit request received from {} for {}", message.from, message.payload.getId());
            ReadWriteLock lock = locks.get(message.payload.getId());
            lock.writeLock().lock();
            Instance instance;
            try
            {
                Instance remoteInstance = message.payload;
                instance = loadInstance(remoteInstance.getId());
                if (instance == null)
                {
                    // TODO: record deps
                    instance = remoteInstance.copyRemote();
                } else
                {
                    instance.applyRemote(remoteInstance);
                }
                instance.commit(remoteInstance.getDependencies());
                saveInstance(instance);

                notifyCommit(instance.getId());
                recordAcknowledgedDeps(instance);
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
                // TODO: send this to another thread
                execute(message.payload.getId());
            }
            catch (BallotException e)
            {
                // this happens when a prepare phase loses the fight for control of an instance
            }
            catch (UnavailableException | RequestTimeoutException | InvalidRequestException e)
            {
                logger.error("Error executing instance", e);
            }
        }
    }

    public IVerbHandler<Instance> getCommitVerbHandler()
    {
        return new CommitVerbHandler();
    }

    public void execute(UUID instanceId) throws UnavailableException, BallotException, InvalidRequestException, RequestTimeoutException
    {
        logger.debug("Running execution phase for instance {}", instanceId);
        ReadWriteLock lock = locks.get(instanceId);

        while (true)
        {
            ExecutionSorter executionSorter;
            lock.writeLock().lock();
            try
            {
                Instance instance = loadInstance(instanceId);
                assert instance.getState().atLeast(Instance.State.COMMITTED);
                executionSorter = new ExecutionSorter(instance, accessor);
                executionSorter.buildGraph();
            }
            finally
            {
                lock.writeLock().unlock();
            }

            if (executionSorter.uncommitted.size() > 0)
            {
                logger.debug("Uncommitted ({}) instances found while attempting to execute {}",
                             executionSorter.uncommitted.size(), instanceId);
                for (UUID iid : executionSorter.uncommitted)
                {
                    try
                    {
                        prepare(iid);
                    }
                    catch (InstanceNotFoundException e)
                    {
                        // this is only a problem if there's only a single uncommitted instance
                        if (executionSorter.uncommitted.size() > 1)
                            continue;
                        // lets see if progress can still be made
                        logger.error("Missing instance for prepare: ", e);
                    }
                }
            }
            else
            {
                for (UUID iid : executionSorter.getOrder())
                {
                    lock.writeLock().lock();
                    Instance toExecute = loadInstance(iid);
                    try
                    {
                        if (toExecute.getState() == Instance.State.EXECUTED)
                        {
                            if (toExecute.getId().equals(instanceId))
                            {
                                return;
                            }
                            else
                            {
                                continue;
                            }
                        }

                        assert toExecute.getState() == Instance.State.COMMITTED;

                        executeInstance(toExecute);
                        toExecute.setExecuted();
                        saveInstance(toExecute);

                        if (toExecute.getId().equals(instanceId))
                            return;

                    }
                    finally
                    {
                        lock.writeLock().unlock();
                    }
                }
                return;
            }
        }
    }

    protected void executeInstance(Instance instance) throws InvalidRequestException, ReadTimeoutException, WriteTimeoutException
    {
        logger.debug("Executing serialized request for {}", instance.getId());
        ColumnFamily result = instance.execute();
        recordExecuted(instance);
        SettableFuture resultFuture = resultFutures.get(instance.getId());
        if (resultFuture != null)
        {
            resultFuture.set(result);
            resultFutures.remove(instance.getId());
        }
    }

    protected PrepareCallback getPrepareCallback(Instance instance, ParticipantInfo participantInfo)
    {
        return new PrepareCallback(instance, participantInfo);
    }

    // TODO: this might be better as a runnable we can throw into a thread pool
    public void prepare(UUID iid) throws UnavailableException, WriteTimeoutException, BallotException, InstanceNotFoundException
    {
        Lock prepareLock = prepareLocks.get(iid);
        logger.debug("Aquiring lock for {} prepare phase", iid);
        prepareLock.lock();
        logger.debug("Running prepare phase for {}", iid);
        try
        {
            for (int i=0; i<PREPARE_BALLOT_FAILURE_RETRIES; i++)
            {
                if (!shouldPrepare(iid))
                    return;

                PrepareCallback callback;
                ParticipantInfo participantInfo;

                ReadWriteLock lock = locks.get(iid);
                lock.writeLock().lock();
                try
                {
                    Instance instance = loadInstance(iid);
                    if (instance.getState().atLeast(Instance.State.COMMITTED))
                        return;
                    instance.incrementBallot();
                    saveInstance(instance);

                    participantInfo = getParticipants(instance);
                    PrepareRequest request = new PrepareRequest(instance);
                    MessageOut<PrepareRequest> message = new MessageOut<>(MessagingService.Verb.EPAXOS_PREPARE,
                                                                          request,
                                                                          PrepareRequest.serializer);
                    callback = getPrepareCallback(instance, participantInfo);
                    for (InetAddress endpoint : participantInfo.liveEndpoints)
                        if (!endpoint.equals(getEndpoint()))
                        {
                            sendRR(message, endpoint, callback);
                        }
                        else
                        {
                            // only provide a response if this instance is not a placeholder
                            callback.countLocal(getEndpoint(), (!instance.isPlaceholder() ? instance : null));
                        }
                }
                finally
                {
                    lock.writeLock().unlock();
                }

                callback.await();

                PrepareDecision decision = callback.getDecision();
                logger.debug("PrepareDecision for {}: {}", iid, decision);

                if (decision.commitNoop)
                {
                    lock.writeLock().lock();
                    try
                    {
                        logger.debug("setting noop flag for {}", iid);
                        Instance instance = loadInstance(iid);
                        if (instance.getState().atLeast(Instance.State.COMMITTED))
                            return;
                        instance.setNoop(true);
                        saveInstance(instance);
                    }
                    finally
                    {
                        lock.writeLock().unlock();
                    }
                }

                // perform try preaccept, if applicable
                if (decision.state == Instance.State.PREACCEPTED && decision.tryPreacceptAttempts.size() > 0)
                {
                    logger.debug("running prepare phase trypreaccept for {}", iid);
                    for (TryPreacceptAttempt attempt: decision.tryPreacceptAttempts)
                    {
                        try
                        {
                            if (tryPreaccept(iid, attempt, participantInfo))
                            {
                                // if the attempt was successful, the next step is the accept
                                // phase with the successful attempt's dependencies
                                decision = new PrepareDecision(Instance.State.ACCEPTED,
                                                               attempt.dependencies,
                                                               Collections.EMPTY_LIST,
                                                               decision.commitNoop);
                                break;
                            }
                        }
                        catch (PrepareAbortException e)
                        {
                            // just return quietly, this instance will get
                            // picked up for repair after the others
                            logger.debug("Prepare aborted: " + e.getMessage());
                            return;
                        }
                    }
                }

                lock.writeLock().lock();
                Set<UUID> deps = decision.deps;
                try
                {
                    AcceptDecision acceptDecision = null;
                    switch (decision.state)
                    {
                        case PREACCEPTED:
                            logger.debug("running prepare phase preaccept for {}", iid);
                            Instance instance = loadInstance(iid);
                            acceptDecision = preaccept(instance);

                            // reassigning will transmit any missing dependencies
                            deps = acceptDecision.acceptDeps;

                        case ACCEPTED:
                            logger.debug("running prepare phase accept for {}", iid);
                            assert deps != null;
                            acceptDecision = acceptDecision != null ? acceptDecision
                                    : new AcceptDecision(true, deps, Collections.EMPTY_MAP);
                            accept(iid, acceptDecision);
                        case COMMITTED:
                            logger.debug("running prepare phase commit for {}", iid);
                            assert deps != null;
                            commit(iid, deps);
                            break;
                        default:
                            throw new AssertionError();
                    }
                }
                catch (BallotException e)
                {
                    logger.debug("Prepare phase ballot failure for {}", iid);
                    if (i >= PREPARE_BALLOT_FAILURE_RETRIES)
                    {
                        throw e;
                    }
                    else
                    {
                        continue;
                    }
                }
                catch (InvalidInstanceStateChange e)
                {
                    throw new AssertionError(e);
                }
                finally
                {
                    lock.writeLock().unlock();
                }

                return;
            }
        }
        finally
        {
            prepareLock.unlock();
            logger.debug("Released lock for {} prepare phase", iid);
        }
    }

    protected int getPrepareWaitTime(UUID iid, InetAddress leader, List<InetAddress> successors)
    {
        // TODO: successors should notify the first successor that a prepare is required instead of waiting it out
        // TODO: wait time should be based off last updated, not created
        int successorIndex = successors.indexOf(getEndpoint()) + 1;
        if (successorIndex < 1 && !leader.equals(getEndpoint()))
            successorIndex = successors.size();

        int waitTime = PREPARE_GRACE_MILLIS * successorIndex;
        // subtract time elapsed since instance creation
        waitTime -= Math.max((System.currentTimeMillis() - UUIDGen.unixTimestamp(iid)), 0);
        waitTime = Math.max(waitTime, 0);
        return waitTime;
    }

    protected boolean shouldPrepare(UUID iid) throws InstanceNotFoundException
    {
        CountDownLatch commitLatch;

        ReadWriteLock lock = locks.get(iid);
        lock.readLock().lock();
        int waitTime;
        try
        {
            Instance instance = loadInstance(iid);

            if (instance == null)
                throw new InstanceNotFoundException(iid);

            if (instance.getState() == Instance.State.COMMITTED || instance.getState() == Instance.State.EXECUTED)
            {
                logger.debug("Skipping prepare for instance {}. It's been committed or executed", iid);
                return false;
            }

            waitTime = getPrepareWaitTime(iid, instance.getLeader(), instance.getSuccessors());
            commitLatch = getCommitLatch(iid);
        }
        finally
        {
            lock.readLock().unlock();
        }

        try
        {
            logger.debug("Waiting on commit or grace period expiration for {} ms before preparing {}.", waitTime, iid);
            boolean committed = !commitLatch.await(waitTime, TimeUnit.MILLISECONDS);
            if (!committed)
            {
                logger.debug("Instance was committed before grace period expired for {}.", iid);
            }
            return committed;
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
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

                // we can't participate in the prepare phase if our
                // local copy of the instance is a placeholder
                if (instance != null && instance.isPlaceholder())
                    instance = null;

                if (instance != null)
                {
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
                }

                MessageOut<Instance> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE,
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

    protected TryPreacceptCallback getTryPreacceptCallback(UUID iid, TryPreacceptAttempt attempt, ParticipantInfo participantInfo)
    {
        return new TryPreacceptCallback(iid, attempt, participantInfo);
    }

    public boolean tryPreaccept(UUID iid, TryPreacceptAttempt attempt, ParticipantInfo participantInfo) throws WriteTimeoutException, PrepareAbortException
    {
        TryPreacceptRequest request = new TryPreacceptRequest(iid, attempt.dependencies);
        MessageOut<TryPreacceptRequest> message = new MessageOut<>(MessagingService.Verb.EPAXOS_TRYPREACCEPT,
                                                                   request,
                                                                   TryPreacceptRequest.serializer);
        TryPreacceptCallback callback = getTryPreacceptCallback(iid, attempt, participantInfo);
        for (InetAddress endpoint: attempt.toConvince)
        {
            if (!endpoint.equals(getEndpoint()))
            {
                sendRR(message, endpoint, callback);
            }
            else
            {
                ReadWriteLock lock = locks.get(iid);
                lock.writeLock().lock();
                try
                {
                    Instance instance = loadInstance(iid);
                    callback.recordDecision(handleTryPreaccept(instance, attempt.dependencies));
                }
                finally
                {
                    lock.writeLock().unlock();
                }
            }
        }

        callback.await();
        return callback.successful();
    }

    // FIXME: this is deadlocking
    protected TryPreacceptDecision handleTryPreaccept(Instance instance, Set<UUID> dependencies)
    {
        // TODO: reread the try preaccept stuff, dependency management may break it if we're not careful
        logger.debug("Attempting TryPreaccept for {} with deps {}", instance.getId(), dependencies);

        // get the ids of instances the the message instance doesn't have in it's dependencies
        Set<UUID> conflictIds = Sets.newHashSet(getCurrentDependencies(instance));
        conflictIds.removeAll(dependencies);
        conflictIds.remove(instance.getId());

        // if this node hasn't seen some of the proposed dependencies, don't preaccept them
        for (UUID dep: dependencies)
        {
            if (loadInstance(dep) == null)
            {
                logger.debug("Missing dep for TryPreaccept for {}, rejecting ", instance.getId());
                return TryPreacceptDecision.REJECTED;
            }
        }

        for (UUID id: conflictIds)
        {
            if (id.equals(instance.getId()))
                continue;

            Instance conflict = loadInstance(id);

            if (!conflict.getState().isCommitted())
            {
                // requiring the potential conflict to be committed can cause
                // an infinite prepare loop, so we just abort this prepare phase.
                // This instance will get picked up again for explicit prepare after
                // the other instances being prepared are successfully committed. Hopefully
                // this conflicting instance will be committed as well.
                logger.debug("TryPreaccept contended for {}, {} is not committed", instance.getId(), conflict.getId());
                return TryPreacceptDecision.CONTENDED;
            }

            // if the instance in question isn't a dependency of the potential
            // conflict, then it couldn't have been committed on the fast path
            if (!conflict.getDependencies().contains(instance.getId()))
            {
                logger.debug("TryPreaccept rejected for {}, not a dependency of conflicting instance", instance.getId());
                return TryPreacceptDecision.REJECTED;
            }
        }

        logger.debug("TryPreaccept accepted for {}, with deps", instance.getId(), dependencies);
        // set dependencies on this instance
        assert instance.getState() == Instance.State.PREACCEPTED;
        instance.setDependencies(dependencies);
        saveInstance(instance);

        return TryPreacceptDecision.ACCEPTED;
    }

    protected class TryPreacceptVerbHandler implements IVerbHandler<TryPreacceptRequest>
    {
        @Override
        public void doVerb(MessageIn<TryPreacceptRequest> message, int id)
        {
            logger.debug("TryPreaccept message received from {} for {}", message.from, message.payload.iid);
            ReadWriteLock lock = locks.get(message.payload.iid);
            lock.writeLock().lock();
            try
            {
                Instance instance = loadInstance(message.payload.iid);
                TryPreacceptDecision decision = handleTryPreaccept(instance, message.payload.dependencies);
                TryPreacceptResponse response = new TryPreacceptResponse(instance.getId(), decision);
                MessageOut<TryPreacceptResponse> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE,
                                                                          response,
                                                                          TryPreacceptResponse.serializer);
                sendReply(reply, id, message.from);
            }
            finally
            {
                lock.writeLock().unlock();
            }
        }
    }

    public IVerbHandler<TryPreacceptRequest> getTryPreacceptVerbHandler()
    {
        return new TryPreacceptVerbHandler();
    }

    protected Instance loadInstance(UUID instanceId)
    {
        logger.debug("Loading instance {}", instanceId);

        Instance instance = instanceCache.getIfPresent(instanceId);
        if (instance != null)
            return instance;

        logger.debug("Loading instance {} from disk", instanceId);
        // read from table
        String query = "SELECT * FROM %s.%s WHERE id=?";
        UntypedResultSet results = QueryProcessor.executeInternal(String.format(query, keyspace(), instanceTable()),
                                                                  instanceId);
        if (results.isEmpty())
            return null;

        UntypedResultSet.Row row = results.one();

        ByteBuffer data = row.getBlob("data");
        assert data.hasArray();  // FIXME
        DataInput in = ByteStreams.newDataInput(data.array(), data.position());
        try
        {
            instance = Instance.internalSerializer.deserialize(in, row.getInt("version"));
            instanceCache.put(instanceId, instance);
            return instance;
        }
        catch (IOException e)
        {
            throw new AssertionError(e);  // TODO: propagate original exception?
        }
    }

    protected void saveInstance(Instance instance)
    {
        logger.debug("Saving instance {}", instance.getId());
        assert instance.getState().atLeast(Instance.State.PREACCEPTED);
        instance.setLastUpdated();
        DataOutputBuffer out = new DataOutputBuffer((int) Instance.internalSerializer.serializedSize(instance, 0));
        try
        {
            Instance.internalSerializer.serialize(instance, out, 0);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);  // TODO: propagate original exception?
        }
        long timestamp = System.currentTimeMillis();
        String instanceReq = "INSERT INTO %s.%s (id, data, version) VALUES (?, ?, ?) USING TIMESTAMP ?";
        QueryProcessor.executeInternal(String.format(instanceReq, keyspace(), instanceTable()),
                                       instance.getId(),
                                       ByteBuffer.wrap(out.getData()),
                                       0,
                                       timestamp);

        instanceCache.put(instance.getId(), instance);
    }

    /**
     * loads a dependency manager. Must be called within a lock held for this key & cfid pair
     */
    protected DependencyManager loadDependencyManager(ByteBuffer key, UUID cfId)
    {
        Pair<ByteBuffer, UUID> keyPair = Pair.create(key, cfId);
        DependencyManager dm = dependencyManagers.getIfPresent(keyPair);
        if (dm != null)
            return dm;

        String query = "SELECT * FROM %s.%s WHERE row_key=? AND cf_id=?";
        UntypedResultSet results = QueryProcessor.executeInternal(String.format(query, keyspace(), dependencyTable()), key, cfId);

        if (results.isEmpty())
        {
            dm = new DependencyManager();
            dependencyManagers.put(keyPair, dm);
            return dm;
        }

        UntypedResultSet.Row row = results.one();

        ByteBuffer data = row.getBlob("data");
        assert data.hasArray();  // FIXME
        DataInput in = ByteStreams.newDataInput(data.array(), data.position());
        try
        {
            dm = DependencyManager.serializer.deserialize(in, 0);
            dependencyManagers.put(keyPair, dm);
            return dm;

        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    /**
     * persists a dependency manager. Must be called within a lock held for this key & cfid pair
     */
    private void saveDependencyManager(ByteBuffer key, UUID cfId, DependencyManager dm)
    {

        DataOutputBuffer out = new DataOutputBuffer((int) DependencyManager.serializer.serializedSize(dm, 0));
        try
        {
            DependencyManager.serializer.serialize(dm, out, 0);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);  // TODO: propagate original exception?
        }
        String depsReq = "INSERT INTO %s.%s (row_key, cf_id, data) VALUES (?, ?, ?)";
        QueryProcessor.executeInternal(String.format(depsReq, keyspace(), dependencyTable()),
                                       key,
                                       cfId,
                                       ByteBuffer.wrap(out.getData()));

        dependencyManagers.put(Pair.create(key, cfId), dm);
    }

    protected Instance addMissingInstance(Instance remoteInstance)
    {
        logger.debug("Adding missing instance: {}", remoteInstance.getId());
        ReadWriteLock lock = locks.get(remoteInstance.getId());
        lock.writeLock().lock();
        try
        {
            Instance instance = loadInstance(remoteInstance.getId());
            if (instance != null)
                return instance;


            // TODO: guard against re-adding expired instances once we start gc'ing them
            instance = remoteInstance.copyRemote();
            // be careful if the instance is only preaccepted
            // if a preaccepted instance from another node is blindly added,
            // it can cause problems during the prepare phase
            if (!instance.getState().atLeast(Instance.State.ACCEPTED))
            {
                logger.debug("Setting {} as placeholder", remoteInstance.getId());
                instance.setDependencies(null);
                instance.setPlaceholder(true);  // TODO: exclude from prepare and trypreaccept
            }
            saveInstance(instance);

            if (instance.getState().atLeast(Instance.State.COMMITTED))
                notifyCommit(instance.getId());

            recordMissingInstance(instance);

            // TODO: execute if committed

            return instance;

        }
        finally
        {
            lock.writeLock().unlock();
        }
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
