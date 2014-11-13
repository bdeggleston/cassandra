package org.apache.cassandra.service.epaxos;

import com.google.common.base.Predicate;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.*;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Striped;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.concurrent.TracingAwareExecutorService;
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
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
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

public class EpaxosState
{
    public static final EpaxosState instance = new EpaxosState();

    private static final Logger logger = LoggerFactory.getLogger(EpaxosState.class);

    private static final List<InetAddress> NO_ENDPOINTS = ImmutableList.of();

    // TODO: put these in DatabaseDescriptor

    // the amount of time the prepare phase will wait for the leader to commit an instance before
    // attempting a prepare phase. This is multiplied by a replica's position in the successor list
    protected static long PREPARE_GRACE_MILLIS = DatabaseDescriptor.getMinRpcTimeout() / 2;

    private static boolean CACHE = Boolean.getBoolean("cassandra.epaxos.cache");

    private final Striped<ReadWriteLock> locks = Striped.readWriteLock(DatabaseDescriptor.getConcurrentWriters() * 1024);
    private final Cache<UUID, Instance> instanceCache;

    // prevents multiple threads from attempting to prepare the same instance. Aquire this before an instance lock
    private final ConcurrentMap<UUID, PrepareGroup> prepareGroups = Maps.newConcurrentMap();

    // prevents multiple threads modifying the dependency manager for a given key. Aquire this after locking an instance
    private final Striped<Lock> depsLocks = Striped.lock(DatabaseDescriptor.getConcurrentWriters() * 1024);
    private final Cache<Pair<ByteBuffer, UUID>, DependencyManager> dependencyManagers;

    // aborts prepare phases on commit
    private final ConcurrentMap<UUID, List<ICommitCallback>> commitCallbacks = Maps.newConcurrentMap();

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

        public List<InetAddress> getSuccessors()
        {
            List<InetAddress> successors = Lists.newArrayList(Iterables.filter(endpoints, nonLocalPredicate));
            Collections.shuffle(successors, getRandom());
            return successors;
        }
    }

    public EpaxosState()
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

    protected long getQueryTimeout(long start)
    {
        return Math.max(1, DatabaseDescriptor.getRpcTimeout() - (System.currentTimeMillis() - start));
    }

    /**
     * Initiates an epaxos instance and waits for the result to become available
     */
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
            preaccept(instance);

            return resultFuture.get(getQueryTimeout(start), TimeUnit.MILLISECONDS);
        }
        catch (ExecutionException | TimeoutException e)
        {
            throw new WriteTimeoutException(WriteType.CAS, query.getConsistencyLevel(), 0, 1);
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

    /**
     * Creates a new instance, setting this node as the leader
     */
    protected Instance createInstance(SerializedRequest request)
    {
        return new Instance(request, getEndpoint());
    }

    protected Set<UUID> getCurrentDependencies(Instance instance)
    {
        SerializedRequest request = instance.getQuery();

        Pair<ByteBuffer, UUID> keyPair = Pair.create(request.getKey(),
                                                     Schema.instance.getId(request.getKeyspaceName(), request.getCfName()));

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

    protected PreacceptCallback getPreacceptCallback(Instance instance, ParticipantInfo participantInfo, Runnable failureCallback, boolean forceAccept)
    {
        return new PreacceptCallback(this, instance, participantInfo, failureCallback, forceAccept);
    }

    public void preacceptPrepare(UUID id, boolean noop, Runnable failureCallback)
    {
        logger.debug("running preaccept prepare for {}", id);
        PreacceptTask task = new PreacceptTask.Prepare(this, id, noop, failureCallback);
        getStage(Stage.MUTATION).submit(task);
    }

    public void preaccept(Instance instance)
    {
        PreacceptTask task = new PreacceptTask.Leader(this, instance);
        getStage(Stage.MUTATION).submit(task);
    }

    public IVerbHandler<Instance> getPreacceptVerbHandler()
    {
        return new PreacceptVerbHandler(this);
    }

    protected AcceptCallback getAcceptCallback(Instance instance, ParticipantInfo participantInfo, Runnable failureCallback)
    {
        return new AcceptCallback(this, instance, participantInfo, failureCallback);
    }

    public void accept(UUID id, Set<UUID> dependencies, Runnable failureCallback)
    {
        accept(id, new AcceptDecision(true, dependencies, Collections.<InetAddress, Set<UUID>>emptyMap()), failureCallback);
    }

    public void accept(UUID iid, AcceptDecision decision, Runnable failureCallback)
    {
        logger.debug("accepting instance {}", iid);

        // get missing instances
        Map<InetAddress, List<Instance>> missingInstances = new HashMap<>();
        for (Map.Entry<InetAddress, Set<UUID>> entry: decision.missingInstances.entrySet())
        {
            missingInstances.put(entry.getKey(), Lists.newArrayList(Iterables.filter(getInstanceCopies(entry.getValue()),
                                                                                     Instance.skipPlaceholderPredicate)));
        }

        Instance instance = getInstanceCopy(iid);
        instance.setDependencies(decision.acceptDeps);
        instance.incrementBallot();

        ParticipantInfo participantInfo;
        try
        {
            // TODO: should we check instance status, and bail out / call the callback if the instance is committed?
            participantInfo = getParticipants(instance);
        }
        catch (UnavailableException e)
        {
            throw new RuntimeException(e);
        }

        AcceptCallback callback = getAcceptCallback(instance, participantInfo, failureCallback);
        for (InetAddress endpoint : participantInfo.liveEndpoints)
        {
            logger.debug("sending accept request to {} for instance {}", endpoint, instance.getId());
            AcceptRequest request = new AcceptRequest(instance, missingInstances.get(endpoint));
            sendRR(request.getMessage(), endpoint, callback);
        }

        // send to remote datacenters for LOCAL_SERIAL queries
        for (InetAddress endpoint: participantInfo.remoteEndpoints)
        {
            logger.debug("sending accept request to non-local dc {} for instance {}", endpoint, instance.getId());
            AcceptRequest request = new AcceptRequest(instance, null);
            sendOneWay(request.getMessage(), endpoint);
        }
    }

    public IVerbHandler<AcceptRequest> getAcceptVerbHandler()
    {
        return new AcceptVerbHandler(this);
    }

    /**
     * notifies threads waiting on this commit
     *
     * must be called with this instances's write lock held
     */
    public void notifyCommit(UUID id)
    {
        List<ICommitCallback> callbacks = commitCallbacks.get(id);
        if (callbacks != null)
        {
            for (ICommitCallback cb: callbacks)
            {
                cb.instanceCommitted(id);
            }
        }
    }

    /**
     * Registers an implementation of ICommitCallback to be notified when an instance is committed
     *
     * The caller should have the instances read or write lock held when registering.
     */
    public void registerCommitCallback(UUID id, ICommitCallback callback)
    {
        // TODO: should we check instance status, and bail out / call the callback if the instance is committed?
        List<ICommitCallback> callbacks = commitCallbacks.get(id);
        if (callbacks == null)
        {
            callbacks = new LinkedList<>();
            List<ICommitCallback> previous = commitCallbacks.putIfAbsent(id, callbacks);
            if (previous != null)
                callbacks = previous;
        }
        callbacks.add(callback);
    }

    public void commit(UUID iid, Set<UUID> dependencies)
    {
        logger.debug("committing instance {}", iid);
        try
        {
            Instance instance = getInstanceCopy(iid);
            instance.incrementBallot();

            // FIXME: unavailable exception shouldn't be thrown by getParticipants. It will prevent instances from being committed locally
            ParticipantInfo participantInfo = getParticipants(instance);
            MessageOut<Instance> message = instance.getMessage(MessagingService.Verb.EPAXOS_COMMIT);
            for (InetAddress endpoint : participantInfo.liveEndpoints)
            {
                logger.debug("sending commit request to {} for instance {}", endpoint, instance.getId());
                sendOneWay(message, endpoint);
            }

            // send to remote datacenters for LOCAL_SERIAL queries
            for (InetAddress endpoint: participantInfo.remoteEndpoints)
            {
                logger.debug("sending commit request to non-local dc {} for instance {}", endpoint, instance.getId());
                sendOneWay(message, endpoint);
            }
        }
        catch (UnavailableException e)
        {
            throw new RuntimeException(e);
        }
    }

    public IVerbHandler<Instance> getCommitVerbHandler()
    {
        return new CommitVerbHandler(this);
    }

    public void execute(UUID instanceId)
    {
        getStage(Stage.MUTATION).submit(new ExecuteTask(this, instanceId));
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

    protected PrepareCallback getPrepareCallback(Instance instance, ParticipantInfo participantInfo, PrepareGroup group)
    {
        return new PrepareCallback(this, instance, participantInfo, group);
    }

    public PrepareTask prepare(UUID id, PrepareGroup group)
    {
        PrepareTask task = new PrepareTask(this, id, group);
        getStage(Stage.READ).submit(task);
        return task;
    }

    protected long getPrepareWaitTime(long lastUpdate)
    {
        long prepareAt = lastUpdate + PREPARE_GRACE_MILLIS;
        return Math.max(prepareAt - System.currentTimeMillis(), 0);
    }

    public IVerbHandler<PrepareRequest> getPrepareVerbHandler()
    {
        return new PrepareVerbHandler(this);
    }

    public PrepareGroup registerPrepareGroup(UUID id, PrepareGroup group)
    {
        return prepareGroups.putIfAbsent(id, group);
    }

    public void unregisterPrepareGroup(UUID id)
    {
        prepareGroups.remove(id);
    }

    protected TryPrepareCallback getTryPrepareCallback()
    {
        return new TryPrepareCallback();
    }

    /**
     * If we rely only on a timeout to determine when a node should prepare an instance,
     * they'll all start trying to prepare at once, usually causing a period of livelock.
     * To prevent this, an ordered list of successors are specified at instance creation.
     * When a node thinks an instance needs to be prepared, it first asks a successor instance
     * to prepare it, then waits for it to prepare the instance
     */
    public void tryprepare(InetAddress endpoint, UUID iid)
    {

    }

    protected TryPrepareVerbHandler getTryPrepareVerbHandler()
    {
        return new TryPrepareVerbHandler(this);
    }


    protected TryPreacceptCallback getTryPreacceptCallback(UUID iid, TryPreacceptAttempt attempt, List<TryPreacceptAttempt> nextAttempts, ParticipantInfo participantInfo, Runnable failureCallback)
    {
        return new TryPreacceptCallback(this, iid, attempt, nextAttempts, participantInfo, failureCallback);
    }

    /**
     * The TryPreaccept phase is the part of failure recovery that makes committing on the
     * fast path of F + ((F + 1) / 2) possible.
     *
     * The test case EpaxosIntegrationRF3Test.inferredFastPathFailedLeaderRecovery demonstrates
     * a scenario that you can't get out of without first running a TryPreaccept.
     *
     * TryPreaccept is basically determining if an instance could have been committed on the fast
     * path, based on responses from only a quorum of replicas.
     *
     * If the highest instance state a prepare phase encounters is PREACCEPTED, PrepareCallback.getTryPreacceptAttempts
     * will identify any dependency sets that are shared by at least (F + 1) / 2 replicas, who are not the command
     * leader.
     *
     * The preparer will then try to convince the other replicas to preaccept this group of dependencies for the instance
     * being prepared. The other replicas will preaccept with the suggested dependencies if all of their active instances
     * are either in the preparing instance's dependencies or have the preparing instance in theirs.
     */
    public void tryPreaccept(UUID iid, List<TryPreacceptAttempt> attempts, ParticipantInfo participantInfo, Runnable failureCallback)
    {
        assert attempts.size() > 0;
        TryPreacceptAttempt attempt = attempts.get(0);
        List<TryPreacceptAttempt> nextAttempts = attempts.subList(1, attempts.size());

        logger.debug("running trypreaccept prepare for {}: {}", iid, attempt);

        TryPreacceptRequest request = new TryPreacceptRequest(iid, attempt.dependencies);
        MessageOut<TryPreacceptRequest> message = request.getMessage();

        TryPreacceptCallback callback = getTryPreacceptCallback(iid, attempt, nextAttempts, participantInfo, failureCallback);
        for (InetAddress endpoint: attempt.toConvince)
        {
            logger.debug("sending trypreaccept request to {} for instance {}", endpoint, iid);
            sendRR(message, endpoint, callback);
        }
    }

    public IVerbHandler<TryPreacceptRequest> getTryPreacceptVerbHandler()
    {
        return new TryPreacceptVerbHandler(this);
    }

    public void updateBallot(UUID id, int ballot, Runnable callback)
    {
        getStage(Stage.MUTATION).submit(new BallotUpdateTask(this, id, ballot, callback));
    }

    public Instance getInstanceCopy(UUID id)
    {
        ReadWriteLock lock = getInstanceLock(id);
        lock.readLock().lock();
        try
        {
            Instance instance = loadInstance(id);
            return instance != null ? instance.copy() : null;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    protected Instance loadInstance(UUID instanceId)
    {
        logger.trace("Loading instance {}", instanceId);

        Instance instance = instanceCache.getIfPresent(instanceId);
        if (instance != null)
            return instance;

        logger.trace("Loading instance {} from disk", instanceId);
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
            if (CACHE)
            {
                instanceCache.put(instanceId, instance);
            }
            return instance;
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    protected void saveInstance(Instance instance)
    {
        logger.trace("Saving instance {}", instance.getId());
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

        if (CACHE)
        {
            instanceCache.put(instance.getId(), instance);
        }
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
            if (CACHE)
            {
                dependencyManagers.put(keyPair, dm);
            }
            return dm;
        }

        UntypedResultSet.Row row = results.one();

        ByteBuffer data = row.getBlob("data");
        assert data.hasArray();  // FIXME
        DataInput in = ByteStreams.newDataInput(data.array(), data.position());
        try
        {
            dm = DependencyManager.serializer.deserialize(in, 0);
            if (CACHE)
            {
                dependencyManagers.put(keyPair, dm);
            }
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

        if (CACHE)
        {
            dependencyManagers.put(Pair.create(key, cfId), dm);
        }
    }

    // TODO: consider only sending a missing instance if it's older than some threshold. Should keep message size down
    protected void addMissingInstance(Instance remoteInstance)
    {
        logger.debug("Adding missing instance: {}", remoteInstance.getId());
        ReadWriteLock lock = getInstanceLock(remoteInstance.getId());
        lock.writeLock().lock();
        try
        {
            Instance instance = loadInstance(remoteInstance.getId());
            if (instance != null)
                return;


            // TODO: guard against re-adding expired instances once we start gc'ing them
            instance = remoteInstance.copyRemote();
            // be careful if the instance is only preaccepted
            // if a preaccepted instance from another node is blindly added,
            // it can cause problems during the prepare phase
            if (!instance.getState().atLeast(Instance.State.ACCEPTED))
            {
                logger.debug("Setting {} as placeholder", remoteInstance.getId());
                instance.makePlacehoder();
            }
            saveInstance(instance);

            if (instance.getState().atLeast(Instance.State.COMMITTED))
                notifyCommit(instance.getId());

            recordMissingInstance(instance);

            // TODO: execute if committed

        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public List<Instance> getInstanceCopies(Set<UUID> iids)
    {
        if (iids == null || iids.size() == 0)
            return Collections.EMPTY_LIST;

        List<Instance> instances = Lists.newArrayListWithCapacity(iids.size());
        for (UUID iid: iids)
        {
            ReadWriteLock lock = getInstanceLock(iid);
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

    // accessor methods
    public ReadWriteLock getInstanceLock(UUID iid)
    {
        return locks.get(iid);
    }

    public TracingAwareExecutorService getStage(Stage stage)
    {
        return StageManager.getStage(stage);
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
