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
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.IEndpointLifecycleSubscriber;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.IFilter;
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
    private static class Handle
    {
        private static final EpaxosState instance = new EpaxosState();
    }

    public static EpaxosState getInstance()
    {
        return Handle.instance;
    }

    private static final Logger logger = LoggerFactory.getLogger(EpaxosState.class);

    private static final List<InetAddress> NO_ENDPOINTS = ImmutableList.of();

    // TODO: put these in DatabaseDescriptor

    // the amount of time the prepare phase will wait for the leader to commit an instance before
    // attempting a prepare phase. This is multiplied by a replica's position in the successor list
    protected static long PREPARE_GRACE_MILLIS = DatabaseDescriptor.getMinRpcTimeout() / 2;

    // how often the TokenMaintenanceTask runs (seconds)
    static final long TOKEN_MAINTENANCE_INTERVAL = Integer.getInteger("cassandra.epaxos.token_state_maintenance_interval", 30);


    //    private static boolean CACHE = Boolean.getBoolean("cassandra.epaxos.cache");
    private static boolean CACHE = true;

    private final Striped<ReadWriteLock> locks = Striped.readWriteLock(DatabaseDescriptor.getConcurrentWriters() * 1024);
    private final Cache<UUID, Instance> instanceCache;

    // prevents multiple threads from attempting to prepare the same instance. Aquire this before an instance lock
    private final ConcurrentMap<UUID, PrepareGroup> prepareGroups = Maps.newConcurrentMap();

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

    protected final TokenStateManager tokenStateManager;
    protected final KeyStateManager keyStateManager;

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

        public ParticipantInfo(List<InetAddress> endpoints, List<InetAddress> remoteEndpoints, ConsistencyLevel cl)
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
        }

        /**
         * returns true or false, depending on whether the number of
         * live endpoints satisfies a quorum or not
         */
        public boolean quorumExists()
        {
            return liveEndpoints.size() >= quorumSize;
        }

        /**
         * Throws an UnavailableException if a quorum isn't present
         *
         * @throws UnavailableException
         */
        public void quorumExistsOrDie() throws UnavailableException
        {
            if (!quorumExists())
                throw new UnavailableException(consistencyLevel, quorumSize, liveEndpoints.size());
        }

        public List<InetAddress> getSuccessors()
        {
            List<InetAddress> successors = Lists.newArrayList(Iterables.filter(endpoints, nonLocalPredicate));
            Collections.shuffle(successors, getRandom());
            return successors;
        }

        public Set<InetAddress> allEndpoints()
        {
            return Sets.newHashSet(Iterables.concat(endpoints, remoteEndpoints));
        }
    }

    public EpaxosState()
    {
        instanceCache = CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.MINUTES).maximumSize(10000).build();
        tokenStateManager = createTokenStateManager();
        keyStateManager = createKeyStateManager();
    }

    public void start()
    {
        start(true);
    }

    public void start(boolean startMaintenanceTasks)
    {
        tokenStateManager.start();

        if (startMaintenanceTasks)
        {
            scheduleTokenStateMaintenanceTask();
        }
    }

    public IEndpointLifecycleSubscriber getLifecycleSubscriber()
    {
        return new EndpointListener(this, tokenStateManager);
    }

    protected KeyStateManager createKeyStateManager()
    {
        return new KeyStateManager(keyspace(), keyStateTable(), tokenStateManager);
    }

    protected TokenStateManager createTokenStateManager()
    {
        return new TokenStateManager(keyspace(), tokenStateTable());
    }

    protected void scheduleTokenStateMaintenanceTask()
    {
        StorageService.scheduledTasks.scheduleAtFixedRate(new TokenStateMaintenanceTask(this, tokenStateManager),
                                                          0,
                                                          TOKEN_MAINTENANCE_INTERVAL,
                                                          TimeUnit.SECONDS);
    }

    protected String keyspace()
    {
        return Keyspace.SYSTEM_KS;
    }

    protected String instanceTable()
    {
        return SystemKeyspace.EPAXOS_INSTANCE;
    }

    protected String keyStateTable()
    {
        return SystemKeyspace.EPAXOS_KEY_STATE;
    }

    protected String tokenStateTable()
    {
        return SystemKeyspace.EPAXOS_TOKEN_STATE;
    }

    protected Random getRandom()
    {
        return random;
    }

    public int getEpochIncrementThreshold(UUID cfId)
    {
        return tokenStateManager.getEpochIncrementThreshold(cfId);
    }

    protected long getQueryTimeout(long start)
    {
        return Math.max(1, DatabaseDescriptor.getRpcTimeout() - (System.currentTimeMillis() - start));
//        return 60 * 1000 * 10;  // TODO: remove
    }

    /**
     * Initiates an epaxos instance and waits for the result to become available
     */
    // TODO: eventually, the type parameter will let this take read and cas requests
    public <T> T query(SerializedRequest query)
            throws UnavailableException, WriteTimeoutException, ReadTimeoutException, InvalidRequestException
    {

        query.getConsistencyLevel().validateForCas();

        Instance instance = createQueryInstance(query);
        return (T) process(instance, query.getConsistencyLevel());
    }

    public Object process(Instance instance, ConsistencyLevel cl) throws WriteTimeoutException
    {
        long start = System.currentTimeMillis();
        SettableFuture resultFuture = SettableFuture.create();
        resultFutures.put(instance.getId(), resultFuture);
        try
        {
            preaccept(instance);

            return resultFuture.get(getQueryTimeout(start), TimeUnit.MILLISECONDS);
        }
        catch (ExecutionException | TimeoutException e)
        {
            throw new WriteTimeoutException(WriteType.CAS, cl, 0, 1);
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
    protected QueryInstance createQueryInstance(SerializedRequest request)
    {
        QueryInstance instance = new QueryInstance(request, getEndpoint());
        logger.debug("Created QueryInstance {} on token {}", instance.getId(), instance.getToken());
        return instance;
    }

    protected EpochInstance createEpochInstance(Token token, UUID cfId, long epoch)
    {
        EpochInstance instance = new EpochInstance(getEndpoint(), token, cfId, epoch);
        logger.debug("Created EpochInstance {} for epoch {} on token {}", instance.getId(), instance.getEpoch(), instance.getToken());
        return instance;
    }

    protected TokenInstance createTokenInstance(Token token, UUID cfId)
    {
        TokenInstance instance = new TokenInstance(getEndpoint(), cfId, token);
        logger.debug("Created TokenInstance {} on token {}", instance.getId(), instance.getToken());
        return instance;
    }

    // a blind instance delete
    void deleteInstance(UUID id)
    {
        Lock lock = getInstanceLock(id).writeLock();
        lock.lock();
        try
        {
            String delete = String.format("DELETE FROM %s.%s WHERE id=?", keyspace(), instanceTable());
            QueryProcessor.executeInternal(delete, id);
            instanceCache.invalidate(id);
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

    public IVerbHandler<MessageEnvelope<Instance>> getPreacceptVerbHandler()
    {
        return new PreacceptVerbHandler(this);
    }

    protected AcceptCallback getAcceptCallback(Instance instance, ParticipantInfo participantInfo, Runnable failureCallback)
    {
        return new AcceptCallback(this, instance, participantInfo, failureCallback);
    }

    public void accept(UUID id, Set<UUID> dependencies, boolean vetoed, Runnable failureCallback)
    {
        accept(id, new AcceptDecision(true, dependencies, vetoed, Collections.<InetAddress, Set<UUID>>emptyMap()), failureCallback);
    }

    public void accept(UUID iid, AcceptDecision decision, Runnable failureCallback)
    {
        logger.debug("accepting instance {}", iid);

        // get missing instances
        Map<InetAddress, List<Instance>> missingInstances = new HashMap<>();
        for (Map.Entry<InetAddress, Set<UUID>> entry : decision.missingInstances.entrySet())
        {
            missingInstances.put(entry.getKey(), Lists.newArrayList(Iterables.filter(getInstanceCopies(entry.getValue()),
                                                                                     Instance.skipPlaceholderPredicate)));
        }

        Instance instance = getInstanceCopy(iid);
        instance.setDependencies(decision.acceptDeps);
        instance.incrementBallot();

        ParticipantInfo participantInfo;
        // TODO: should we check instance status, and bail out / call the callback if the instance is committed?
        participantInfo = getParticipants(instance);

        AcceptCallback callback = getAcceptCallback(instance, participantInfo, failureCallback);
        for (InetAddress endpoint : participantInfo.liveEndpoints)
        {
            logger.debug("sending accept request to {} for instance {}", endpoint, instance.getId());
            AcceptRequest request = new AcceptRequest(instance,
                                                      getCurrentEpoch(instance),
                                                      missingInstances.get(endpoint));
            sendRR(request.getMessage(), endpoint, callback);
        }

        // send to remote datacenters for LOCAL_SERIAL queries
        for (InetAddress endpoint : participantInfo.remoteEndpoints)
        {
            logger.debug("sending accept request to non-local dc {} for instance {}", endpoint, instance.getId());
            AcceptRequest request = new AcceptRequest(instance,
                                                      getCurrentEpoch(instance),
                                                      null);
            sendOneWay(request.getMessage(), endpoint);
        }
    }

    public IVerbHandler<AcceptRequest> getAcceptVerbHandler()
    {
        return new AcceptVerbHandler(this);
    }

    /**
     * notifies threads waiting on this commit
     * <p/>
     * must be called with this instances's write lock held
     */
    public void notifyCommit(UUID id)
    {
        List<ICommitCallback> callbacks = commitCallbacks.get(id);
        if (callbacks != null)
        {
            for (ICommitCallback cb : callbacks)
            {
                cb.instanceCommitted(id);
            }
        }
    }

    /**
     * Registers an implementation of ICommitCallback to be notified when an instance is committed
     * <p/>
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
            if (instance.getState() == Instance.State.EXECUTED)
            {
                assert instance.getDependencies().equals(dependencies);
            } else
            {
                instance.commit(dependencies);
            }

            // FIXME: unavailable exception shouldn't be thrown by getParticipants. It will prevent instances from being committed locally
            ParticipantInfo participantInfo = getParticipants(instance);
            MessageOut<MessageEnvelope<Instance>> message = instance.getMessage(MessagingService.Verb.EPAXOS_COMMIT,
                                                                                tokenStateManager.getEpoch(instance));
            for (InetAddress endpoint : participantInfo.liveEndpoints)
            {
                logger.debug("sending commit request to {} for instance {}", endpoint, instance.getId());
                sendOneWay(message, endpoint);
            }

            // send to remote datacenters for LOCAL_SERIAL queries
            for (InetAddress endpoint : participantInfo.remoteEndpoints)
            {
                // TODO: skip dead nodes
                logger.debug("sending commit request to non-local dc {} for instance {}", endpoint, instance.getId());
                sendOneWay(message, endpoint);
            }
        }
        catch (InvalidInstanceStateChange e)
        {
            throw new RuntimeException(e);
        }
    }

    public IVerbHandler<MessageEnvelope<Instance>> getCommitVerbHandler()
    {
        return new CommitVerbHandler(this);
    }

    public void execute(UUID instanceId)
    {
        getStage(Stage.MUTATION).submit(new ExecuteTask(this, instanceId));
    }

    protected ReplayPosition executeInstance(Instance instance) throws InvalidRequestException, ReadTimeoutException, WriteTimeoutException
    {
        switch (instance.getType())
        {
            case QUERY:
                return executeQueryInstance((QueryInstance) instance);
            case EPOCH:
                executeEpochInstance((EpochInstance) instance);
                return null;
            case TOKEN:
                executeTokenInstance((TokenInstance) instance);
                return null;
            default:
                throw new IllegalArgumentException("Unsupported instance type: " + instance.getClass().getName());
        }
    }

    private void maybeSetResultFuture(UUID id, Object result)
    {
        SettableFuture resultFuture = resultFutures.get(id);
        if (resultFuture != null)
        {
            resultFuture.set(result);
            resultFutures.remove(id);
        }
    }

    protected ReplayPosition executeQueryInstance(QueryInstance instance) throws ReadTimeoutException, WriteTimeoutException
    {
        logger.debug("Executing serialized request for {}", instance.getId());

        Pair<ColumnFamily, ReplayPosition> result = instance.getQuery().execute();
        maybeSetResultFuture(instance.getId(), result.left);
        return result.right;
    }

    /**
     * Token Instance will be recorded as executing in the epoch it increments to.
     * This means that the first instance executed for any epoch > 0 will be the
     * incrementing token instance.
     */
    protected void executeEpochInstance(EpochInstance instance)
    {
        // TODO: get exact
        TokenState tokenState = tokenStateManager.get(instance);

        tokenState.rwLock.writeLock().lock();
        try
        {
            // no use iterating over all key states if this is effectively a noop
            if (instance.getEpoch() > tokenState.getEpoch())
            {
                tokenState.setEpoch(instance.getEpoch());
                // TODO: consider moving keyspace.updateEpoch out of the critical section, creation of new key states are blocked while the token state lock is held
                keyStateManager.updateEpoch(tokenState);
                tokenStateManager.save(tokenState);

                logger.info("Epoch set to {} for token {} on {}", instance.getEpoch(), instance.getToken(), instance.getCfId());
            }
        }
        finally
        {
            tokenState.rwLock.writeLock().unlock();
        }

        //
        maybeSetResultFuture(instance.getId(), null);
        startTokenStateGc(tokenState);
    }

    protected synchronized void executeTokenInstance(TokenInstance instance)
    {
        UUID cfId = instance.getCfId();
        Token token = instance.getToken();

        logger.debug("Executing token state instance: {}", instance);

        if (tokenStateManager.getExact(instance.getToken(), instance.getCfId()) != null)
        {
            logger.debug("Token State already exists for {} on {}", instance.getToken(), instance.getCfId());
            maybeSetResultFuture(instance.getId(), null);
            return;
        }

        // TODO: should the neighbor be part of the instance?
        TokenState neighbor = tokenStateManager.get(token, cfId);

        // token states for this node's replicated tokens
        // are initialized at epoch 0 the first time `get` is
        // called for a cfId
        assert neighbor != null;

        neighbor.rwLock.writeLock().lock();
        try
        {
            long epoch = neighbor.getEpoch();
            TokenState tokenState = new TokenState(token, cfId, neighbor.getEpoch(), neighbor.getEpoch(), 0);
            tokenState.rwLock.writeLock().lock();
            try
            {
                if (tokenStateManager.putState(tokenState) != tokenState)
                {
                    logger.warn("Token state {} exists unexpectedly for {}", token, cfId);
                    maybeSetResultFuture(instance.getId(), null);
                    return;
                }

                Range<Token> neighborRange = new Range<>(token, neighbor.getToken());
                // transfer token state uuid to new token state
                // epoch instances aren't also recorded because those
                // will only  affect the neighbor, not the new token instance
                for (Map.Entry<Token, Set<UUID>> entry: neighbor.splitTokenInstances(token).entrySet())
                {
                    if (!neighborRange.contains(entry.getKey()))
                    {
                        for (UUID id: entry.getValue())
                        {
                            tokenState.recordTokenInstance(entry.getKey(), id);
                            neighbor.removeTokenInstance(entry.getKey(), id);
                        }
                    }
                }

//                tokenState.setEpoch(epoch + 1);
//                neighbor.setEpoch(epoch + 1);

                // the epoch for both this token state, and it's neighbor (the token it was split from)
                // need to be greater than the epoch at the time of the split. This prevents new token
                // owners from having blocking dependencies on instances it doesn't replicate.
                tokenState.setMinStreamEpoch(tokenState.getEpoch() + 1);
                neighbor.setMinStreamEpoch(neighbor.getEpoch() + 1);

                logger.info("Token state created at {} on epoch {} with instance {}",
                            tokenState.getToken(),
                            tokenState.getEpoch(),
                            instance.getId());

                // neighbor is saved after in case of failure. Double entries
                // can be removed from the neighbor if initialization doesn't complete
                tokenStateManager.save(tokenState);
                tokenStateManager.save(neighbor);

//                keyStateManager.updateEpoch(tokenState);
//                keyStateManager.updateEpoch(neighbor);
            }
            finally
            {
                tokenState.rwLock.writeLock().unlock();
            }
        }
        finally
        {
            neighbor.rwLock.writeLock().unlock();
        }
        maybeSetResultFuture(instance.getId(), null);
    }

    void startTokenStateGc(TokenState tokenState)
    {
        getStage(Stage.MUTATION).submit(new GarbageCollectionTask(this, tokenState, keyStateManager));
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
     * <p/>
     * The test case EpaxosIntegrationRF3Test.inferredFastPathFailedLeaderRecovery demonstrates
     * a scenario that you can't get out of without first running a TryPreaccept.
     * <p/>
     * TryPreaccept is basically determining if an instance could have been committed on the fast
     * path, based on responses from only a quorum of replicas.
     * <p/>
     * If the highest instance state a prepare phase encounters is PREACCEPTED, PrepareCallback.getTryPreacceptAttempts
     * will identify any dependency sets that are shared by at least (F + 1) / 2 replicas, who are not the command
     * leader.
     * <p/>
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

        Token token;
        UUID cfId;
        long epoch;
        Lock lock = getInstanceLock(iid).readLock();
        lock.lock();
        try
        {
            Instance instance = loadInstance(iid);
            token = instance.getToken();
            cfId = instance.getCfId();
            epoch = getCurrentEpoch(instance);
        }
        finally
        {
            lock.unlock();
        }

        TryPreacceptRequest request = new TryPreacceptRequest(token, cfId, epoch, iid, attempt.dependencies);
        MessageOut<TryPreacceptRequest> message = request.getMessage();

        TryPreacceptCallback callback = getTryPreacceptCallback(iid, attempt, nextAttempts, participantInfo, failureCallback);
        for (InetAddress endpoint : attempt.toConvince)
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
            throw new AssertionError(e);
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

    // failure recovery
    public long getCurrentEpoch(Instance i)
    {
        return getCurrentEpoch(i.getToken(), i.getCfId());
    }

    public long getCurrentEpoch(Token token, UUID cfId)
    {
        return tokenStateManager.get(token, cfId).getEpoch();
    }

    public EpochDecision validateMessageEpoch(IEpochMessage message)
    {
        long localEpoch = getCurrentEpoch(message.getToken(), message.getCfId());
        long remoteEpoch = message.getEpoch();
        return new EpochDecision(EpochDecision.evaluate(localEpoch, remoteEpoch),
                                 message.getToken(),
                                 localEpoch,
                                 remoteEpoch);
    }

    public TokenState getTokenState(IEpochMessage message)
    {
        return tokenStateManager.get(message.getToken(), message.getCfId());
    }

    private final Set<FailureRecoveryTask> failureRecoveryTasks = new HashSet<>();

    /**
     * starts a new local failure recovery task for the given token.
     */
    public void startLocalFailureRecovery(Token token, UUID cfId, long epoch)
    {
        FailureRecoveryTask task = new FailureRecoveryTask(this, token, cfId, epoch);
        synchronized (failureRecoveryTasks)
        {
            if (failureRecoveryTasks.contains(task))
            {
                return;
            }
            getStage(Stage.MISC).submit(task);
        }
    }

    // TODO: make this more tolerant of hung failure recovery tasks
    public void failureRecoveryTaskCompleted(FailureRecoveryTask task)
    {
        synchronized (failureRecoveryTasks)
        {
            failureRecoveryTasks.remove(task);
        }
    }

    /**
     * starts a new failure recovery task for the given endpoint and token.
     */
    public void startRemoteFailureRecovery(InetAddress endpoint, Token token, UUID cfId, long epoch)
    {
        if (FailureDetector.instance.isAlive(endpoint))
        {
            // TODO: track recently sent requests so we don't spam the other server
            MessageOut<FailureRecoveryRequest> msg = new MessageOut<>(MessagingService.Verb.EPAXOS_FAILURE_RECOVERY,
                                                                      new FailureRecoveryRequest(token, cfId, epoch),
                                                                      FailureRecoveryRequest.serializer);
            sendOneWay(msg, endpoint);
        }
    }

    public IVerbHandler<FailureRecoveryRequest> getFailureRecoveryVerbHandler()
    {
        return new FailureRecoveryVerbHandler(this);
    }

    // repair / streaming support
    public boolean managesCfId(UUID cfId)
    {
        return tokenStateManager.managesCfId(cfId);
    }

    /**
     * Returns a tuple containing the current epoch, and instances executed in the current epoch for the given key/cfId
     */
    public ExecutionInfo getEpochExecutionInfo(ByteBuffer key, UUID cfId)
    {
        if (!managesCfId(cfId))
        {
            return null;
        }
        CfKey cfKey = new CfKey(key, cfId);
        Lock lock = keyStateManager.getCfKeyLock(cfKey);
        lock.lock();
        try
        {
            KeyState keyState = keyStateManager.loadKeyState(key, cfId);
            return keyState != null ? new ExecutionInfo(keyState.getEpoch(), keyState.getExecutionCount()) : null;
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Determines if a repair can be applied to a key.
     */
    public boolean canApplyRepair(ByteBuffer key, UUID cfId, long epoch, long executionCount)
    {
        ExecutionInfo local = getEpochExecutionInfo(key, cfId);

        if (local == null || local.epoch > epoch)
        {
            return true;
        }
        else if (local.epoch == epoch)
        {
            return local.executed >= executionCount;
        }
        else
        {
            return false;
        }
    }

    public boolean canExecute(Instance instance)
    {
        if (instance.getType() == Instance.Type.QUERY)
        {
            QueryInstance queryInstance = (QueryInstance) instance;
            return keyStateManager.canExecute(queryInstance.getQuery().getCfKey());
        }

        return true;
    }

    public boolean reportFutureExecution(ByteBuffer key, UUID cfId, ExecutionInfo info)
    {
        // TODO: check epoch and maybe start recovery
        return keyStateManager.reportFutureRepair(new CfKey(key, cfId), info);
    }

    public Iterator<Pair<ByteBuffer, ExecutionInfo>> getRangeExecutionInfo(UUID cfId, Range<Token> range, ReplayPosition position)
    {
        return keyStateManager.getRangeExecutionInfo(cfId, range, position);
    }

    /**
     * writes metadata about the state of epaxos when the given sstable was flushed.
     */
    public void writeStreamHeader(SSTableReader ssTable, Collection<Range<Token>> ranges, FileMessageHeader header)
    {
        UUID cfId = ssTable.metadata.cfId;
        ReplayPosition position = ssTable.getReplayPosition();
        IFilter filter = ssTable.getBloomFilter();

        Range<Token> sstableRange = new Range<>(ssTable.first.getToken(), ssTable.last.getToken());
        for (Range<Token> range: ranges)
        {
            if (!range.intersects(sstableRange))
            {
                logger.debug("skipping range not covered by sstable {}", range);
                continue;
            }

            Iterator<Pair<ByteBuffer, ExecutionInfo>> iter = getRangeExecutionInfo(cfId, range, position);

            while (iter.hasNext())
            {
                Pair<ByteBuffer, ExecutionInfo> next = iter.next();
                if (!filter.isPresent(next.left))
                {
                    logger.debug("skipping key not contained by sstable {}", range);
                    continue;
                }
                header.epaxos.put(next.left, next.right);
                if (logger.isDebugEnabled())
                {
                    logger.debug("Added {} {} to outgoing epaxos header", next.left, next.right);
                }

            }
        }
    }

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

            instance = remoteInstance.copyRemote();
            // be careful if the instance is only preaccepted
            // if a preaccepted instance from another node is blindly added,
            // it can cause problems during the prepare phase
            if (!instance.getState().atLeast(Instance.State.ACCEPTED))
            {
                logger.debug("Setting {} as placeholder", remoteInstance.getId());
                instance.makePlacehoder();
            }

            // don't add missing instances with an EXECUTED state
            if (instance.getState().atLeast(Instance.State.EXECUTED))
            {
                instance.commitRemote();
            }

            saveInstance(instance);

            if (instance.getState().atLeast(Instance.State.COMMITTED))
                notifyCommit(instance.getId());

            keyStateManager.recordMissingInstance(instance);

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

    public UUID addToken(UUID cfId, Token token)
    {
        TokenInstance instance = new TokenInstance(getEndpoint(), cfId, token);
        preaccept(instance);
        return instance.getId();
    }

    // key state methods
    public Set<UUID> getCurrentDependencies(Instance instance)
    {
        return keyStateManager.getCurrentDependencies(instance);
    }

    public void recordMissingInstance(Instance instance)
    {
        keyStateManager.recordMissingInstance(instance);
    }

    public void recordAcknowledgedDeps(Instance instance)
    {
        keyStateManager.recordAcknowledgedDeps(instance);
    }

    public void recordExecuted(Instance instance, ReplayPosition position)
    {
        keyStateManager.recordExecuted(instance, position);
        tokenStateManager.reportExecution(instance.getToken(), instance.getCfId());
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

    protected ParticipantInfo getParticipants(Instance instance)
    {
        switch (instance.getType())
        {
            case QUERY:
                return getQueryParticipants((QueryInstance) instance);
            case TOKEN:
            case EPOCH:
                return getTokenParticipants((AbstractTokenInstance) instance);
            default:
                throw new IllegalArgumentException("Unsupported instance type: " + instance.getType());
        }
    }

    // TODO: factor out (rework integration tests)
    protected ParticipantInfo getQueryParticipants(QueryInstance instance)
    {
        return getInstanceParticipants(instance, instance.getQuery().getConsistencyLevel());
    }

    // TODO: factor out (rework integration tests)
    protected ParticipantInfo getTokenParticipants(AbstractTokenInstance instance)
    {
        // FIXME: add support for LOCAL_SERIAL
        return getInstanceParticipants(instance, ConsistencyLevel.SERIAL);
    }

    protected ParticipantInfo getInstanceParticipants(Instance instance, ConsistencyLevel cl)
    {
        String ks = Schema.instance.getCF(instance.getCfId()).left;

        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(ks, instance.getToken());
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(instance.getToken(), ks);

        List<InetAddress> endpoints = ImmutableList.copyOf(Iterables.filter(Iterables.concat(naturalEndpoints, pendingEndpoints), duplicateFilter()));
        List<InetAddress> remoteEndpoints = null;
        if (cl == ConsistencyLevel.LOCAL_SERIAL)
        {
            // Restrict naturalEndpoints and pendingEndpoints to node in the local DC only
            String localDc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
            Predicate<InetAddress> isLocalDc = dcPredicateFor(localDc, false);
            Predicate<InetAddress> notLocalDc = dcPredicateFor(localDc, true);

            remoteEndpoints = ImmutableList.copyOf(Iterables.filter(endpoints, notLocalDc));
            endpoints = ImmutableList.copyOf(Iterables.filter(endpoints, isLocalDc));
        }
        return new ParticipantInfo(endpoints, remoteEndpoints, cl);
    }

    public boolean replicates(Instance instance)
    {
        return getParticipants(instance).allEndpoints().contains(getEndpoint());
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

    private static <T> Predicate<T> duplicateFilter()
    {
        return new Predicate<T>()
        {
            Set<T> seen = new HashSet<>();
            public boolean apply(T t)
            {
                if (seen.contains(t))
                {
                    return false;
                }
                else
                {
                    seen.add(t);
                    return true;
                }
            }
        };
    }

    protected Predicate<InetAddress> livePredicate()
    {
        return IAsyncCallback.isAlive;
    }

}
