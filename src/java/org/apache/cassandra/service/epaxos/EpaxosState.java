package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.*;
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
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataInputStream;
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

    // the amount of time the prepare phase will wait for the leader to commit an instance before
    // attempting a prepare phase. This is multiplied by a replica's position in the successor list
    protected static long PREPARE_GRACE_MILLIS = Long.getLong("cassandra.epaxos.prepare_grace_millis",
                                                              DatabaseDescriptor.getMinRpcTimeout() / 2);

    // how often the TokenMaintenanceTask runs (seconds)
    static final long TOKEN_MAINTENANCE_INTERVAL = Integer.getInteger("cassandra.epaxos.token_state_maintenance_interval", 30);

    // how long we wait between sending the same failure recovery messgae to a node
    static final long FAILURE_RECOVERY_MESSAGE_INTERVAL = Integer.getInteger("cassandra.epaxos.failure_recovery_message_interval", 30);

    // for testing only
    static final boolean QUERY_FORWARDING_DISABLED = Boolean.getBoolean("cassandra.epaxos.disable_query_forwarding");

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

            if (cl == ConsistencyLevel.SERIAL && (remoteEndpoints != null && !remoteEndpoints.isEmpty()))
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

        public boolean fastQuorumExists()
        {
            return liveEndpoints.size() >= fastQuorumSize;
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

        public Set<InetAddress> allEndpoints()
        {
            return Sets.newHashSet(Iterables.concat(endpoints, remoteEndpoints));
        }
    }

    private final String keyspace;
    private final String instanceTable;
    private final String keyStateTable;
    private final String tokenStateTable;

    protected final Map<Scope, TokenStateManager> tokenStateManagers = new EnumMap<>(Scope.class);
    protected final Map<Scope, KeyStateManager> keyStateManagers = new EnumMap<>(Scope.class);
    protected final RemoteActivity remoteActivity = new RemoteActivity();

    public EpaxosState()
    {
        this(Keyspace.SYSTEM_KS, SystemKeyspace.EPAXOS_INSTANCE, SystemKeyspace.EPAXOS_KEY_STATE, SystemKeyspace.EPAXOS_TOKEN_STATE);
    }

    public EpaxosState(String keyspace, String instanceTable, String keyStateTable, String tokenStateTable)
    {
        this.keyspace = keyspace;
        this.instanceTable = instanceTable;
        this.keyStateTable = keyStateTable;
        this.tokenStateTable = tokenStateTable;

        instanceCache = CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.MINUTES).maximumSize(10000).build();
        createScope(Scope.GLOBAL);
        createScope(Scope.LOCAL);
    }

    public void start()
    {
        start(true);
    }

    public void start(boolean startMaintenanceTasks)
    {
        for (TokenStateManager tsm: tokenStateManagers.values())
        {
            tsm.start();
        }

        if (startMaintenanceTasks)
        {
            scheduleTokenStateMaintenanceTask();
        }
    }

    protected KeyStateManager createKeyStateManager(TokenStateManager tsm, Scope scope)
    {
        return new KeyStateManager(getKeyspace(), getKeyStateTable(), tsm, scope);
    }

    protected TokenStateManager createTokenStateManager(Scope scope)
    {
        return new TokenStateManager(getKeyspace(), getTokenStateTable(), scope);
    }

    private void createScope(Scope scope)
    {
        TokenStateManager tsm = createTokenStateManager(scope);
        KeyStateManager ksm = createKeyStateManager(tsm, scope);
        tokenStateManagers.put(scope, tsm);
        keyStateManagers.put(scope, ksm);
    }

    protected void scheduleTokenStateMaintenanceTask()
    {
        Runnable task = new TokenStateMaintenanceTask(this, tokenStateManagers.values());
        StorageService.scheduledTasks.scheduleAtFixedRate(task,
                                                          0,
                                                          TOKEN_MAINTENANCE_INTERVAL,
                                                          TimeUnit.SECONDS);
    }

    protected String getKeyspace()
    {
        return keyspace;
    }

    protected String getInstanceTable()
    {
        return instanceTable;
    }

    protected String getKeyStateTable()
    {
        return keyStateTable;
    }

    protected String getTokenStateTable()
    {
        return tokenStateTable;
    }

    protected Random getRandom()
    {
        return random;
    }

    protected TokenStateManager getTokenStateManager(Instance instance)
    {
        return getTokenStateManager(Scope.get(instance));
    }

    protected TokenStateManager getTokenStateManager(Scope scope)
    {
        return tokenStateManagers.get(scope);
    }

    protected KeyStateManager getKeyStateManager(Instance instance)
    {
        return getKeyStateManager(Scope.get(instance));
    }

    protected KeyStateManager getKeyStateManager(Scope scope)
    {
        return keyStateManagers.get(scope);
    }

    public int getEpochIncrementThreshold(UUID cfId, Scope scope)
    {
        return getTokenStateManager(scope).getEpochIncrementThreshold(cfId);
    }

    protected long getQueryTimeout(long start)
    {
        return Math.max(1, DatabaseDescriptor.getRpcTimeout() - (System.currentTimeMillis() - start));
    }

    /**
     * Initiates an epaxos instance and waits for the result to become available
     */
    public <T> T query(SerializedRequest query)
            throws UnavailableException, WriteTimeoutException, ReadTimeoutException, InvalidRequestException
    {

        query.getConsistencyLevel().validateForCas();
        Instance instance = createQueryInstance(query);

        ParticipantInfo pi = getParticipants(instance);
        if (!pi.endpoints.contains(getEndpoint()))
        {
            if (!QUERY_FORWARDING_DISABLED)
            {
                return (T) forwardQuery(query, pi);
            }
            else
            {
                throw new InvalidRequestException("Query forwarding disabled");
            }
        }
        else
        {
            return (T) process(instance);
        }
    }

    SettableFuture setFuture(Instance instance)
    {
        SettableFuture future = SettableFuture.create();
        resultFutures.put(instance.getId(), future);
        return future;
    }

    private int blockFor(Instance instance)
    {
        return instance.getConsistencyLevel().blockFor(Keyspace.open(Schema.instance.getCF(instance.getCfId()).left));
    }

    public Object process(Instance instance) throws WriteTimeoutException
    {
        long start = System.currentTimeMillis();
        SettableFuture resultFuture = setFuture(instance);
        try
        {
            preaccept(instance);

            return resultFuture.get(getQueryTimeout(start), TimeUnit.MILLISECONDS);
        }
        catch (ExecutionException | TimeoutException e)
        {
            throw new WriteTimeoutException(WriteType.CAS, instance.getConsistencyLevel(), 0, blockFor(instance));
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

    public Object forwardQuery(SerializedRequest query, ParticipantInfo pi) throws UnavailableException, WriteTimeoutException
    {
        Predicate<InetAddress> isLive = livePredicate();
        for (InetAddress endpoint: pi.endpoints)
        {
            if (isLive.apply(endpoint))
            {
                long start = System.currentTimeMillis();
                ForwardedQueryCallback cb = new ForwardedQueryCallback();
                MessageOut<SerializedRequest> msg = new MessageOut<>(MessagingService.Verb.EPAXOS_FORWARD_QUERY,
                                                                     query, SerializedRequest.serializer);
                sendRR(msg, endpoint, cb);

                try
                {
                    return cb.future.get(getQueryTimeout(start), TimeUnit.MILLISECONDS);
                }
                catch (ExecutionException | TimeoutException e)
                {
                    throw new WriteTimeoutException(WriteType.CAS, query.getConsistencyLevel(), 0, 1);
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
            }
        }
        throw new UnavailableException(query.getConsistencyLevel(), 1, 0);
    }

    public IVerbHandler<SerializedRequest> getForwardQueryVerbHandler()
    {
        return new ForwardedQueryVerbHandler(this);

    }

    /**
     * Creates a new instance, setting this node as the leader
     */
    protected QueryInstance createQueryInstance(SerializedRequest request)
    {
        QueryInstance instance = new QueryInstance(request, getEndpoint(), getDc());
        logger.debug("Created QueryInstance {} on token {}", instance.getId(), instance.getToken());
        return instance;
    }

    protected EpochInstance createEpochInstance(Token token, UUID cfId, long epoch, Scope scope)
    {
        EpochInstance instance = new EpochInstance(getEndpoint(), getDc(), token, cfId, epoch, scope);
        logger.debug("Created EpochInstance {} for epoch {} on token {}", instance.getId(), instance.getEpoch(), instance.getToken());
        return instance;
    }

    protected TokenInstance createTokenInstance(Token token, UUID cfId, Scope scope)
    {
        TokenInstance instance;
        TokenState ts = getTokenStateManager(scope).get(token, cfId);
        ts.lock.readLock().lock();
        try
        {
            instance = new TokenInstance(getEndpoint(), getDc(), cfId, token, ts.getRange(), scope);
        }
        finally
        {
            ts.lock.readLock().unlock();
        }
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
            String delete = String.format("DELETE FROM %s.%s WHERE id=?", getKeyspace(), getInstanceTable());
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

    public void accept(UUID id, Set<UUID> dependencies, boolean vetoed, Range<Token> splitRange, Runnable failureCallback)
    {
        accept(id, new AcceptDecision(true, dependencies, vetoed, splitRange, Collections.<InetAddress, Set<UUID>>emptyMap()), failureCallback);
    }

    public void accept(UUID iid, AcceptDecision decision, Runnable failureCallback)
    {
        logger.debug("accepting instance {}", iid);

        // get missing instances
        Map<InetAddress, List<Instance>> missingInstances = new HashMap<>();
        for (Map.Entry<InetAddress, Set<UUID>> entry : decision.missingInstances.entrySet())
        {
            missingInstances.put(entry.getKey(), getInstanceCopies(entry.getValue()));
        }

        Instance instance = getInstanceCopy(iid);

        if (instance.getState().atLeast(Instance.State.COMMITTED))
        {
            // nothing to do here...
            return;
        }

        instance.setDependencies(decision.acceptDeps);

        if (instance instanceof EpochInstance)
        {
            ((EpochInstance) instance).setVetoed(decision.vetoed);
        }

        instance.incrementBallot();

        ParticipantInfo participantInfo;
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
        logger.debug("notifying commit listener of commit for {}", id);
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

    @VisibleForTesting
    Set<UUID> registeredCommitCallbacks()
    {
        return commitCallbacks.keySet();
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
            }
            else
            {
                instance.commit(dependencies);
            }

            ParticipantInfo participantInfo = getParticipants(instance);
            MessageOut<MessageEnvelope<Instance>> message = instance.getMessage(MessagingService.Verb.EPAXOS_COMMIT,
                                                                                getTokenStateManager(instance).getEpoch(instance));
            for (InetAddress endpoint : participantInfo.liveEndpoints)
            {
                logger.debug("sending commit request to {} for instance {}", endpoint, instance.getId());
                sendOneWay(message, endpoint);
            }

            // send to remote datacenters for LOCAL_SERIAL queries
            for (InetAddress endpoint : participantInfo.remoteEndpoints)
            {
                if (isAlive(endpoint))
                {
                    logger.debug("sending commit request to non-local dc {} for instance {}", endpoint, instance.getId());
                    sendOneWay(message, endpoint);
                }
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

    protected Pair<ReplayPosition, Long> executeInstance(Instance instance) throws InvalidRequestException, ReadTimeoutException, WriteTimeoutException
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

    void maybeSetResultFuture(UUID id, Object result)
    {
        SettableFuture resultFuture = resultFutures.get(id);
        if (resultFuture != null)
        {
            resultFuture.set(result);
            resultFutures.remove(id);
        }
    }

    protected Pair<ReplayPosition, Long> executeQueryInstance(QueryInstance instance) throws ReadTimeoutException, WriteTimeoutException
    {
        logger.debug("Executing serialized request for {}", instance.getId());

        SerializedRequest request = instance.getQuery();

        long maxTimestamp = getKeyStateManager(instance).getMaxTimestamp(request.getCfKey());
        long minTimestamp = Math.max(maxTimestamp + 1, UUIDGen.unixTimestamp(instance.getId()) * 1000);
        SerializedRequest.ExecutionMetaData metaData = request.execute(minTimestamp);
        maybeSetResultFuture(instance.getId(), metaData.result);
        // TODO: disseminate mutations to remote dcs for LOCAL_SERIAL instances
        return Pair.create(metaData.replayPosition, metaData.maxTimestamp);
    }

    /**
     * Token Instance will be recorded as executing in the epoch it increments to.
     * This means that the first instance executed for any epoch > 0 will be the
     * incrementing token instance.
     */
    protected void executeEpochInstance(EpochInstance instance)
    {
        TokenStateManager tokenStateManager = getTokenStateManager(instance);
        TokenState tokenState = tokenStateManager.getExact(instance.getToken(), instance.getCfId());
        if (tokenState == null)
        {
            throw new AssertionError(String.format("No token state exists for %s", instance));
        }

        tokenState.lock.writeLock().lock();
        try
        {
            // no use iterating over all key states if this is effectively a noop
            if (instance.getEpoch() > tokenState.getEpoch())
            {
                tokenState.setEpoch(instance.getEpoch());
                tokenStateManager.save(tokenState);

                logger.info("Epoch set to {} for token {} on {}", instance.getEpoch(), instance.getToken(), instance.getCfId());
            }
        }
        finally
        {
            tokenState.lock.writeLock().unlock();
        }

        maybeSetResultFuture(instance.getId(), null);
        startTokenStateGc(tokenState, instance.getScope());
    }

    protected synchronized void executeTokenInstance(TokenInstance instance)
    {
        UUID cfId = instance.getCfId();
        Token token = instance.getToken();
        TokenStateManager tokenStateManager = getTokenStateManager(instance);

        logger.debug("Executing token state instance: {}", instance);

        if (tokenStateManager.getExact(instance.getToken(), instance.getCfId()) != null)
        {
            logger.debug("Token State already exists for {} on {}", instance.getToken(), instance.getCfId());
            maybeSetResultFuture(instance.getId(), null);
            return;
        }

        TokenState neighbor = tokenStateManager.get(token, cfId);

        // token states for this node's replicated tokens
        // are initialized at epoch 0 the first time `get` is
        // called for a cfId
        assert neighbor != null;

        neighbor.lock.writeLock().lock();
        try
        {
            long epoch = neighbor.getEpoch();
            Range<Token> range = new Range<>(neighbor.getPredecessor(), token);
            TokenState tokenState = new TokenState(range, cfId, neighbor.getEpoch(), 0);
            neighbor.setPredecessor(token);
            tokenState.lock.writeLock().lock();
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
                for (Map.Entry<Token, Set<UUID>> entry: neighbor.allTokenInstances().entrySet())
                {
                    if (!neighborRange.contains(entry.getKey()))
                    {
                        for (UUID id: entry.getValue())
                        {
                            tokenState.recordTokenInstance(entry.getKey(), id);
                            if (!id.equals(instance.getId()))
                            {
                                neighbor.removeTokenInstance(entry.getKey(), id);
                            }
                        }
                    }
                }
                tokenState.setCreatorToken(neighbor.getToken());

                tokenState.setEpoch(epoch + 1);
                neighbor.setEpoch(epoch + 1);

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
                // TODO: have the token state manager check for inconsistencies introduced here
            }
            finally
            {
                tokenState.lock.writeLock().unlock();
            }
        }
        finally
        {
            neighbor.lock.writeLock().unlock();
        }
        maybeSetResultFuture(instance.getId(), null);
    }

    void startTokenStateGc(TokenState tokenState, Scope scope)
    {
        getStage(Stage.MUTATION).submit(new GarbageCollectionTask(this, tokenState, getKeyStateManager(scope)));
    }

    protected PrepareCallback getPrepareCallback(UUID id, int ballot, ParticipantInfo participantInfo, PrepareGroup group, int attempt)
    {
        return new PrepareCallback(this, id, ballot, participantInfo, group, attempt);
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

    @VisibleForTesting
    Set<UUID> registeredPrepareGroups()
    {
        return prepareGroups.keySet();
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
        assert !attempts.isEmpty();
        TryPreacceptAttempt attempt = attempts.get(0);
        List<TryPreacceptAttempt> nextAttempts = attempts.subList(1, attempts.size());

        logger.debug("running trypreaccept prepare for {}: {}", iid, attempt);

        // if all replicas have the same deps, and they all agree with the leader,
        // then we can jump right to the accept phase
        if (attempt.requiredConvinced == 0)
        {
            assert attempt.agreedWithLeader;
            accept(iid, attempt.dependencies, attempt.vetoed, attempt.splitRange, failureCallback);
            return;
        }

        Token token;
        UUID cfId;
        long epoch;
        Scope scope;
        int ballot;
        boolean localAttempt = attempt.toConvince.contains(getEndpoint());
        Lock lock = localAttempt ? getInstanceLock(iid).readLock() : getInstanceLock(iid).writeLock();
        lock.lock();
        try
        {
            Instance instance = loadInstance(iid);
            token = instance.getToken();
            cfId = instance.getCfId();
            epoch = getCurrentEpoch(instance);
            scope = instance.getScope();
            ballot = instance.getBallot() + 1;

            // if the prepare leader isn't being sent a trypreaccept message, update
            // it's ballot locally so follow on requests don't fail
            if (!localAttempt)
            {
                instance.updateBallot(ballot);
                saveInstance(instance);
            }
        }
        finally
        {
            lock.unlock();
        }

        TryPreacceptRequest request = new TryPreacceptRequest(token, cfId, epoch, scope, iid, attempt.dependencies, ballot);
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
        UntypedResultSet results = QueryProcessor.executeInternal(String.format(query, getKeyspace(), getInstanceTable()),
                                                                  instanceId);
        if (results.isEmpty())
            return null;

        UntypedResultSet.Row row = results.one();

        DataInput in = new DataInputStream(ByteBufferUtil.inputStream(row.getBlob("data")));

        try
        {
            instance = Instance.internalSerializer.deserialize(in, row.getInt("version"));
            instanceCache.put(instanceId, instance);
            return instance;
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    @VisibleForTesting
    void clearInstanceCache()
    {
        instanceCache.invalidateAll();
    }

    boolean cacheContains(UUID id)
    {
        return instanceCache.getIfPresent(id) != null;
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
        QueryProcessor.executeInternal(String.format(instanceReq, getKeyspace(), getInstanceTable()),
                                       instance.getId(),
                                       ByteBuffer.wrap(out.getData()),
                                       0,
                                       timestamp);

        instanceCache.put(instance.getId(), instance);
    }

    // failure recovery
    public long getCurrentEpoch(Instance i)
    {
        return getCurrentEpoch(i.getToken(), i.getCfId(), i.getScope());
    }

    public long getCurrentEpoch(Token token, UUID cfId, Scope scope)
    {
        return getTokenStateManager(scope).get(token, cfId).getEpoch();
    }

    public EpochDecision validateMessageEpoch(IEpochMessage message)
    {
        long localEpoch = getCurrentEpoch(message.getToken(), message.getCfId(), message.getScope());
        long remoteEpoch = message.getEpoch();
        return new EpochDecision(EpochDecision.evaluate(localEpoch, remoteEpoch),
                                 message.getToken(),
                                 localEpoch,
                                 remoteEpoch);
    }

    public TokenState getTokenState(IEpochMessage message)
    {
        return getTokenStateManager(message.getScope()).get(message.getToken(), message.getCfId());
    }

    private final Set<FailureRecoveryTask> failureRecoveryTasks = new HashSet<>();

    /**
     * starts a new local failure recovery task for the given token.
     */
    public void startLocalFailureRecovery(Token token, UUID cfId, long epoch, Scope scope)
    {
        FailureRecoveryTask task = new FailureRecoveryTask(this, token, cfId, epoch, scope);
        synchronized (failureRecoveryTasks)
        {
            if (failureRecoveryTasks.contains(task))
            {
                return;
            }
            getStage(Stage.MISC).submit(task);
        }
    }

    public void failureRecoveryTaskCompleted(FailureRecoveryTask task)
    {
        synchronized (failureRecoveryTasks)
        {
            failureRecoveryTasks.remove(task);
        }
    }

    private final Map<Pair<FailureRecoveryRequest, InetAddress>, Long> lastFailureMessageTime = Maps.newHashMap();

    protected boolean shouldSendFailureRecoverMessage(FailureRecoveryRequest request, InetAddress endpoint)
    {
        long now = System.currentTimeMillis();
        Pair<FailureRecoveryRequest, InetAddress> key = Pair.create(request, endpoint);
        synchronized (lastFailureMessageTime)
        {
            if (lastFailureMessageTime.containsKey(key))
            {
                long then = lastFailureMessageTime.get(key);
                if (now - then < FAILURE_RECOVERY_MESSAGE_INTERVAL * 1000)
                    return false;
            }

            lastFailureMessageTime.put(key, now);
        }
        return true;
    }

    /**
     * starts a new failure recovery task for the given endpoint and token.
     */
    public void startRemoteFailureRecovery(InetAddress endpoint, Token token, UUID cfId, long epoch, Scope scope)
    {
        if (FailureDetector.instance.isAlive(endpoint))
        {
            FailureRecoveryRequest request = new FailureRecoveryRequest(token, cfId, epoch, scope);
            logger.debug("Sending {} to {}", request, endpoint);
            if (shouldSendFailureRecoverMessage(request, endpoint))
            {
                MessageOut<FailureRecoveryRequest> msg = new MessageOut<>(MessagingService.Verb.EPAXOS_FAILURE_RECOVERY,
                                                                          request,
                                                                          FailureRecoveryRequest.serializer);
                sendOneWay(msg, endpoint);
            }
        }
    }

    public IVerbHandler<FailureRecoveryRequest> getFailureRecoveryVerbHandler()
    {
        return new FailureRecoveryVerbHandler(this);
    }

    private void prepareForIncomingStream(Range<Token> range, UUID cfId, Scope scope)
    {
        //   -- this might be a good argument to have token states for every token, across all DCs
        TokenStateManager tokenStateManager = getTokenStateManager(scope);
        KeyStateManager keyStateManager = getKeyStateManager(scope);

        tokenStateManager.prepareForIncomingStream(range, cfId);
        Iterator<CfKey> cfKeys = keyStateManager.getCfKeyIterator(range, cfId, 10000);
        while (cfKeys.hasNext())
        {
            Set<UUID> toDelete = new HashSet<>();
            CfKey cfKey = cfKeys.next();
            keyStateManager.getCfKeyLock(cfKey).lock();
            try
            {
                KeyState ks = keyStateManager.loadKeyState(cfKey);

                toDelete.addAll(ks.getActiveInstanceIds());
                for (Set<UUID> ids: ks.getEpochExecutions().values())
                {
                    toDelete.addAll(ids);
                }
                keyStateManager.deleteKeyState(cfKey);
            }
            finally
            {
                keyStateManager.getCfKeyLock(cfKey).unlock();
            }

            // aquiring the instance lock after the key state lock can create
            // a deadlock, so we get all the instance ids we want to delete,
            // then delete them after we're done deleting the key state
            for (UUID id: toDelete)
            {
                deleteInstance(id);
            }
        }
    }

    // duplicates a lot of the FailureRecoveryTask.preRecover
    // TODO: rethink this method, it should work with multiple scopes and dcs gracefully
    public boolean prepareForIncomingStream(Range<Token> range, UUID cfId)
    {
        // check that this isn't for an existing failure recovery task
        synchronized (failureRecoveryTasks)
        {
            for (FailureRecoveryTask task: failureRecoveryTasks)
            {
                if (task.token.equals(range.right) && task.cfId.equals(cfId))
                    return false;
            }
        }

        // TODO: only delete local scope if the stream is coming from local DC
        // TODO: think through implications of multiple streams from multiple dcs happening
        prepareForIncomingStream(range, cfId, Scope.GLOBAL);
        prepareForIncomingStream(range, cfId, Scope.LOCAL);
        return true;
    }

    // repair / streaming support
    // TODO: maybe rename to hasExecutionInfoForTable:w
    public boolean managesCfId(UUID cfId)
    {
        for (TokenStateManager tokenStateManager: tokenStateManagers.values())
        {
            if (tokenStateManager.managesCfId(cfId))
                return true;
        }
        return remoteActivity.managesCfId(cfId);
    }

    /**
     * Returns a tuple containing the current epoch, and instances executed in the current epoch for the given key/cfId
     */
    public Map<Scope.DC, ExecutionInfo> getEpochExecutionInfo(ByteBuffer key, UUID cfId)
    {
        // TODO: test
        if (!managesCfId(cfId))
        {
            return null;
        }

        CfKey cfKey = new CfKey(key, cfId);
        Map<Scope.DC, ExecutionInfo> rmap = new HashMap<>();


        ExecutionInfo info;
        info = keyStateManagers.get(Scope.GLOBAL).getExecutionInfo(cfKey);
        if (info != null) rmap.put(Scope.DC.global(), info);

        info = keyStateManagers.get(Scope.LOCAL).getExecutionInfo(cfKey);
        if (info != null) rmap.put(getLocalScope(), info);

        rmap.putAll(remoteActivity.getExecutionInfo(key, cfId));

        return rmap;
    }

    // TODO: rename to data is from future, or something
    public boolean canApplyRepair(ByteBuffer key, UUID cfId, ExecutionInfo info, Scope scope)
    {
        return canApplyRepair(key, cfId, info.epoch, info.executed, scope);
    }

    /**
     * Determines if a repair can be applied to a key.
     */
    // TODO: rename to data is from future, or something
    public boolean canApplyRepair(ByteBuffer key, UUID cfId, long epoch, long executionCount, Scope scope)
    {
        ExecutionInfo local = getKeyStateManager(scope).getExecutionInfo(new CfKey(key, cfId));

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
            return getKeyStateManager(instance).canExecute(queryInstance.getQuery().getCfKey());
        }

        return true;
    }

    public boolean reportFutureExecutions(ByteBuffer key, UUID cfId, Map<Scope.DC, ExecutionInfo> executions)
    {
        boolean result = false;
        for (Map.Entry<Scope.DC, ExecutionInfo> entry: executions.entrySet())
        {
            boolean added = reportFutureExecution(key, cfId, entry.getKey(), entry.getValue());
            result |= added;
        }
        return result;
    }

    public boolean reportFutureExecution(ByteBuffer key, UUID cfId, Scope.DC dcScope, ExecutionInfo info)
    {
        if (dcScope.equals(Scope.DC.global()))
        {
            return getKeyStateManager(Scope.GLOBAL).reportFutureRepair(new CfKey(key, cfId), info);
        }
        else if (dcScope.equals(getLocalScope()))
        {
            return getKeyStateManager(Scope.LOCAL).reportFutureRepair(new CfKey(key, cfId), info);
        }
        else
        {
            // TODO: how does this fit into the blind writes?
            // TODO: execution info could include the highest timestamp for a keystate?
            remoteActivity.recordMutation(dcScope.dc, key, cfId, info);
            return true;
        }
    }

    public static String EXECUTION_INFO_PARAMETER = "EPX_NFO";

    protected static <T> Map<Scope.DC, ExecutionInfo> getMessageExecutionInfo(Map<String, byte[]> parameters, int version) throws IOException
    {
        byte[] d = parameters.get(EXECUTION_INFO_PARAMETER);
        if (d == null)
            return null;

        try (DataInputStream in = new DataInputStream(ByteBufferUtil.inputStream(ByteBuffer.wrap(d))))
        {
            return Serializers.executionMap.deserialize(in, version);
        }
    }

    protected static byte[] serializeMessageExecutionParameters(Map<Scope.DC, ExecutionInfo> info, int version) throws IOException
    {
        Map<String, byte[]> parameters = new HashMap<>();

        try (DataOutputBuffer out = new DataOutputBuffer((int) Serializers.executionMap.serializedSize(info, version)))
        {
            Serializers.executionMap.serialize(info, out, version);
            return out.getData();
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    /**
     * Adds epaxos execution info for the given key and cfid to the outgoing message
     */
    public <T> MessageOut<T> maybeAddExecutionInfo(ByteBuffer key, UUID cfId, MessageOut<T> msg, int version)
    {
        // TODO: test
        Map<Scope.DC, ExecutionInfo> info = getEpochExecutionInfo(key, cfId);
        if (info == null)
            return msg;

        try
        {
            byte[] data = serializeMessageExecutionParameters(info, version);
            assert data != null && data.length > 0;
            return msg.withParameter(EXECUTION_INFO_PARAMETER, data);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    /**
     * Record execution info from incoming stream/repair data
     */
    public <T> void maybeRecordExecutionInfo(ByteBuffer key, UUID cfId, MessageIn<T> msg) throws IOException
    {
        Map<Scope.DC, ExecutionInfo> info = getMessageExecutionInfo(msg.parameters, msg.version);
        // TODO: finish
    }

    /**
     * Checks message parameters for execution info, and returns true if the
     * execution info is from instances that haven't been recorded yet
     */
    public <T> boolean shouldApplyRepair(ByteBuffer key, UUID cfId, MessageIn<T> msg)
    {
        // TODO: test
        // TODO: only apply to remote activity if we can apply to to global and local
        // TODO: will inactive scope prevent any mutations

        byte[] d = msg.parameters.get(EXECUTION_INFO_PARAMETER);
        if (d == null)
            return true;

        try (DataInputStream in = new DataInputStream(ByteBufferUtil.inputStream(ByteBuffer.wrap(d))))
        {
            Map<Scope.DC, ExecutionInfo> info = Serializers.executionMap.deserialize(in, msg.version);
            Map<Scope.DC, ExecutionInfo> applyRemote = new HashMap<>();

            for (Map.Entry<Scope.DC, ExecutionInfo> entry: info.entrySet())
            {
                Scope.DC scope = entry.getKey();
                if (scope.equals(Scope.DC.global()))
                {
                    if (!canApplyRepair(key, cfId, entry.getValue(), Scope.GLOBAL))
                        return false;
                }
                else if (scope.equals(getLocalScope()))
                {
                    if (!canApplyRepair(key, cfId, entry.getValue(), Scope.LOCAL))
                        return false;
                }
                else
                {
                    applyRemote.put(entry.getKey(), entry.getValue());
                }
            }

            // record any remote activity
            // TODO: test remote entries are recorded
            for (Map.Entry<Scope.DC, ExecutionInfo> entry: applyRemote.entrySet())
            {
                remoteActivity.recordMutation(entry.getKey().dc, key, cfId, entry.getValue());
            }

            return true;
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    public Iterator<Pair<ByteBuffer, ExecutionInfo>> getRangeExecutionInfo(UUID cfId, Range<Token> range, ReplayPosition position)
    {
        // TODO: merge data from both ksms and the remote activity
        // return keyStateManager.getRangeExecutionInfo(cfId, range, position);
        return new ArrayList<Pair<ByteBuffer, ExecutionInfo>>().iterator();  // FIXME: this
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

            // FIXME: doesn't work at all I think
            Iterator<Pair<ByteBuffer, ExecutionInfo>> iter = getRangeExecutionInfo(cfId, range, position);

            while (iter.hasNext())
            {
                Pair<ByteBuffer, ExecutionInfo> next = iter.next();
                if (!filter.isPresent(next.left))
                {
                    logger.debug("skipping key not contained by sstable {}", range);
                    continue;
                }
                // TODO: uncomment
                // header.epaxos.put(next.left, next.right);
                if (logger.isDebugEnabled())
                {
                    logger.debug("Added {} {} to outgoing epaxos header", next.left, next.right);
                }

            }
        }
    }

    public void addMissingInstances(Collection<Instance> instances)
    {
        getStage(Stage.MUTATION).submit(new AddMissingInstances(this, instances));
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
            {
                return;
            }

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

            getKeyStateManager(instance).recordMissingInstance(instance);

            if (instance.getState().atLeast(Instance.State.COMMITTED))
            {
                notifyCommit(instance.getId());
                execute(instance.getId());
            }

        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public List<Instance> getInstanceCopies(Set<UUID> iids)
    {
        if (iids == null || iids.isEmpty())
            return Collections.<Instance>emptyList();

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

    public UUID addToken(UUID cfId, Token token, Scope scope)
    {
        TokenInstance instance = createTokenInstance(token, cfId, scope);
        preaccept(instance);
        return instance.getId();
    }

    // key state methods
    public Pair<Set<UUID>, Range<Token>> getCurrentDependencies(Instance instance)
    {
        return getKeyStateManager(instance).getCurrentDependencies(instance);
    }

    public void recordMissingInstance(Instance instance)
    {
        getKeyStateManager(instance).recordMissingInstance(instance);
    }

    public void recordAcknowledgedDeps(Instance instance)
    {
        getKeyStateManager(instance).recordAcknowledgedDeps(instance);
    }

    public void recordExecuted(Instance instance, ReplayPosition position, long maxTimestamp)
    {
        getKeyStateManager(instance).recordExecuted(instance, position, maxTimestamp);
        getTokenStateManager(instance).reportExecution(instance.getToken(), instance.getCfId());
    }

    public ReadWriteLock getInstanceLock(UUID iid)
    {
        return locks.get(iid);
    }

    // wrapped for testing
    public TracingAwareExecutorService getStage(Stage stage)
    {
        return StageManager.getStage(stage);
    }

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

    protected List<InetAddress> getNaturalEndpoints(String ks, Token token)
    {
        return StorageService.instance.getNaturalEndpoints(ks, token);
    }

    protected Collection<InetAddress> getPendingEndpoints(String ks, Token token)
    {
        return StorageService.instance.getTokenMetadata().pendingEndpointsFor(token, ks);
    }

    protected String getDc()
    {
        return getDc(FBUtilities.getBroadcastAddress());
    }

    protected String getDc(InetAddress endpoint)
    {
        return DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint);
    }

    protected Scope.DC getDcScope(Scope scope)
    {
        switch (scope)
        {
            case GLOBAL:
                return Scope.DC.global();
            case LOCAL:
                return getLocalScope();
            default:
                throw new IllegalArgumentException("Unsupported scope: " + scope);
        }
    }

    protected Scope.DC getLocalScope()
    {
        return Scope.DC.local(getDc());
    }

    public Scope[] getActiveScopes(UUID cfId, Range<Token> range, InetAddress address)
    {
        // TODO: don't request local if the address is in another DC
        // TODO: this
        return new Scope[]{Scope.GLOBAL, Scope.LOCAL};
    }

    public boolean coversScope(Scope.DC scope)
    {
        return Scope.DC.global().equals(scope) || getLocalScope().equals(scope);
    }

    protected String getInstanceKeyspace(Instance instance)
    {
        return Schema.instance.getCF(instance.getCfId()).left;
    }

    protected ParticipantInfo getParticipants(Instance instance)
    {
        String ks = getInstanceKeyspace(instance);

        List<InetAddress> naturalEndpoints = getNaturalEndpoints(ks, instance.getToken());
        Collection<InetAddress> pendingEndpoints = getPendingEndpoints(ks, instance.getToken());

        List<InetAddress> endpoints = ImmutableList.copyOf(Iterables.filter(Iterables.concat(naturalEndpoints, pendingEndpoints), duplicateFilter()));
        List<InetAddress> remoteEndpoints = null;
        if (instance.getConsistencyLevel() == ConsistencyLevel.LOCAL_SERIAL)
        {
            // Restrict naturalEndpoints and pendingEndpoints to node in the local DC only
            String localDc = getDc();
            Predicate<InetAddress> isLocalDc = dcPredicateFor(localDc, true);
            Predicate<InetAddress> notLocalDc = dcPredicateFor(localDc, false);

            remoteEndpoints = ImmutableList.copyOf(Iterables.filter(endpoints, notLocalDc));
            endpoints = ImmutableList.copyOf(Iterables.filter(endpoints, isLocalDc));
        }
        if (endpoints.isEmpty())
        {
            logger.warn("no endpoints found for instance: {} at {}, natural: {}, pending: {}, remote: {}",
                        instance, instance.getConsistencyLevel(), naturalEndpoints, pendingEndpoints, remoteEndpoints);
        }
        return new ParticipantInfo(endpoints, remoteEndpoints, instance.getConsistencyLevel());
    }

    protected boolean isAlive(InetAddress endpoint)
    {
        return FailureDetector.instance.isAlive(endpoint);
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
