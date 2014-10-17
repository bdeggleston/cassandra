package org.apache.cassandra.service.epaxos;

import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.google.common.util.concurrent.Striped;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.apache.cassandra.service.epaxos.exceptions.PrepareAbortException;
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

    private static final int PREPARE_BALLOT_FAILURE_RETRIES = 5;

    private final ConcurrentMap<UUID, Instance> instances = Maps.newConcurrentMap();
    private final Striped<ReadWriteLock> locks = Striped.readWriteLock(DatabaseDescriptor.getConcurrentWriters() * 1024);

    private final Random random = new Random();

    private final Predicate<InetAddress> nonLocalPredicate = new Predicate<InetAddress>()
    {
        @Override
        public boolean apply(InetAddress inetAddress)
        {
            return !getEndpoint().equals(inetAddress);
        }
    };

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

    public ColumnFamily query(SerializedRequest query)
            throws InvalidRequestException, UnavailableException, InvalidInstanceStateChange, WriteTimeoutException, BallotException
    {
        query.getConsistencyLevel().validateForCas();

        Instance instance = createInstance(query);

        AcceptDecision acceptDecision = preaccept(instance);

        if (acceptDecision.acceptNeeded)
            accept(instance, acceptDecision);

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
        ParticipantInfo participantInfo = getParticipants(instance);
        ReadWriteLock lock = locks.get(instance.getId());
        lock.writeLock().lock();
        try
        {
            instance.preaccept(getCurrentDependencies(instance.getQuery()));
            List<InetAddress> successors = Lists.newArrayList(Iterables.filter(participantInfo.endpoints, nonLocalPredicate));
            Collections.shuffle(successors, getRandom());
            instance.setSuccessors(successors);
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

        for (Instance missingInstance: callback.getMissingInstances())
            addMissingInstance(missingInstance);

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
                    {
                        response = PreacceptResponse.success(instance);
                    }
                    else
                    {
                        Set<UUID> missingInstanceIds = Sets.difference(instance.getDependencies(), remoteInstance.getDependencies());
                        missingInstanceIds.remove(instance.getId());
                        response = PreacceptResponse.failure(instance, getInstanceCopies(missingInstanceIds));
                    }
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

    private List<Instance> getInstanceCopies(Set<UUID> iids)
    {
        if (iids == null || iids.size() == 0)
            return Collections.EMPTY_LIST;

        List<Instance> instances = Lists.newArrayListWithCapacity(iids.size());
        for (UUID iid: iids)
        {
            ReadWriteLock missingLock = locks.get(iid);
            missingLock.readLock().lock();
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
                missingLock.readLock().unlock();
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

    public void accept(Instance instance, AcceptDecision decision) throws InvalidInstanceStateChange, UnavailableException, WriteTimeoutException, BallotException
    {
        ReadWriteLock lock = locks.get(instance.getId());
        lock.writeLock().lock();
        try
        {
            instance.accept(decision.acceptDeps);
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

            callback = getAcceptCallback(instance, participantInfo);
            for (InetAddress endpoint : participantInfo.liveEndpoints)
                if (!endpoint.equals(getEndpoint()))
                {
                    Set<UUID> missingIds = decision.missingInstances.get(endpoint);
                    AcceptRequest request = new AcceptRequest(instance, getInstanceCopies(missingIds));
                    MessageOut<AcceptRequest> message = new MessageOut<>(MessagingService.Verb.ACCEPT_REQUEST,
                                                                         request,
                                                                         AcceptRequest.serializer);
                    sendRR(message, endpoint, callback);
                }
                else
                {
                    callback.countLocal();
                }
        }
        finally
        {
            lock.readLock().unlock();
        }

        callback.await();
        callback.checkSuccess();
    }

    protected class AcceptVerbHandler implements IVerbHandler<AcceptRequest>
    {
        @Override
        public void doVerb(MessageIn<AcceptRequest> message, int id)
        {
            Instance remoteInstance = message.payload.instance;
            ReadWriteLock lock = locks.get(remoteInstance.getId());
            lock.writeLock().lock();
            try
            {
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

                for (Instance missing: message.payload.missingInstances)
                    addMissingInstance(missing);

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

    public IVerbHandler<AcceptRequest> getAcceptVerbHandler()
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
            catch (UnavailableException | WriteTimeoutException | BallotException e)
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

    public void execute(Instance instance) throws UnavailableException, WriteTimeoutException, BallotException
    {
        while (true)
        {
            ExecutionSorter executionSorter = new ExecutionSorter(instance, accessor);
            executionSorter.buildGraph();

            if (executionSorter.uncommitted.size() > 0)
            {
                for (UUID iid : executionSorter.uncommitted)
                {
                    Instance toPrepare = loadInstance(iid);
                    if (toPrepare == null)
                    {
                        // this is only a problem if there's only a single uncommitted instance
                        if (executionSorter.uncommitted.size() > 1)
                            continue;
                        throw new AssertionError("Missing instance for prepare: " + iid.toString());
                    }

                    prepare(toPrepare);
                }
            }
            else
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

    // TODO: this might be better as a runnable we can throw into a thread pool
    public void prepare(Instance instance) throws UnavailableException, WriteTimeoutException, BallotException
    {
        for (int i=0; i<PREPARE_BALLOT_FAILURE_RETRIES; i++)
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
                    MessageOut<PrepareRequest> message = new MessageOut<>(MessagingService.Verb.PREPARE_REQUEST,
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

                lock.writeLock().lock();
                try
                {
                    PrepareDecision decision = callback.getDecision();
                    AcceptDecision acceptDecision = null;
                    Set<UUID> deps = decision.deps;
                    switch (decision.state)
                    {
                        case PREACCEPTED:
                            boolean fullPreaccept = true;
                            for (TryPreacceptAttempt attempt: decision.tryPreacceptAttempts)
                            {
                                try
                                {
                                    if (tryPreaccept(instance, attempt, participantInfo))
                                    {
                                        fullPreaccept = false;
                                        deps = attempt.dependencies;
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
                            if (fullPreaccept)
                            {
                                // reassigning will transmit any missing dependencies
                                acceptDecision = preaccept(instance);
                                deps = acceptDecision.acceptDeps;
                            }

                        case ACCEPTED:
                            assert deps != null;
                            acceptDecision = acceptDecision != null ? acceptDecision
                                                                    : new AcceptDecision(true, deps, Collections.EMPTY_MAP);
                            accept(instance, acceptDecision);
                        case COMMITTED:
                            assert deps != null;
                            instance.setDependencies(deps);
                            commit(instance);
                            break;
                        default:
                            throw new AssertionError();
                    }
                }
                catch (BallotException e)
                {
                    logger.debug("Prepare ballot failure " + instance.getId());
                    if (i >= PREPARE_BALLOT_FAILURE_RETRIES)
                    {
                        throw e;
                    }
                    else
                    {
                        // TODO: sleep, sleep times should be slightly random and proportional to succession order
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
    }

    protected boolean shouldPrepare(Instance instance)
    {
        ReadWriteLock lock = locks.get(instance.getId());
        lock.readLock().lock();
        try
        {
            if (instance.getState() == Instance.State.COMMITTED || instance.getState() == Instance.State.EXECUTED)
                return false;

            // TODO: wait for successors and grace periods
            return true;
        }
        finally
        {
            lock.readLock().unlock();
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

    protected TryPreacceptCallback getTryPreacceptCallback(Instance instance, TryPreacceptAttempt attempt, ParticipantInfo participantInfo)
    {
        return new TryPreacceptCallback(instance, attempt, participantInfo);
    }

    public boolean tryPreaccept(Instance instance, TryPreacceptAttempt attempt, ParticipantInfo participantInfo) throws WriteTimeoutException, PrepareAbortException
    {
        TryPreacceptRequest request = new TryPreacceptRequest(instance.getId(), attempt.dependencies);
        MessageOut<TryPreacceptRequest> message = new MessageOut<>(MessagingService.Verb.TRYPREACCEPT_REQUEST,
                                                                   request,
                                                                   TryPreacceptRequest.serializer);
        TryPreacceptCallback callback = getTryPreacceptCallback(instance, attempt, participantInfo);
        for (InetAddress endpoint: attempt.toConvince)
        {
            if (!endpoint.equals(getEndpoint()))
            {
                sendRR(message, endpoint, callback);
            }
            else
            {
                callback.recordDecision(handleTryPreaccept(instance, attempt.dependencies));
            }
        }

        callback.await();
        return callback.successful();
    }

    protected TryPreacceptDecision handleTryPreaccept(Instance instance, Set<UUID> dependencies)
    {
        // TODO: reread the try preaccept stuff, dependency management may break it if we're not careful

        // get the ids of instances the the message instance doesn't have in it's dependencies
        Set<UUID> conflictIds = Sets.newHashSet(getCurrentDependencies(instance.getQuery()));
        conflictIds.removeAll(dependencies);
        conflictIds.remove(instance.getId());

        // if this node hasn't seen some of the proposed dependencies, don't preaccept them
        for (UUID dep: dependencies)
        {
            if (loadInstance(dep) == null)
                return TryPreacceptDecision.REJECTED;
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
                return TryPreacceptDecision.CONTENDED;
            }

            // if the instance in question isn't a dependency of the potential
            // conflict, then it couldn't have been committed on the fast path
            if (!conflict.getDependencies().contains(instance.getId()))
            {
                return TryPreacceptDecision.REJECTED;
            }
        }

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
            Instance instance = loadInstance(message.payload.iid);
            TryPreacceptDecision decision = handleTryPreaccept(instance, message.payload.dependencies);
            TryPreacceptResponse response = new TryPreacceptResponse(instance.getId(), decision);
            MessageOut<TryPreacceptResponse> reply = new MessageOut<>(MessagingService.Verb.TRYPREACCEPT_RESPONSE,
                                                                      response,
                                                                      TryPreacceptResponse.serializer);
            sendReply(reply, id, message.from);
        }
    }

    public IVerbHandler<TryPreacceptRequest> getTryPreacceptVerbHandler()
    {
        return new TryPreacceptVerbHandler();
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

    protected Instance addMissingInstance(Instance remoteInstance)
    {
        ReadWriteLock lock = locks.get(remoteInstance.getId());
        lock.writeLock().lock();
        try
        {
            Instance instance = loadInstance(remoteInstance.getId());
            if (instance != null)
                return instance;


            // TODO: guard against re-adding expired instances once we start gc'ing them
            instance = remoteInstance.copyRemote();
            Instance previous = instances.putIfAbsent(instance.getId(), instance);
            if (previous == null)
            {
                // be careful if the instance is only preaccepted
                // if a preaccepted instance from another node is blindly added,
                // it can cause problems during the prepare phase
                if (!instance.getState().atLeast(Instance.State.ACCEPTED))
                {
                    instance.setDependencies(null);
                    instance.setPlaceholder(true);  // TODO: exclude from prepare and trypreaccept
                }
                saveInstance(instance);
            }
            else
            {
                instance = previous;
            }

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
