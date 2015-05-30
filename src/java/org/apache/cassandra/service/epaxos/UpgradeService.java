/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service.epaxos;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * Handles upgrading a cluster to epaxos, and determining whether it's been upgraded (for queries)
 *
 * An non-upgraded node will read in the paxos table on startup, creating epaxos token states in the
 * INACTIVE states for the tables serial queries are being executed against. As new tables are queried,
 * inactive token states will be created for them as well.
 *
 * Token state behavior:
 *  INACTIVE: classic paxos is used for queries, epaxos isn't touched
 *  UPGRADING: classic paxos is used for queries, commits are mirrored into epaxos as executed instances
 *  NORMAL (or other upgraded state): epaxos is used
 *
 * Upgrading process for a given scoped token range is:
 *  1) Leader: create a new ballot value, send to every node that replicates the range
 *     Replica: respond with highest seen ballot, and state for the token state
 *     Leader:
 *       - if any of the replicas respond with an upgraded status for the token state, proceed to step 3
 *       - if all replicas agree with the ballot, and all are on the UPGRADING state, proceed to step 3
 *       - if all replicas agree with the ballot, and have an upgradeable state for that range (INACTIVE, UPGRADING), proceed to step 2
 *       - otherwise, abort
 *
 *  2) Leader: resend previous ballot, instruct all replicas to set their states to upgrading
 *     Replica:
 *       - if ballot provided is still the highers we've seen
 *       - and if the range is still in an upgradeable state,
 *       - then clear any previous token/key/instance data for that range, set state to UPGRADING, and respond with success
 *       - otherwise, respond with failure
 *     Leader:
 *       - if all replicas respond with success, proceed to step 3
 *       - otherwise, abort
 *
 *  3) Leader: send messages to all replicas instructing them to set token state status to normal
 *     Replica: always set status to normal (if not upgraded already)
 *
 */
// TODO: bootstrapping clusters should default to epaxos
// TODO: new nodes should default to upgraded in a upgraded cluster (should be handled by streaming)
public class UpgradeService
{
    private static class Handle
    {
        private static final UpgradeService instance = new UpgradeService();
    }

    public static UpgradeService instance()
    {
        return Handle.instance;
    }

    public static class UpgradeFailure extends Exception
    {
        public UpgradeFailure(String message)
        {
            super(message);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(EpaxosService.class);
    private static final Comparator<UUID> comparator = new Comparator<UUID>()
    {
        @Override
        public int compare(UUID o1, UUID o2)
        {
            if (o1.version() == 1 && o2.version() == 1)
            {
                return DependencyGraph.comparator.compare(o1, o2);
            }
            return o1.compareTo(o2);
        }
    };

    private static final int UPGRADE_KEY = 0; // partition key for upgrade table
    private static final UUID MIN_BALLOT = new UUID(Long.MIN_VALUE, Long.MIN_VALUE);

    private final EpaxosService service;
    private final String keyspace;
    private final String paxosTable;
    private final String upgradeTable;

    private volatile boolean started = false;
    private volatile boolean upgraded = false;
    private volatile UUID lastBallot = MIN_BALLOT;

    public UpgradeService()
    {
        this(EpaxosService.getInstance());
    }

    public UpgradeService(EpaxosService service)
    {
        this(service, SystemKeyspace.PAXOS_CF, SystemKeyspace.PAXOS_UPGRADE);
    }

    public UpgradeService(EpaxosService service, String paxosTable, String upgradeTable)
    {
        this.service = service;
        this.paxosTable = paxosTable;
        this.upgradeTable = upgradeTable;
        keyspace = service.getKeyspace();
    }

    private void saveState()
    {
        String dml = String.format("INSERT INTO %s.%s (key, upgraded, last_ballot) VALUES (?,?,?)", keyspace, upgradeTable);
        QueryProcessor.executeInternal(dml, UPGRADE_KEY, upgraded, lastBallot);
    }

    /**
     * loads the last state from the system table. Returns true
     * if state was loaded, false if there was no state to load
     */
    private boolean loadState()
    {
        String query = String.format("SELECT * FROM %s.%s WHERE key=%s", keyspace, upgradeTable, UPGRADE_KEY);
        UntypedResultSet rows = QueryProcessor.executeInternal(query);
        if (rows.isEmpty())
            return false;

        assert rows.size() == 1;
        UntypedResultSet.Row row = rows.one();
        assert row.has("upgraded") && row.has("last_ballot");
        upgraded = row.getBoolean("upgraded");
        lastBallot = row.getUUID("last_ballot");
        return true;
    }

    public void checkStarted()
    {
        if (!started)
            throw new AssertionError("UpgradeService isn't started");
    }

    public synchronized void start()
    {
        if (started)
            return;

        // TODO: add some flags to force things (setting upgraded true, removing inactive states, putting node in non-upgraded state, ignoring previous state)
        if (!loadState())
        {
            Token minToken = DatabaseDescriptor.getPartitioner().getMinimumToken();
            Range<Token> everything = new Range<>(minToken, minToken);
            KeyTableIterable iterable = new KeyTableIterable(keyspace, paxosTable, everything, true);

            // TODO: discourage, but handle ring mutations - needs to recompute the token states and cancel any in progress upgrades
            // create inactive epaxos token states for any previous paxos instances
            for (UntypedResultSet.Row row: iterable)
            {
                service.getTokenStateManager(Scope.GLOBAL).getOrInitManagedCf(row.getUUID("cf_id"), TokenState.State.INACTIVE);
                service.getTokenStateManager(Scope.LOCAL).getOrInitManagedCf(row.getUUID("cf_id"), TokenState.State.INACTIVE);
            }
        }
        started = true;
    }

    // runtime stuff

    public boolean isUpgradedForQuery(Token token, UUID cfId, Scope scope)
    {
        checkStarted();
        // TODO: test
        if (upgraded)
            return true;
        // TODO: test
        TokenState ts = service.getTokenStateManager(scope).getWithDefaultState(token, cfId, TokenState.State.INACTIVE);
        return TokenState.State.isUpgraded(ts.getState());
    }

    /**
     * after token states are set to normal, this checks all token
     * states, and sets the status to upgraded if they're all normal
     */
    protected void reevaluateUpgradedStatus()
    {
        // TODO: this
    }

    public void reportEpaxosActivity(Token token, UUID cfId, Scope scope)
    {
        checkStarted();
        if (upgraded)
            return;

        TokenStateManager tsm = service.getTokenStateManager(scope);
        TokenState ts = tsm.get(token, cfId);

        if (!TokenState.State.isUpgraded(ts.getState()))
        {
            ts.lock.writeLock().lock();
            try
            {
                if (!TokenState.State.isUpgraded(ts.getState()))
                {
                    ts.setState(TokenState.State.NORMAL);
                    tsm.save(ts);
                }
            }
            finally
            {
                ts.lock.writeLock().unlock();
            }
        }
        // TODO: re-evaluate upgraded state
    }

    public void reportPaxosCommit()
    {
        checkStarted();

    }

    // upgrade stuff

    public void upgradeNode()
    {
        // TODO: fancy jmx notifications
        checkStarted();
        if (upgraded)
        {
            // TODO: test
            logger.info("Paxos already upgraded");
            return;
        }
        // TODO: maybe lock queries on new cfIds
        Set<UUID> cfIds = new HashSet<>();
        cfIds.addAll(service.getTokenStateManager(Scope.GLOBAL).getAllManagedCfIds());
        cfIds.addAll(service.getTokenStateManager(Scope.LOCAL).getAllManagedCfIds());

        for (UUID cfId: cfIds)
        {
            logger.info("Upgrading paxos on {}", Schema.instance.getCF(cfId));
            upgradeCfScope(cfId, Scope.GLOBAL);
            upgradeCfScope(cfId, Scope.LOCAL);
        }
    }

    protected void upgradeCfScope(UUID cfId, Scope scope)
    {
        int numUpgraded = 0;
        int failures = 0;
        Pair<String, String> tbl = Schema.instance.getCF(cfId);
        TokenStateManager tsm = service.getTokenStateManager(scope);
        if (tsm.managesCfId(cfId))
        {
            for (Token token: tsm.allTokenStatesForCf(cfId))
            {
                TokenState ts = tsm.getExact(token, cfId);
                assert ts != null;

                if (TokenState.State.isUpgraded(ts.getState()))
                {
                    // TODO: test
                    logger.debug("Range {}:{} for {} is already upgraded, skipping", ts.getRange(), scope, tbl);
                    continue;
                }

                try
                {
                    upgradeTokenState(ts.getRange(), cfId, scope);
                    numUpgraded++;
                }
                catch (UpgradeFailure e)
                {
                    logger.warn("Unable to upgrade {}:{} for {}. {}", ts.getRange(), scope, tbl, e.getMessage());
                    failures++;
                }
            }
        }

        if (numUpgraded == 0)
        {
            logger.info("No ranges to upgrade for {}", Schema.instance.getCF(cfId));
        }

        if (failures == 0)
        {
            upgraded = true;
            saveState();
        }
    }

    public static enum Stage { BEGIN, UPGRADE, COMPLETE }


    protected synchronized boolean checkNewBallot(UUID ballot)
    {
        if (comparator.compare(ballot, lastBallot) > 0)
        {
            lastBallot = ballot;
            saveState();
            return true;
        }
        else
        {
            return false;
        }
    }

    protected synchronized UUID newBallot()
    {
        UUID newBallot;
        // don't save, that will be done by the verb handler
        do
        {
            newBallot = UUIDGen.getTimeUUID();
        }
        while (comparator.compare(newBallot, lastBallot) > 0);
        return newBallot;
    }

    private MessageOut<Request> getMessage(Request request)
    {
        return new MessageOut<>(MessagingService.Verb.PAXOS_UPGRADE, request, Request.serializer);
    }

    protected void refreshRanges(Range<Token> range, UUID cfId, Scope scope)
    {
        // TODO: this, called when mismatched ranges are encountered during upgrade
    }

    @VisibleForTesting
    Callback createCallback(Collection<InetAddress> targets)
    {
        return new Callback(targets);
    }

    /**
     *
     * @return run upgrade phase if true, skip it otherwise
     * @throws UpgradeFailure
     */
    protected boolean begin(Range<Token> range, UUID cfId, Scope scope, Collection<InetAddress> endpoints, final UUID ballot) throws UpgradeFailure
    {
        Request request = new Request(range, cfId, scope, ballot, Stage.BEGIN);
        Callback callback = createCallback(endpoints);
        for (InetAddress endpoint : endpoints)
        {
            service.sendRR(getMessage(request), endpoint, callback);
        }
        callback.await();

        Collection<Response> responses = callback.getResponses().values();

        if (callback.wasSuccessful())
        {
            // skip upgrade step if all ballots agreed, and all nodes were in the upgrading state or any were already upgraded
            return !Iterables.any(responses, Response.upgraded) && !Iterables.all(responses, Response.upgrading);
        }
        else
        {
            if (Iterables.any(responses, Response.upgraded))
            {
                return false;
            }
            if (Iterables.any(responses, new Response.RangeMismatch(range)))
            {
                refreshRanges(range, cfId, scope);
            }
            // upgrade our ballot
            for (Response response: responses)
            {
                checkNewBallot(response.ballot);
            }
            for (Response response: responses)
            {
                if (!response.success)
                {
                    throw new UpgradeFailure(response.failureReason);
                }
            }
        }
        throw new AssertionError("Shouldn't be reachable");
    }

    protected void upgrade(Range<Token> range, UUID cfId, Scope scope, Collection<InetAddress> endpoints, final UUID ballot) throws UpgradeFailure
    {
        Request request = new Request(range, cfId, scope, ballot, Stage.UPGRADE);
        Callback callback = createCallback(endpoints);
        for (InetAddress endpoint : endpoints)
        {
            service.sendRR(getMessage(request), endpoint, callback);
        }
        callback.await();

        Collection<Response> responses = callback.getResponses().values();
        if (!callback.wasSuccessful())
        {
            if (Iterables.any(responses, Response.upgraded))
            {
                return;
            }
            if (Iterables.any(responses, new Response.RangeMismatch(range)))
            {
                refreshRanges(range, cfId, scope);
            }
            for (Response response: responses)
            {
                if (!response.success)
                {
                    throw new UpgradeFailure(response.failureReason);
                }
            }
        }
        else
        {
            // if every response isn't in the UPGRADED state and the round was successful, something is wrong
            if (!Iterables.any(responses, Response.upgraded) && !Iterables.all(responses, Response.upgrading))
            {
                throw new UpgradeFailure("Some responses unexpectedly not UPGRADED");
            }
        }
    }

    protected void complete(Range<Token> range, UUID cfId, Scope scope, Collection<InetAddress> endpoints, final UUID ballot)
    {
        Request request = new Request(range, cfId, scope, ballot, Stage.COMPLETE);
        for (InetAddress endpoint : endpoints)
        {
            service.sendOneWay(getMessage(request), endpoint);
        }
    }

    protected void upgradeTokenState(Range<Token> range, UUID cfId, Scope scope) throws UpgradeFailure
    {
        EpaxosService.ParticipantInfo pi = service.getParticipants(range.right, cfId, scope);

        if (pi.endpoints.size() != pi.liveEndpoints.size())
        {
            Set<InetAddress> dead = Sets.difference(Sets.newHashSet(pi.endpoints), Sets.newHashSet(pi.liveEndpoints));
            String message = String.format("All token replicas must be live to upgrade. %s are down.", dead);
            throw new UpgradeFailure(message);
        }

        final UUID ballot = newBallot();

        boolean runUpgrade = begin(range, cfId, scope, pi.endpoints, ballot);

        if (runUpgrade)
        {
            upgrade(range, cfId, scope, pi.endpoints, ballot);
        }

        complete(range, cfId, scope, pi.endpoints, ballot);
    }

    protected synchronized Response handleRequest(Request request)
    {
        checkStarted();
        // TODO: handle cases where the node is already upgraded
        // TODO: test everything
        TokenStateManager tsm = service.getTokenStateManager(request.scope);
        tsm.getOrInitManagedCf(request.cfId, upgraded ? TokenState.State.NORMAL : TokenState.State.INACTIVE);
        TokenState ts = tsm.get(request.range.right, request.cfId);
        ts.lock.writeLock().lock();

        if (TokenState.State.isUpgraded(ts.getState()))
        {
            return new Response(false, lastBallot, ts.getRange(), ts.getState(), "already upgraded");
        }

        if (!ts.getRange().equals(request.range))
        {
            return new Response(false, lastBallot, ts.getRange(), ts.getState(), "range mismatch");
        }

        try
        {
            switch (request.stage)
            {
                case BEGIN:
                    if (!checkNewBallot(request.ballot))
                    {
                        return new Response(false, lastBallot, ts.getRange(), ts.getState(), "newer ballot encountered");
                    }

                    return new Response(true, lastBallot, ts.getRange(), ts.getState());

                case UPGRADE:
                    if (!request.ballot.equals(lastBallot))
                    {
                        return new Response(false, lastBallot, ts.getRange(), ts.getState(), "different ballot encountered");
                    }

                    service.clearTokenStateData(ts, request.scope, TokenState.State.UPGRADING);
                    return new Response(true, lastBallot, ts.getRange(), ts.getState());

                case COMPLETE:
                    ts.setState(TokenState.State.NORMAL);
                    tsm.save(ts);
                    return null;
            }
            return null;
        }
        finally
        {
            ts.lock.writeLock().unlock();
        }
    }

    public class Callback implements IAsyncCallback<Response>
    {
        private final Set<InetAddress> targets;
        private final CountDownLatch latch;
        private final Map<InetAddress, Response> responses = new HashMap<>();
        private boolean complete = false;
        private boolean success = true;

        public Callback(Collection<InetAddress> targets)
        {
            this.targets = ImmutableSet.copyOf(targets);
            latch = new CountDownLatch(targets.size());
        }

        @Override
        public synchronized void response(MessageIn<Response> msg)
        {
            if (complete)
            {
                logger.debug("ignoring response for completed round from {}", msg.from);
                return;
            }

            if (responses.containsKey(msg.from))
            {
                logger.debug("ignoring duplicate response from {}", msg.from);
                return;
            }

            if (!targets.contains(msg.from))
            {
                logger.warn("ignoring unexpected response from {}", msg.from);
                return;
            }
            Response response = msg.payload;
            responses.put(msg.from, response);
            latch.countDown();

            if (latch.getCount() == 0)
            {
                complete = true;
            }

            if (!response.success)
            {
                success = false;
                complete = true;
                while (latch.getCount() > 0)
                {
                    latch.countDown();
                }
            }
        }

        public synchronized Map<InetAddress, Response> getResponses()
        {
            return responses;
        }

        public synchronized boolean wasSuccessful()
        {
            return success;
        }

        public boolean isComplete()
        {
            return complete;
        }

        public void await() throws UpgradeFailure
        {
            try
            {
                if (!latch.await(service.getUpgradeTimeout(), TimeUnit.MILLISECONDS))
                    throw new UpgradeFailure("timeout waiting on replies");
            }
            catch (InterruptedException ex)
            {
                throw new AssertionError("This latch shouldn't have been interrupted.");
            }
        }

        @Override
        public boolean isLatencyForSnitch()
        {
            return false;
        }
    }

    public static class Request
    {
        public final Range<Token> range;
        public final UUID cfId;
        public final Scope scope;
        public final UUID ballot;
        public final Stage stage;

        public Request(Range<Token> range, UUID cfId, Scope scope, UUID ballot, Stage stage)
        {
            this.range = range;
            this.cfId = cfId;
            this.scope = scope;
            this.ballot = ballot;
            this.stage = stage;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Request request = (Request) o;

            if (!ballot.equals(request.ballot)) return false;
            if (!cfId.equals(request.cfId)) return false;
            if (!range.equals(request.range)) return false;
            if (scope != request.scope) return false;
            if (stage != request.stage) return false;

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = range.hashCode();
            result = 31 * result + cfId.hashCode();
            result = 31 * result + scope.hashCode();
            result = 31 * result + ballot.hashCode();
            result = 31 * result + stage.hashCode();
            return result;
        }

        @Override
        public String toString()
        {
            return "Request{" +
                   "range=" + range +
                   ", cfId=" + cfId +
                   ", scope=" + scope +
                   ", ballot=" + ballot +
                   ", stage=" + stage +
                   '}';
        }

        public static final IVersionedSerializer<Request> serializer = new IVersionedSerializer<Request>()
        {
            @Override
            public void serialize(Request request, DataOutputPlus out, int version) throws IOException
            {
                Token.serializer.serialize(request.range.left, out);
                Token.serializer.serialize(request.range.right, out);
                UUIDSerializer.serializer.serialize(request.cfId, out, version);
                Scope.serializer.serialize(request.scope, out, version);
                UUIDSerializer.serializer.serialize(request.ballot, out, version);
                out.writeInt(request.stage.ordinal());

            }

            @Override
            public Request deserialize(DataInput in, int version) throws IOException
            {
                return new Request(new Range<>(Token.serializer.deserialize(in), Token.serializer.deserialize(in)),
                                   UUIDSerializer.serializer.deserialize(in, version),
                                   Scope.serializer.deserialize(in, version),
                                   UUIDSerializer.serializer.deserialize(in, version),
                                   Stage.values()[in.readInt()]);
            }

            @Override
            public long serializedSize(Request request, int version)
            {
                long size = Token.serializer.serializedSize(request.range.left, TypeSizes.NATIVE);
                size += Token.serializer.serializedSize(request.range.right, TypeSizes.NATIVE);
                size += UUIDSerializer.serializer.serializedSize(request.cfId, version);
                size += Scope.serializer.serializedSize(request.scope, version);
                size += UUIDSerializer.serializer.serializedSize(request.ballot, version);
                size += 4;
                return size;
            }
        };
    }

    public static class Response
    {
        public final boolean success;
        public final UUID ballot;
        public final Range<Token> range;
        public final TokenState.State state;
        public final String failureReason;

        public Response(boolean success, UUID ballot, Range<Token> range, TokenState.State state)
        {
            this(success, ballot, range, state, "");
        }

        public Response(boolean success, UUID ballot, Range<Token> range, TokenState.State state, String failureReason)
        {
            this.success = success;
            this.ballot = ballot;
            this.range = range;
            this.state = state;
            this.failureReason = failureReason;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Response response = (Response) o;

            if (success != response.success) return false;
            if (!ballot.equals(response.ballot)) return false;
            if (!range.equals(response.range)) return false;
            if (state != response.state) return false;

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = (success ? 1 : 0);
            result = 31 * result + ballot.hashCode();
            result = 31 * result + range.hashCode();
            result = 31 * result + state.hashCode();
            return result;
        }

        @Override
        public String toString()
        {
            return "Response{" +
                   "success=" + success +
                   ", ballot=" + ballot +
                   ", range=" + range +
                   ", state=" + state +
                   ", failureReason='" + failureReason + '\'' +
                   '}';
        }

        public static final IVersionedSerializer<Response> serializer = new IVersionedSerializer<Response>()
        {
            @Override
            public void serialize(Response response, DataOutputPlus out, int version) throws IOException
            {
                out.writeBoolean(response.success);
                UUIDSerializer.serializer.serialize(response.ballot, out, version);
                Token.serializer.serialize(response.range.left, out);
                Token.serializer.serialize(response.range.right, out);
                out.writeInt(response.state.ordinal());
                out.writeUTF(response.failureReason);
            }

            @Override
            public Response deserialize(DataInput in, int version) throws IOException
            {
                return new Response(in.readBoolean(),
                                    UUIDSerializer.serializer.deserialize(in, version),
                                    new Range<>(Token.serializer.deserialize(in), Token.serializer.deserialize(in)),
                                    TokenState.State.values()[in.readInt()],
                                    in.readUTF());
            }

            @Override
            public long serializedSize(Response response, int version)
            {
                long size = 1;
                size += UUIDSerializer.serializer.serializedSize(response.ballot, version);
                size += Token.serializer.serializedSize(response.range.left, TypeSizes.NATIVE);
                size += Token.serializer.serializedSize(response.range.right, TypeSizes.NATIVE);
                size += 4;
                size += TypeSizes.NATIVE.sizeof(response.failureReason);
                return size;
            }
        };

        static final Predicate<Response> upgrading = new Predicate<Response>()
        {
            @Override
            public boolean apply(Response response)
            {
                return response.state == TokenState.State.UPGRADING;
            }
        };

        static final Predicate<Response> upgraded = new Predicate<Response>()
        {
            @Override
            public boolean apply(Response response)
            {
                return TokenState.State.isUpgraded(response.state);
            }
        };

        static class RangeMismatch implements Predicate<Response>
        {
            private final Range<Token> expected;

            RangeMismatch(Range<Token> expected)
            {
                this.expected = expected;
            }

            @Override
            public boolean apply(Response response)
            {
                return response.range.equals(expected);
            }
        }
    }

    public class Handler implements IVerbHandler<Request>
    {
        @Override
        public void doVerb(MessageIn<Request> message, int id)
        {
            Response response = handleRequest(message.payload);
            if (message.payload.stage != Stage.COMPLETE)
            {
                MessageOut<Response> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE, response, Response.serializer);
                service.sendReply(reply, id, message.from);
            }
        }
    }

    public IVerbHandler<Request> getVerbHandler()
    {
        return new Handler();
    }
}
