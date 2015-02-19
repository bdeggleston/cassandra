package org.apache.cassandra.service.epaxos;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ning.compress.lzf.LZFOutputStream;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.*;

public class InstanceStreamWriter
{
    private static final Logger logger = LoggerFactory.getLogger(InstanceStreamWriter.class);

    private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;
    private static final int CFKEY_ITER_CHUNK = 10000;

    private final EpaxosState state;
    private final TokenStateManager manager;
    private final UUID cfId;
    private final Range<Token> range;
    private final StreamManager.StreamRateLimiter limiter;
    private long bytesSinceFlush = 0;

    public InstanceStreamWriter(UUID cfId, Range<Token> range, InetAddress peer)
    {
        this(EpaxosState.getInstance(), cfId, range, peer);
    }

    public InstanceStreamWriter(EpaxosState state, UUID cfId, Range<Token> range, InetAddress peer)
    {
        this.state = state;
        manager = state.tokenStateManager;
        this.cfId = cfId;
        this.range = range;
        limiter = StreamManager.getRateLimiter(peer);
    }

    protected TokenState getTokenState()
    {
        return state.tokenStateManager.get(range.left, cfId);
    }

    protected boolean replicates(Token token)
    {
        return Keyspace.open(Schema.instance.getCF(cfId).left).getReplicationStrategy().getCachedEndpoints(token).contains(state.getEndpoint());
    }

    private TokenState getExact(Token token)
    {
        return state.tokenStateManager.getExact(token, cfId);
    }

    private Token getNext(Token last)
    {
        Predicate<Token> covered = new Predicate<Token>()
        {
            public boolean apply(Token token)
            {
                return range.contains(token);
            }
        };

        Set<Token> tokenSet = Sets.newHashSet(Iterables.filter(state.tokenStateManager.allTokenStatesForCf(cfId), covered));
        tokenSet.add(range.left);
        tokenSet.add(range.right);
        ArrayList<Token> tokens = Lists.newArrayList(tokenSet);
        assert tokens.size() > 1;

        // rotate the list so the range start is at the head
        while (!tokens.get(0).equals(range.left))
            Collections.rotate(tokens, 1);

        for (Token token: tokens)
        {
            if (token.compareTo(last) > 0)
                return token;
        }
        throw new AssertionError("No tokens found");
    }

    /**
     * a true is written to the channel before any instances for a key is sent, and a false
     * is sent after. This let's the receiver know that we're not sending any more instances
     */
    public void write(WritableByteChannel channel) throws IOException
    {
        // TODO: find all managed tokens in the given range.
        // TODO: create a token state for the right token, if neccesary
        // TODO: read-lock all token states to be streamed
        // TODO: transmit poison pill token states if token states couldn't be created for the edges

        // TODO: unwrap range
        // TODO: handle multiple token states

        if (getExact(range.right) == null)
        {
            logger.debug("Creating token state for right token {}", range.right);
            TokenInstance leftTokenInstance = state.createTokenInstance(range.right, cfId);
            try
            {
                state.process(leftTokenInstance, ConsistencyLevel.SERIAL);
            }
            catch (WriteTimeoutException e)
            {
                // not having a valid token state by the time we finish streaming these
                // will result in the remote token state being created, then set to failure
                // recovery
                logger.debug("Unable to create token state for right token {}", range.right);
            }
        }

        if (getExact(range.left) == null && replicates(range.left))
        {
            logger.debug("Creating token state for left token {}", range.right);
            TokenInstance leftTokenInstance = state.createTokenInstance(range.left, cfId);
            try
            {
                state.process(leftTokenInstance, ConsistencyLevel.SERIAL);
            }
            catch (WriteTimeoutException e)
            {
                // TODO: Make a locally recovering token state and carry on
                throw new IOException(e);
            }
        }

        OutputStream outputStream = new LZFOutputStream(Channels.newOutputStream(channel));
        DataOutputPlus out = new DataOutputStreamPlus(outputStream);

        // TODO: while loop get ranges
        // TODO: handle token states being added while we iterate
        // we iterate over the token states covering the given range from left to right.
        Token last = range.left;

        while (last.compareTo(range.right) < 0)
        {
            // Token state has next
            Token token = getNext(last);
            Range<Token> tokenRange = new Range<Token>(last, token);
            last = token;
            int instancesWritten = 0;


            out.writeBoolean(true);
            Token.serializer.serialize(token, out);

            TokenState tokenState = getExact(token);
            out.writeBoolean(tokenState != null);

            if (tokenState == null)
            {
                // if we don't have a token state for the given token, it means that we weren't
                // able to create it. The to-node will create the token state, but mark it as
                // needing a failure recover operation. This prevents larger availability from
                // interfering with bootstrapping nodes
                //
                // This shouldn't happen for tokens other than the requested ranges right token
                if (!token.equals(range.right))
                {
                    logger.warn("missing token state for mid-range token {}", token);
                }
                else
                {
                    logger.debug("missing token state for right token {}", token);
                }
                continue;
            }
            else
            {
                logger.debug("Streaming out token state {} on {}. Range {}", token, cfId, tokenRange);
            }

            Iterator<CfKey> cfKeyIter = state.keyStateManager.getCfKeyIterator(tokenRange, cfId, CFKEY_ITER_CHUNK);
            tokenState.lockGc();
            try
            {
                long currentEpoch = tokenState.getEpoch();

                // it's possible that the epoch has been incremented, but all of the
                // key states haven't finished gc'ing their instances. Instances from
                // epochs less than this won't be transmitted
                long minEpoch = currentEpoch - 1;

                out.writeLong(currentEpoch);

                while (cfKeyIter.hasNext())
                {
                    CfKey cfKey = cfKeyIter.next();
                    if (!cfKey.cfId.equals(cfId))
                    {
                        continue;
                    }

                    // to minimize the time the key state lock is held, and to prevent
                    // a deadlock when locking instances for transmission, we first get
                    // the ids of all the instances we need to send
                    Map<Long, List<UUID>> executed;
                    Set<UUID> active;
                    state.keyStateManager.getCfKeyLock(cfKey).lock();
                    try
                    {
                        KeyState keyState = state.keyStateManager.loadKeyState(cfKey);
                        executed = keyState.getOrderedEpochExecutions();
                        active = keyState.getActiveInstanceIds();
                    }
                    finally
                    {
                        state.keyStateManager.getCfKeyLock(cfKey).unlock();
                    }

                    out.writeBoolean(true);
                    ByteBufferUtil.writeWithShortLength(cfKey.key, out);

                    // send executed
                    List<Long> epochs = Lists.newArrayList(executed.keySet());
                    Collections.sort(epochs);
                    for (Long epoch: epochs)
                    {
                        if (epoch < minEpoch)
                        {
                            continue;
                        }

                        out.writeLong(epoch);
                        List<UUID> ids = executed.get(epoch);
                        out.writeInt(ids.size());

                        for (UUID id: ids)
                        {
                            Instance instance = state.getInstanceCopy(id);
                            logger.debug("Writing instance {} on token {} for {}", instance.getId(), token, cfId);
                            writeInstance(instance, out, outputStream);
                        }
                    }

                    out.writeLong(-1);
                    out.writeInt(active.size());
                    // send active
                    for (UUID id: active)
                    {
                        Instance instance = state.getInstanceCopy(id);
                        writeInstance(instance, out, outputStream);
                        instancesWritten++;
                    }
                }

                out.writeBoolean(false);
            }
            finally
            {
                tokenState.unlockGc();

                // in case we prevented any from running
                state.startTokenStateGc(tokenState);
            }
            logger.debug("Wrote out {} instances for token {} on {}", instancesWritten, token, cfId);
        }

        // no more outgoing token states
        out.writeBoolean(false);
        outputStream.flush();
    }

    private void writeInstance(Instance instance, DataOutputPlus out, OutputStream outputStream) throws IOException
    {
        long size = Instance.serializer.serializedSize(instance, MessagingService.current_version);
        size += Serializers.uuidSets.serializedSize(instance.getStronglyConnected(), MessagingService.current_version);

        limiter.acquire((int) size);
        bytesSinceFlush += size;
        Instance.serializer.serialize(instance, out , MessagingService.current_version);
        Serializers.uuidSets.serialize(instance.getStronglyConnected(), out, MessagingService.current_version);
        if (bytesSinceFlush > DEFAULT_CHUNK_SIZE)
        {
            outputStream.flush();
            bytesSinceFlush = 0;
        }
    }
}
