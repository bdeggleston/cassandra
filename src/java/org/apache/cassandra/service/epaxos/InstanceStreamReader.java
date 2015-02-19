package org.apache.cassandra.service.epaxos;

import com.ning.compress.lzf.LZFInputStream;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

public class InstanceStreamReader
{
    private static final Logger logger = LoggerFactory.getLogger(InstanceStreamReader.class);

    private final EpaxosState state;
    private final UUID cfId;
    private final Range<Token> range;

    public InstanceStreamReader(UUID cfId, Range<Token> range)
    {
        this(EpaxosState.getInstance(), cfId, range);
    }

    public InstanceStreamReader(EpaxosState state, UUID cfId, Range<Token> range)
    {
        this.state = state;
        this.cfId = cfId;
        this.range = range;
    }

    protected TokenState getTokenState()
    {
        return state.tokenStateManager.get(range.left, cfId);
    }

    protected TokenState getExact(Token token)
    {
        return state.tokenStateManager.getExact(token, cfId);
    }

    protected TokenState getOrCreate(Token token)
    {
        TokenState ts = getExact(token);
        if (ts == null)
        {
            ts = state.tokenStateManager.putState(new TokenState(token, cfId, 0, 0, 0, TokenState.State.RECOVERY_REQUIRED));
        }
        return ts;
    }

    private void drainInstanceStream(DataInputStream in) throws IOException
    {
        while (in.readBoolean())
        {
            boolean last = false;
            while (!last)
            {
                in.readLong();  // epoch
                in.readInt();  // executions
                Instance.serializer.deserialize(in, MessagingService.current_version);
                Serializers.uuidSets.deserialize(in, MessagingService.current_version);
            }
        }
    }

    public synchronized void read(ReadableByteChannel channel) throws IOException
    {
        DataInputStream in = new DataInputStream(new LZFInputStream(Channels.newInputStream(channel)));

        // TODO: think through concurrency problems with this
        while (in.readBoolean())
        {
            Token token = Token.serializer.deserialize(in);
            int instancesRead = 0;

            boolean createdNew = false;
            TokenState tokenState = getExact(token);
            if (tokenState == null)
            {
                tokenState = new TokenState(token, cfId, 0, 0, 0, TokenState.State.RECOVERY_REQUIRED);
                TokenState previous = state.tokenStateManager.putState(tokenState);
                if (previous == tokenState)
                {
                    createdNew = true;
                }
                else
                {
                    tokenState = previous;
                }
            }
            // TODO: work out which state we should handle in which ways
//            assert tokenState.getState() != TokenState.State.NORMAL;

            logger.info("Streaming in token state for {} on {}", token, cfId);

            // TODO: check that the token state locking/saving/state changes work with all instance stream applications
            tokenState.lockGc();
            try
            {
                if (!in.readBoolean())
                {
                    logger.info("Token state doesn't exist for {} on {}", token, cfId);
                    continue;
                }

                long currentEpoch = in.readLong();

                boolean ignore = !createdNew && currentEpoch <= tokenState.getEpoch();
                if (ignore)
                {
                    logger.info("Remote epoch {} is <= to the local one {}. Ignoring instance stream for this token", currentEpoch, tokenState.getEpoch());
                    drainInstanceStream(in);
                    continue;
                }

                long minEpoch = currentEpoch - 1;

                while (in.readBoolean())
                {
                    ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                    CfKey cfKey = new CfKey(key, cfId);
                    // this will instantiate the key state (if it doesn't already exist) in the epoch
                    // of the token state manager
                    KeyState ks = state.keyStateManager.loadKeyState(cfKey);

                    boolean last = false;
                    while (!last)
                    {
                        long epoch = in.readLong();
                        last = epoch < 0;
                        int size = in.readInt();
                        ks.setEpoch(last ? currentEpoch : epoch);
                        boolean ignoreEpoch = epoch < minEpoch && !last;
                        if (ignoreEpoch)
                        {
                            logger.debug("Ignoring epoch {}. Min epoch is {}", epoch, minEpoch);
                        }

                        for (int i=0; i<size; i++)
                        {
                            Instance instance = Instance.serializer.deserialize(in, MessagingService.current_version);
                            Set<UUID> stronglyConnected = Serializers.uuidSets.deserialize(in, MessagingService.current_version);
                            logger.debug("Reading instance {} on token {} for {}", instance.getId(), token, cfId);
                            instancesRead++;

                            if (ignoreEpoch)
                                continue;

                            Lock instanceLock = state.getInstanceLock(instance.getId()).writeLock();
                            Lock ksLock = state.keyStateManager.getCfKeyLock(cfKey);
                            instanceLock.lock();
                            ksLock.lock();
                            try
                            {
                                // reload the key state in case there are other threads receiving instances
                                // it should be cached anyway
                                ks = state.keyStateManager.loadKeyState(cfKey);
                                // don't add the same instance multiple times
                                if (!ks.contains(instance.getId()))
                                {

                                    // TODO: if last, add as per usual, including remoteCommit
                                    // TODO: previous should all be considered ack'd?

                                    // TODO: do token and epoch instances require and special handling? previous epochs should be transmitted
                                    if (instance.getState().atLeast(Instance.State.ACCEPTED))
                                    {
                                        if (!last && instance.getState() != Instance.State.EXECUTED)
                                        {
                                            logger.warn("Got non-executed instance from previous epoch: {}", instance);
                                        }

//                                        state.keyStateManager.recordMissingInstance(instance);
                                        ks.recordInstance(instance.getId());
                                        ks.markAcknowledged(instance.getDependencies(), instance.getId());
                                        if (instance.getState() == Instance.State.EXECUTED)
                                        {
                                            // since instance streams are expected to be accompanied by data streams
                                            // we do not call commitRemote on the instances
                                            if (stronglyConnected != null && stronglyConnected.size() > 1)
                                            {
                                                instance.setStronglyConnected(stronglyConnected);
                                            }
                                            ks.markExecuted(instance.getId(), stronglyConnected, null);
                                        }
                                        state.keyStateManager.saveKeyState(cfKey, ks);
                                        // the instance is persisted after the keystate is, so if this bootstrap/recovery
                                        // fails, and another failure recovery starts, we know to delete the instance
                                        state.saveInstance(instance);

                                    }
                                    else
                                    {
                                        if (!last)
                                        {
                                            logger.warn("Got non-accepted instance from previous epoch: {}", instance);
                                        }
                                    }
                                }
                                else
                                {
                                    logger.debug("Skipping adding already recorded instance: {}", instance.getId());
                                }
                            }
                            finally
                            {
                                ksLock.unlock();
                                instanceLock.unlock();
                            }
                        }
                    }
                }
                tokenState.setEpoch(currentEpoch);
                tokenState.rwLock.writeLock().lock();
                try
                {
                    if (tokenState.getState() == TokenState.State.RECOVERY_REQUIRED)
                    {
                        tokenState.setState(TokenState.State.NORMAL);
                    }
                    else
                    {
                        throw new AssertionError("make this work with failure recovery, and existing token range recovery");
                    }
                    state.tokenStateManager.save(tokenState);

                }
                finally
                {
                    tokenState.rwLock.writeLock().unlock();
                }
            }
            finally
            {
                tokenState.unlockGc();
            }

            logger.debug("Read in {} instances for token {} on {}", instancesRead, token, cfId);
            state.tokenStateManager.save(tokenState);
        }
    }
}
