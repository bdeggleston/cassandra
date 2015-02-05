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
        this(EpaxosState.instance, cfId, range);
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

    public void read(ReadableByteChannel channel) throws IOException
    {
        TokenState tokenState = getTokenState();
        assert tokenState.getState() != TokenState.State.NORMAL;
        tokenState.lockGc();
        try
        {
            DataInputStream in = new DataInputStream(new LZFInputStream(Channels.newInputStream(channel)));
            long currentEpoch = in.readLong();

            if (currentEpoch <= tokenState.getEpoch())
            {
                logger.info("Remote epoch is <= to the local one. Aborting instance stream");
                return;
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
                    boolean ignoreEpoch = epoch >= minEpoch;
                    if (ignoreEpoch)
                    {
                        logger.debug("Ignoring epoch {}. Min epoch is {}", epoch, minEpoch);
                    }

                    for (int i=0; i<size; i++)
                    {
                        Instance instance = Instance.serializer.deserialize(in, MessagingService.current_version);
                        Set<UUID> scc = Serializers.uuidSets.deserialize(in, MessagingService.current_version);

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
                                if (!last)
                                {
                                    // if this isn't the active set of instances, all these instances should be executed
                                    assert instance.getState() == Instance.State.EXECUTED;
                                    ks.markAcknowledged(instance.getDependencies(), instance.getId());
                                    ks.markExecuted(instance.getId(), scc, epoch, null);

                                    // the keystate needs to be persisted before the instance is, so
                                    // if we need to start over, there aren't a bunch of orphan instances
                                    // sitting around forever
                                    state.keyStateManager.saveKeyState(cfKey, ks);
                                    state.saveInstance(instance);

                                    logger.debug("Adding instance {} to epoch {}", instance.getId(), epoch);
                                }
                                else if (instance.getState().atLeast(Instance.State.ACCEPTED))
                                {
                                    state.addMissingInstance(instance);
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
        }
        finally
        {
            tokenState.unlockGc();
        }

    }
}
