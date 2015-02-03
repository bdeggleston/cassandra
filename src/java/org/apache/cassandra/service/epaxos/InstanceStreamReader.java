package org.apache.cassandra.service.epaxos;

import com.ning.compress.lzf.LZFInputStream;
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
    private final TokenState tokenState;
    private final UUID cfId;

    public InstanceStreamReader(EpaxosState state, TokenState tokenState, UUID cfId)
    {
        this.state = state;
        this.tokenState = tokenState;
        this.cfId = cfId;
    }

    public void read(ReadableByteChannel channel) throws IOException
    {
        // TODO: this shouldn't run concurrently, but it shouldn't need to. we should only need data from a single node, assuming it's not also behind

        // TODO: how will these conflict with the live instances coming in?
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

                state.keyStateManager.getCfKeyLock(cfKey).lock();
                try
                {
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

                        for (int i=0; i<size; i++)
                        {
                            Instance instance = Instance.serializer.deserialize(in, MessagingService.current_version);
                            Set<UUID> scc = Serializers.uuidSets.deserialize(in, MessagingService.current_version);

                            Lock lock = state.getInstanceLock(instance.getId()).writeLock();
                            lock.lock();
                            try
                            {
                                if (!last)
                                {
                                    // if this isn't the active set of instances, all these instances should be executed
                                    assert instance.getState() == Instance.State.EXECUTED;
                                    state.saveInstance(instance);
                                    ks.markAcknowledged(instance.getDependencies(), instance.getId());
                                    ks.markExecuted(instance.getId(), scc, epoch, null);

                                }
                                else if (instance.getState().atLeast(Instance.State.ACCEPTED))
                                {
                                    state.addMissingInstance(instance);
                                }
                            }
                            finally
                            {
                                lock.unlock();
                            }
                        }
                    }
                }
                finally
                {
                    state.keyStateManager.getCfKeyLock(cfKey).unlock();
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
