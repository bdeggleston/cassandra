package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.ning.compress.lzf.LZFOutputStream;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.*;

public class InstanceStreamWriter
{
    private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;

    private final EpaxosState state;
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
        this.cfId = cfId;
        this.range = range;
        limiter = StreamManager.getRateLimiter(peer);
    }

    protected TokenState getTokenState()
    {
        return state.tokenStateManager.get(range.left, cfId);
    }

    /**
     * a true is written to the channel before any instances for a key is sent, and a false
     * is sent after. This let's the receiver know that we're not sending any more instances
     */
    public void write(WritableByteChannel channel) throws IOException
    {
        TokenState tokenState = getTokenState();
        Iterator<CfKey> cfKeyIter = state.keyStateManager.getCfKeyIterator(tokenState);
        OutputStream outputStream = new LZFOutputStream(Channels.newOutputStream(channel));
        DataOutputPlus out = new DataOutputStreamPlus(outputStream);

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
                ByteBufferUtil.writeWithLength(cfKey.key, out);

                // send executed
                List<Long> epochs = Lists.newArrayList();
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
                }
            }

            out.writeBoolean(false);
            outputStream.flush();
        }
        finally
        {
            tokenState.unlockGc();

            // in case we prevented any from running
            state.startTokenStateGc(tokenState);
        }

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
