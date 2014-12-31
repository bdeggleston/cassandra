package org.apache.cassandra.service.epaxos;

import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;

public class TryPreacceptRequest extends AbstractEpochMessage
{
    public static final IVersionedSerializer<TryPreacceptRequest> serializer = new Serializer();

    public final UUID iid;
    public final Set<UUID> dependencies;

    public TryPreacceptRequest(Token token, UUID cfId, long epoch, UUID iid, Set<UUID> dependencies)
    {
        super(token, cfId, epoch);
        this.iid = iid;
        this.dependencies = dependencies;
    }

    public MessageOut<TryPreacceptRequest> getMessage()
    {
        return new MessageOut<TryPreacceptRequest>(MessagingService.Verb.EPAXOS_TRYPREACCEPT, this, serializer);
    }

    private static class Serializer implements IVersionedSerializer<TryPreacceptRequest>
    {
        @Override
        public void serialize(TryPreacceptRequest request, DataOutputPlus out, int version) throws IOException
        {
            AbstractEpochMessage.serializer.serialize(request, out, version);
            UUIDSerializer.serializer.serialize(request.iid, out, version);
            out.writeInt(request.dependencies.size());
            for (UUID dep : request.dependencies)
                UUIDSerializer.serializer.serialize(dep, out, version);
        }

        @Override
        public TryPreacceptRequest deserialize(DataInput in, int version) throws IOException
        {
            AbstractEpochMessage epochInfo = AbstractEpochMessage.serializer.deserialize(in, version);
            UUID iid = UUIDSerializer.serializer.deserialize(in, version);

            UUID[] deps = new UUID[in.readInt()];
            for (int i=0; i<deps.length; i++)
                deps[i] = UUIDSerializer.serializer.deserialize(in, version);
            return new TryPreacceptRequest(epochInfo.token, epochInfo.cfId, epochInfo.epoch, iid, ImmutableSet.copyOf(deps));
        }

        @Override
        public long serializedSize(TryPreacceptRequest request, int version)
        {
            long size = AbstractEpochMessage.serializer.serializedSize(request, version);

            size += UUIDSerializer.serializer.serializedSize(request.iid, version);

            size += 4;  // deps.size
            for (UUID dep : request.dependencies)
                size += UUIDSerializer.serializer.serializedSize(dep, version);

            return size;
        }
    }
}
