package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.UUID;

public class FailureRecoveryRequest
{
    public final Token token;
    public final UUID cfId;
    public final long epoch;

    public FailureRecoveryRequest(Token token, UUID cfId, long epoch)
    {
        this.token = token;
        this.cfId = cfId;
        this.epoch = epoch;
    }

    public static final IVersionedSerializer<FailureRecoveryRequest> serializer = new IVersionedSerializer<FailureRecoveryRequest>()
    {
        @Override
        public void serialize(FailureRecoveryRequest request, DataOutputPlus out, int version) throws IOException
        {
            Token.serializer.serialize(request.token, out);
            UUIDSerializer.serializer.serialize(request.cfId, out, version);
            out.writeLong(request.epoch);
        }

        @Override
        public FailureRecoveryRequest deserialize(DataInput in, int version) throws IOException
        {
            return new FailureRecoveryRequest(Token.serializer.deserialize(in),
                                              UUIDSerializer.serializer.deserialize(in, version),
                                              in.readLong());
        }

        @Override
        public long serializedSize(FailureRecoveryRequest request, int version)
        {
            return Token.serializer.serializedSize(request.token, TypeSizes.NATIVE)
                    + UUIDSerializer.serializer.serializedSize(request.cfId, version)
                    + 8;
        }
    };
}
