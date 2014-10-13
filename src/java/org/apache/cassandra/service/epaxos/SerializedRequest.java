package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.CASRequest;
import org.apache.cassandra.service.ThriftCASRequest;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class SerializedRequest
{
    public static final IVersionedSerializer<SerializedRequest> serializer = new Serializer();

    private final String keyspaceName;
    private final String cfName;
    private final ThriftCASRequest request;
    private final ByteBuffer key;
    private final ConsistencyLevel consistencyLevel;

    private SerializedRequest(Builder builder)
    {
        keyspaceName = builder.keyspaceName;
        cfName = builder.cfName;
        request = builder.casRequest;
        key = builder.key;
        consistencyLevel = builder.consistencyLevel;
    }

    public String getKeyspaceName()
    {
        return keyspaceName;
    }

    public String getCfName()
    {
        return cfName;
    }

    public ThriftCASRequest getRequest()
    {
        return request;
    }

    public ByteBuffer getKey()
    {
        return key;
    }

    public ConsistencyLevel getConsistencyLevel()
    {
        return consistencyLevel;
    }

    public static class Serializer implements IVersionedSerializer<SerializedRequest>
    {
        @Override
        public void serialize(SerializedRequest request, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(request.keyspaceName);
            out.writeUTF(request.cfName);
            ByteBufferUtil.writeWithShortLength(request.key, out);
            out.writeShort(request.consistencyLevel.code);
            ThriftCASRequest.serializer.serialize(request.request, out, version);
        }

        @Override
        public SerializedRequest deserialize(DataInput in, int version) throws IOException
        {
            Builder builder = builder();

            builder.keyspaceName(in.readUTF());
            builder.cfName(in.readUTF());
            builder.key(ByteBufferUtil.readWithShortLength(in));
            builder.consistencyLevel(ConsistencyLevel.fromCode(in.readShort()));
            builder.casRequest(ThriftCASRequest.serializer.deserialize(in, version));

            return builder.build();
        }

        @Override
        public long serializedSize(SerializedRequest request, int version)
        {
            long size = 0;
            size += TypeSizes.NATIVE.sizeof(request.keyspaceName);
            size += TypeSizes.NATIVE.sizeof(request.cfName);
            size += TypeSizes.NATIVE.sizeofWithShortLength(request.key);
            size += 2;
            size += ThriftCASRequest.serializer.serializedSize(request.request, version);
            return size;
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String keyspaceName;
        private String cfName;
        private boolean isReadOnly;
        private ByteBuffer key;
        private ConsistencyLevel consistencyLevel;
        private ThriftCASRequest casRequest;
        private List<ReadCommand> readCommands;

        public Builder keyspaceName(String keyspaceName)
        {
            this.keyspaceName = keyspaceName;
            return this;
        }

        public Builder cfName(String cfName)
        {
            this.cfName = cfName;
            return this;
        }

        public Builder key(ByteBuffer key)
        {
            this.key = key;
            return this;
        }

        public Builder consistencyLevel(ConsistencyLevel cl)
        {
            if (!cl.isSerialConsistency())
                throw new IllegalArgumentException("Consistency level must be SERIAL or LOCAL_SERIAL");
            this.consistencyLevel = cl;
            return this;
        }

        public Builder casRequest(ThriftCASRequest request)
        {
            this.casRequest = request;
            return this;
        }

        public Builder readCommands(List<ReadCommand> readCommands)
        {
            this.readCommands = readCommands;
            return this;
        }

        public SerializedRequest build()
        {
            return new SerializedRequest(this);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SerializedRequest that = (SerializedRequest) o;

        if (!cfName.equals(that.cfName)) return false;
        if (consistencyLevel != that.consistencyLevel) return false;
        if (!key.equals(that.key)) return false;
        if (!keyspaceName.equals(that.keyspaceName)) return false;
        if (!request.equals(that.request)) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = keyspaceName.hashCode();
        result = 31 * result + cfName.hashCode();
        result = 31 * result + request.hashCode();
        result = 31 * result + key.hashCode();
        result = 31 * result + consistencyLevel.hashCode();
        return result;
    }
}
