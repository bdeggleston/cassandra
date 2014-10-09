package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.CASRequest;
import org.apache.cassandra.service.ThriftCASRequest;

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
            out.write(request.key);
            out.writeInt(request.consistencyLevel.code);
            ThriftCASRequest.serializer.serialize(request.request, out, version);
        }

        @Override
        public SerializedRequest deserialize(DataInput in, int version) throws IOException
        {
            return null;
        }

        @Override
        public long serializedSize(SerializedRequest request, int version)
        {
            return 0;
        }
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

        public Builder setKeyspaceName(String keyspaceName)
        {
            this.keyspaceName = keyspaceName;
            return this;
        }

        public Builder setCfName(String cfName)
        {
            this.cfName = cfName;
            return this;
        }

        public Builder setKey(ByteBuffer key)
        {
            this.key = key;
            return this;
        }

        public Builder setConsistencyLevel(ConsistencyLevel consistencyLevel)
        {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        public Builder setCASRequest(ThriftCASRequest request)
        {
            this.casRequest = request;
            return this;
        }

        public Builder setReadCommands(List<ReadCommand> readCommands)
        {
            this.readCommands = readCommands;
            return this;
        }

        public SerializedRequest build()
        {
            return new SerializedRequest(this);
        }
    }

}
