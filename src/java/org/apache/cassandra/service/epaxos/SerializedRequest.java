package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.CASRequest;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public abstract class SerializedRequest
{
    public static enum Type
    {
        CAS(1),
        READ(2);

        public final int number;

        Type(int number)
        {
            this.number = number;
        }
    }

    private final String keyspaceName;
    private final String cfName;
    private final ByteBuffer key;
    private final ConsistencyLevel consistencyLevel;

    private SerializedRequest(Builder builder)
    {
        keyspaceName = builder.keyspaceName;
        cfName = builder.cfName;
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

    public abstract boolean isReadOnly();

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
        public void serialize(SerializedRequest serializedRequest, DataOutputPlus out, int version) throws IOException
        {

        }

        @Override
        public SerializedRequest deserialize(DataInput in, int version) throws IOException
        {
            return null;
        }

        @Override
        public long serializedSize(SerializedRequest serializedRequest, int version)
        {
            return 0;
        }
    }

    public static class SerializedRead extends SerializedRequest
    {
        private final List<ReadCommand> readCommands;

        public SerializedRead(Builder builder)
        {
            super(builder);
            assert builder.casRequest == null;
            this.readCommands = builder.readCommands;
        }

        @Override
        public boolean isReadOnly()
        {
            return true;
        }

        public List<ReadCommand> getReadCommands()
        {
            return readCommands;
        }
    }

    public static class SerializedCAS extends SerializedRequest
    {
        public static enum Type
        {
            CQL(1),
            THRIFT(2);

            public final int number;

            Type(int number)
            {
                this.number = number;
            }
        }

        private final CASRequest casRequest;

        public SerializedCAS(Builder builder)
        {
            super(builder);
            assert builder.readCommands == null;
            this.casRequest = builder.casRequest;
        }

        @Override
        public boolean isReadOnly()
        {
            return false;
        }

        public CASRequest getCasRequest()
        {
            return casRequest;
        }
    }

    public static class Builder
    {
        private String keyspaceName;
        private String cfName;
        private boolean isReadOnly;
        private ByteBuffer key;
        private ConsistencyLevel consistencyLevel;
        private CASRequest casRequest;
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

        public Builder setCASRequest(CASRequest request)
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
            if (casRequest != null && readCommands != null)
            {
                throw new IllegalStateException("SerializedQuery instances cannot have both CASRequest and ReadCommands");
            }
            if (casRequest != null)
            {
                return new SerializedCAS(this);
            }
            else if (readCommands != null)
            {
                return new SerializedRead(this);
            }
            else
            {
                throw new IllegalStateException("SerializedQuery instances must have either a CASRequest or ReadCommands");
            }
        }
    }

}
