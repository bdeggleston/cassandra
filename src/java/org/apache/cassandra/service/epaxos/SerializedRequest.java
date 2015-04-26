package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.CASRequest;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class SerializedRequest
{
    private static final Logger logger = LoggerFactory.getLogger(SerializedRequest.class);

    public static final IVersionedSerializer<SerializedRequest> serializer = new Serializer();

    private final String keyspaceName;
    private final String cfName;
    private final CASRequest request;
    private final ByteBuffer key;
    private final ConsistencyLevel consistencyLevel;

    public static class ExecutionMetaData
    {
        public final ColumnFamily cf;
        public final ReplayPosition replayPosition;
        public final long maxTimestamp;

        public ExecutionMetaData(ColumnFamily cf, ReplayPosition replayPosition, long maxTimestamp)
        {
            this.cf = cf;
            this.replayPosition = replayPosition;
            this.maxTimestamp = maxTimestamp;
        }
    }


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

    public CASRequest getRequest()
    {
        return request;
    }

    public ByteBuffer getKey()
    {
        return key;
    }

    public CfKey getCfKey()
    {
        return new CfKey(key, keyspaceName, cfName);
    }

    public ConsistencyLevel getConsistencyLevel()
    {
        return consistencyLevel;
    }

    /**
     * increases the timestamps in the given column family to be at least
     * the min timestamp given
     */
    static long applyMinTimestamp(ColumnFamily cf, long minTs)
    {
        long delta = minTs - cf.minTimestamp();

        if (delta > 0)
        {
            for (Cell cell: cf)
            {
                cf.addColumn(cell.withUpdatedTimestamp(cell.timestamp() + delta));
            }

            cf.deletionInfo().applyTimestampDelta(delta);
        }

        return cf.maxTimestamp();
    }

    public ExecutionMetaData execute(long minTimestamp) throws ReadTimeoutException, WriteTimeoutException
    {
        Tracing.trace("Reading existing values for CAS precondition");
        CFMetaData metadata = Schema.instance.getCFMetaData(keyspaceName, cfName);

        // the read timestamp needs to be the same across nodes
        ReadCommand command = ReadCommand.create(keyspaceName, key, cfName, minTimestamp, request.readFilter());

        Keyspace keyspace = Keyspace.open(command.ksName);
        Row row = command.getRow(keyspace);

        ColumnFamily current = row.cf;

        boolean applies;
        try
        {
            applies = request.appliesTo(current);
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e);
        }

        if (!applies)
        {
            Tracing.trace("CAS precondition does not match current values {}", current);
            logger.debug("CAS precondition does not match current values {}", current);
            // We should not return null as this means success
            ColumnFamily rCF = current == null ? ArrayBackedSortedColumns.factory.create(metadata) : current;
            return new ExecutionMetaData(rCF, null, 0);
        }
        else
        {
            ReplayPosition rp;
            try
            {
                ColumnFamily cf = request.makeUpdates(current);
                long maxTimestamp = applyMinTimestamp(cf, minTimestamp);
                Mutation mutation = new Mutation(key, cf);
                rp = Keyspace.open(mutation.getKeyspaceName()).apply(mutation, true);
                logger.debug("Applying mutation {} at {}", mutation, current);
                return new ExecutionMetaData(null, rp, maxTimestamp);
            }
            catch (InvalidRequestException e)
            {
                throw new RuntimeException(e);
            }
        }
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
            CASRequest.serializer.serialize(request.request, out, version);
        }

        @Override
        public SerializedRequest deserialize(DataInput in, int version) throws IOException
        {
            Builder builder = builder();

            builder.keyspaceName(in.readUTF());
            builder.cfName(in.readUTF());
            builder.key(ByteBufferUtil.readWithShortLength(in));
            builder.consistencyLevel(ConsistencyLevel.fromCode(in.readShort()));
            builder.casRequest(CASRequest.serializer.deserialize(in, version));
            builder.isRemoteQuery();

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
            size += CASRequest.serializer.serializedSize(request.request, version);
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
        private CASRequest casRequest;
        private List<ReadCommand> readCommands;
        private boolean isRemote;

        // indicates that a future doesn't need to be created
        private Builder isRemoteQuery()
        {
            isRemote = true;
            return this;
        }

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

        public Builder casRequest(CASRequest request)
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
