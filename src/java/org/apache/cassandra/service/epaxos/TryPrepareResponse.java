package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

import java.io.DataInput;
import java.io.IOException;

public class TryPrepareResponse
{
    // indicates that the prepare phase was started, or was already
    // in progress when this message was received
    public final boolean inProgress;

    // indicates that the instance will attempt to prepare. Only false
    // if the instance is already committed
    public final boolean willPrepare;

    // the time this instance will wait to prepare. This is only > 0 if
    // this node has seen more recent activity that the requesting node
    public final long timeToPrepare;

    // if the instance was committed, it will return a copy here so the
    // requesting node can record it
    public final Instance committed;

    public TryPrepareResponse(boolean inProgress, boolean willPrepare, long timeToPrepare, Instance committed)
    {
        this.inProgress = inProgress;
        this.willPrepare = willPrepare;
        this.timeToPrepare = timeToPrepare;
        this.committed = committed;
    }

    public static final IVersionedSerializer<TryPrepareResponse> serializer = new IVersionedSerializer<TryPrepareResponse>()
    {
        @Override
        public void serialize(TryPrepareResponse response, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(response.inProgress);
            out.writeBoolean(response.willPrepare);
            out.writeLong(response.timeToPrepare);
            out.writeBoolean(response.committed != null);
            if (response.committed != null)
            {
                Instance.serializer.serialize(response.committed, out, version);
            }
        }

        @Override
        public TryPrepareResponse deserialize(DataInput in, int version) throws IOException
        {
            return new TryPrepareResponse(
                    in.readBoolean(),
                    in.readBoolean(),
                    in.readLong(),
                    in.readBoolean() ? Instance.serializer.deserialize(in, version) : null);
        }

        @Override
        public long serializedSize(TryPrepareResponse response, int version)
        {
            long size = 0;
            size += 1; //response.inProgress
            size += 1; //response.willPrepare
            size += 8; //response.timeToPrepare
            size += 1; //response.committed != null
            if (response.committed != null)
            {
                size += Instance.serializer.serializedSize(response.committed, version); //response.committed
            }
            return size;
        }
    };
}
