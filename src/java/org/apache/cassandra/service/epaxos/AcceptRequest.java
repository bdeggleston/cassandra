package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

public class AcceptRequest
{
    public static final IVersionedSerializer<AcceptRequest> serializer = new Serializer();

    public final Instance instance;
    public final List<Instance> missingInstances;

    public AcceptRequest(Instance instance, List<Instance> missingInstances)
    {
        this.instance = instance;
        this.missingInstances = missingInstances;
    }

    private static class Serializer implements IVersionedSerializer<AcceptRequest>
    {
        @Override
        public void serialize(AcceptRequest request, DataOutputPlus out, int version) throws IOException
        {
            Instance.serializer.serialize(request.instance, out, version);
            out.writeInt(request.missingInstances.size());
            for (Instance missing: request.missingInstances)
                Instance.serializer.serialize(missing, out, version);
        }

        @Override
        public AcceptRequest deserialize(DataInput in, int version) throws IOException
        {
            Instance instance = Instance.serializer.deserialize(in, version);
            int numMissing = in.readInt();
            List<Instance> missingInstances = Lists.newArrayListWithCapacity(numMissing);
            for (int i=0; i<numMissing; i++)
                missingInstances.add(Instance.serializer.deserialize(in, version));
            return new AcceptRequest(instance, missingInstances);
        }

        @Override
        public long serializedSize(AcceptRequest request, int version)
        {
            long size = Instance.serializer.serializedSize(request.instance, version);
            size += 4;
            for (Instance missing: request.missingInstances)
                size += Instance.serializer.serializedSize(missing, version);

            return size;
        }
    }
}
