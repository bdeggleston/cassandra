package org.apache.cassandra.service.epaxos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class PreacceptResponse
{
    public static final IVersionedSerializer<PreacceptResponse> serializer = new Serializer();

    public final boolean successful;
    public final int ballotFailure;
    public final Set<UUID> dependencies;
    public volatile List<Instance> missingInstances;

    private static final List<Instance> NO_INSTANCES = ImmutableList.of();
    private static final Set<UUID> NO_DEPS = ImmutableSet.of();

    private PreacceptResponse(boolean successful, int ballotFailure, Set<UUID> dependencies, List<Instance> missingInstances)
    {
        this.successful = successful;
        this.ballotFailure = ballotFailure;
        this.dependencies = dependencies;
        this.missingInstances = missingInstances;
    }

    public static PreacceptResponse success(Instance instance)
    {
        return new PreacceptResponse(instance.getLeaderDepsMatch(), 0, instance.getDependencies(), NO_INSTANCES);
    }

    public static PreacceptResponse failure(Instance instance)
    {
        return new PreacceptResponse(false, 0, instance.getDependencies(), NO_INSTANCES);
    }

    public static PreacceptResponse ballotFailure(int localBallot)
    {
        return new PreacceptResponse(false, localBallot, NO_DEPS, NO_INSTANCES);
    }

    public static class Serializer implements IVersionedSerializer<PreacceptResponse>
    {
        @Override
        public void serialize(PreacceptResponse response, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(response.successful);
            out.writeInt(response.ballotFailure);

            Set<UUID> deps = response.dependencies;
            out.writeInt(deps.size());
            for (UUID dep : deps)
                UUIDSerializer.serializer.serialize(dep, out, version);

            out.writeInt(response.missingInstances.size());
            for (Instance instance: response.missingInstances)
                Instance.serializer.serialize(instance, out, version);
        }

        @Override
        public PreacceptResponse deserialize(DataInput in, int version) throws IOException
        {
            boolean successful = in.readBoolean();
            int ballotFailure = in.readInt();

            UUID[] deps = new UUID[in.readInt()];
            for (int i=0; i<deps.length; i++)
                deps[i] = UUIDSerializer.serializer.deserialize(in, version);

            Instance[] missing = new Instance[in.readInt()];
            for (int i=0; i<missing.length; i++)
                missing[i] = Instance.serializer.deserialize(in, version);

            return new PreacceptResponse(successful, ballotFailure, ImmutableSet.copyOf(deps), Lists.newArrayList(missing));
        }

        @Override
        public long serializedSize(PreacceptResponse response, int version)
        {
            int size = 0;
            size += 1;  //out.writeBoolean(response.successful);
            size += 4;  //out.writeInt(response.ballotFailure);

            size += 4;  //out.writeInt(deps.size());
            for (UUID dep : response.dependencies)
                size += UUIDSerializer.serializer.serializedSize(dep, version);

            size += 4;  //out.writeInt(response.missingInstances.size());
            for (Instance instance: response.missingInstances)
                size += Instance.serializer.serializedSize(instance, version);
            return size;
        }
    }
}
