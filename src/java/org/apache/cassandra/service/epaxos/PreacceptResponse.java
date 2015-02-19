package org.apache.cassandra.service.epaxos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class PreacceptResponse extends AbstractEpochMessage
{
    public static final IVersionedSerializer<PreacceptResponse> serializer = new Serializer();

    public final boolean success;
    public final int ballotFailure;
    public final Set<UUID> dependencies;
    public final boolean vetoed;
    public volatile List<Instance> missingInstances;

    private static final List<Instance> NO_INSTANCES = ImmutableList.of();
    private static final Set<UUID> NO_DEPS = ImmutableSet.of();

    public PreacceptResponse(Token token,
                             UUID cfId,
                             long epoch,
                             boolean success,
                             int ballotFailure,
                             Set<UUID> dependencies,
                             boolean vetoed,
                             List<Instance> missingInstances)
    {
        super(token, cfId, epoch);
        this.success = success;
        this.ballotFailure = ballotFailure;
        this.dependencies = dependencies;
        this.vetoed = vetoed;
        this.missingInstances = missingInstances;
    }

    private static boolean getVetoed(Instance instance)
    {
        if (instance instanceof EpochInstance)
        {
            return ((EpochInstance) instance).isVetoed();
        }
        return false;
    }

    public static PreacceptResponse success(Token token, long epoch, Instance instance)
    {
        return new PreacceptResponse(token, instance.getCfId(), epoch, instance.getLeaderAttrsMatch(), 0, instance.getDependencies(), getVetoed(instance), NO_INSTANCES);
    }

    public static PreacceptResponse failure(Token token, long epoch, Instance instance)
    {
        return new PreacceptResponse(token, instance.getCfId(), epoch, false, 0, instance.getDependencies(), getVetoed(instance), NO_INSTANCES);
    }

    public static PreacceptResponse ballotFailure(Token token, UUID cfId, long epoch, int localBallot)
    {
        return new PreacceptResponse(token, cfId, epoch, false, localBallot, NO_DEPS, false, NO_INSTANCES);
    }

    @Override
    public String toString()
    {
        return "PreacceptResponse{" +
                "success=" + success +
                ", ballotFailure=" + ballotFailure +
                ", dependencies=" + dependencies +
                ", vetoed=" + vetoed +
                ", missingInstances=" + missingInstances +
                '}';
    }

    public static class Serializer implements IVersionedSerializer<PreacceptResponse>
    {
        @Override
        public void serialize(PreacceptResponse response, DataOutputPlus out, int version) throws IOException
        {
            Token.serializer.serialize(response.token, out);
            UUIDSerializer.serializer.serialize(response.cfId, out, version);
            out.writeLong(response.epoch);

            out.writeBoolean(response.success);
            out.writeInt(response.ballotFailure);

            Set<UUID> deps = response.dependencies;
            out.writeInt(deps.size());
            for (UUID dep : deps)
            {
                UUIDSerializer.serializer.serialize(dep, out, version);
            }

            out.writeBoolean(response.vetoed);

            out.writeInt(response.missingInstances.size());
            for (Instance instance: response.missingInstances)
            {
                Instance.serializer.serialize(instance, out, version);
            }
        }

        @Override
        public PreacceptResponse deserialize(DataInput in, int version) throws IOException
        {
            Token token = Token.serializer.deserialize(in);
            UUID cfId = UUIDSerializer.serializer.deserialize(in, version);
            long epoch = in.readLong();

            boolean successful = in.readBoolean();
            int ballotFailure = in.readInt();

            UUID[] deps = new UUID[in.readInt()];
            for (int i=0; i<deps.length; i++)
            {
                deps[i] = UUIDSerializer.serializer.deserialize(in, version);
            }

            boolean vetoed = in.readBoolean();

            Instance[] missing = new Instance[in.readInt()];
            for (int i=0; i<missing.length; i++)
            {
                missing[i] = Instance.serializer.deserialize(in, version);
            }

            return new PreacceptResponse(token,
                                         cfId,
                                         epoch,
                                         successful,
                                         ballotFailure,
                                         ImmutableSet.copyOf(deps),
                                         vetoed,
                                         Lists.newArrayList(missing));
        }

        @Override
        public long serializedSize(PreacceptResponse response, int version)
        {
            long size = Token.serializer.serializedSize(response.token, TypeSizes.NATIVE);
            size += UUIDSerializer.serializer.serializedSize(response.cfId, version);
            size += 8;  // response.epoch

            size += 1;  //out.writeBoolean(response.success);
            size += 4;  //out.writeInt(response.ballotFailure);

            size += 4;  //out.writeInt(deps.size());
            for (UUID dep : response.dependencies)
            {
                size += UUIDSerializer.serializer.serializedSize(dep, version);
            }

            size += 1;  //out.writeBoolean(response.vetoed);

            size += 4;  //out.writeInt(response.missingInstances.size());
            for (Instance instance: response.missingInstances)
            {
                size += Instance.serializer.serializedSize(instance, version);
            }
            return size;
        }
    }
}
