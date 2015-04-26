package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageOut;

import java.net.InetAddress;
import java.util.*;

public class MockVerbHandlerState extends EpaxosState
{

    @Override
    protected TokenStateManager createTokenStateManager()
    {
        return new MockTokenStateManager();
    }

    @Override
    public void preacceptPrepare(UUID id, boolean noop, Runnable failureCallback)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void accept(UUID iid, AcceptDecision decision, Runnable failureCallback)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit(UUID iid, Set<UUID> dependencies)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrepareTask prepare(UUID id, PrepareGroup group)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void tryPreaccept(UUID iid, List<TryPreacceptAttempt> attempts, ParticipantInfo participantInfo, Runnable failureCallback)
    {
        throw new UnsupportedOperationException();
    }

    public final Set<UUID> commitNotified = Sets.newHashSet();

    @Override
    public void notifyCommit(UUID id)
    {
        commitNotified.add(id);
    }

    public final Set<UUID> executed = Sets.newHashSet();

    @Override
    public void execute(UUID instanceId)
    {
        executed.add(instanceId);
    }

    public final Set<UUID> currentDeps = Sets.newHashSet();

    public final Set<UUID> getCurrentDeps = Sets.newHashSet();

    @Override
    public Set<UUID> getCurrentDependencies(Instance instance)
    {
        getCurrentDeps.add(instance.getId());
        return Sets.newHashSet(currentDeps);
    }

    public final Map<UUID, Instance> instanceMap = Maps.newHashMap();

    @Override
    protected Instance loadInstance(UUID instanceId)
    {
        return instanceMap.get(instanceId);
    }

    public final Map<UUID, Instance> savedInstances = Maps.newHashMap();

    @Override
    protected void saveInstance(Instance instance)
    {
        savedInstances.put(instance.getId(), instance);
    }

    public final Set<UUID> missingRecoreded = Sets.newHashSet();

    @Override
    public void recordMissingInstance(Instance instance)
    {
        missingRecoreded.add(instance.getId());
        super.recordMissingInstance(instance);
    }

    public final Set<UUID> acknowledgedRecoreded = Sets.newHashSet();

    @Override
    public void recordAcknowledgedDeps(Instance instance)
    {
        acknowledgedRecoreded.add(instance.getId());
    }

    public final Set<UUID> executedRecorded = Sets.newHashSet();

    @Override
    public void recordExecuted(Instance instance, ReplayPosition position, long maxTimestamp)
    {
        executedRecorded.add(instance.getId());
    }

    public final Set<UUID> missingAdded = Sets.newHashSet();

    @Override
    protected void addMissingInstance(Instance remoteInstance)
    {
        missingAdded.add(remoteInstance.getId());
    }

    List<MessageOut> replies = new LinkedList<>();

    protected void sendReply(MessageOut message, int id, InetAddress to)
    {
        replies.add(message);
    }

    @Override
    protected int sendRR(MessageOut message, InetAddress to, IAsyncCallback cb)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void sendOneWay(MessageOut message, InetAddress to)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void scheduleTokenStateMaintenanceTask()
    {
        // no-op
    }
}
