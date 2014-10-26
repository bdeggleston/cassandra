package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.concurrent.Stage;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * A group of prepare phases that an instance execution is waiting on
 */
public class PrepareGroup implements ICommitCallback
{
    private final EpaxosState state;
    private final UUID id;
    private final Set<UUID> uncommitted;
    private final Set<UUID> outstanding;
    private boolean completed = false;

    public PrepareGroup(EpaxosState state, UUID id, Set<UUID> uncommitted)
    {
        this.state = state;
        this.id = id;
        this.uncommitted = uncommitted;
        this.outstanding = new HashSet<>(this.uncommitted.size());
    }

    public synchronized void schedule()
    {
        for (UUID toPrepare: uncommitted)
        {
            outstanding.add(toPrepare);
            state.prepare(toPrepare, this);
        }
    }

    @Override
    public synchronized void instanceCommitted(UUID committed)
    {
        outstanding.remove(committed);
        if (outstanding.size() == 0 && !completed)
        {
            completed = true;
            state.getStage(Stage.MUTATION).execute(new ExecuteTask(state, id));
        }
    }

    public int size()
    {
        return uncommitted.size();
    }
}
