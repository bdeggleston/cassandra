package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.utils.Pair;

import java.util.*;

/**
 * A group of prepare phases that an instance execution is waiting on
 */
public class PrepareGroup implements ICommitCallback
{
    private final EpaxosState state;
    private final UUID id;
    private final Set<UUID> uncommitted;
    private final Set<UUID> outstanding;
    private final List<Pair<Stage, Runnable>> postComplete = new LinkedList<>();
    private boolean completed = false;

    public PrepareGroup(EpaxosState state, UUID id, Set<UUID> uncommitted)
    {
        this.state = state;
        this.id = id;
        this.uncommitted = uncommitted;
        this.outstanding = new HashSet<>(this.uncommitted);
    }

    public synchronized void schedule()
    {
        for (UUID toPrepare: uncommitted)
        {
            state.registerCommitCallback(toPrepare, this);
        }

        for (UUID toPrepare: uncommitted)
        {
            state.prepare(toPrepare, this);
        }
    }

    public synchronized void prepareComplete(UUID completedId)
    {
        outstanding.remove(completedId);
        if (outstanding.size() == 0)
        {
            for (UUID toPrepare: uncommitted)
                state.unregisterPrepareGroup(toPrepare);
            completed = true;

            state.getStage(Stage.MUTATION).submit(new ExecuteTask(state, id));

            // FIXME: this sucks
            for (Pair<Stage, Runnable> pair: postComplete)
            {
                if (pair.left != null)
                {
                    state.getStage(pair.left).submit(pair.right);
                }
                else
                {
                    pair.right.run();
                }
            }
        }
    }

    /**
     *
     * @param runnable
     * @return
     */
    // FIXME: this sucks
    public synchronized boolean addCompleteRunnable(Stage stage, Runnable runnable)
    {
        if (completed)
            return false;

        postComplete.add(Pair.create(stage, runnable));
        return true;
    }

    @Override
    public void instanceCommitted(UUID committed)
    {
        prepareComplete(committed);
    }

    public int size()
    {
        return uncommitted.size();
    }
}
