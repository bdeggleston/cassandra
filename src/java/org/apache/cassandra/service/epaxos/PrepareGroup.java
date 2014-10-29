package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * A group of prepare phases that an instance execution is waiting on
 */
public class PrepareGroup implements ICommitCallback
{
    private static final Logger logger = LoggerFactory.getLogger(EpaxosState.class);

    private final EpaxosState state;
    private final UUID id;
    private final Set<UUID> uncommitted;
    private final Set<UUID> outstanding;
    private final List<Pair<Stage, Runnable>> postComplete = new LinkedList<>();
    private final Map<UUID, List<PrepareGroup>> groupNotify = new HashMap<>();
//    private boolean completed = false;

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
            ReadWriteLock lock = state.getInstanceLock(toPrepare);
            lock.readLock().lock();
            try
            {
                Instance instance = state.loadInstance(toPrepare);
                if (instance.getState().atLeast(Instance.State.COMMITTED))
                {
                    instanceCommitted(toPrepare);
                }
                else
                {

                    state.registerCommitCallback(toPrepare, this);
                    // if there's another prepare in progress for this instance, tell
                    // it to rerun this one when it finishes. This prevents a single
                    // node from running multiple concurrent prepare phases for the
                    // same instance.
                    // the api however, kinda sucks
                    // TODO: make not suck
                    while (true)
                    {
                        PrepareGroup previous = state.registerPrepareGroup(toPrepare, this);
                        if (previous == null)
                        {
                            break;
                        }
                        else if (previous.addCompleteGroup(toPrepare, this))
                        {
                            logger.debug("prepare already in progress for {}. Waiting for it to finish", id);
                            return;
                        }
                        logger.debug("attempting to register prepare group for {} failed, trying again.", id);
                    }
                    PrepareTask task = state.prepare(toPrepare, this);
                    state.registerCommitCallback(toPrepare, task);
                }
            }
            finally
            {
                lock.readLock().unlock();
            }
        }
    }

    public synchronized void prepareComplete(UUID completedId)
    {
        outstanding.remove(completedId);
        state.unregisterPrepareGroup(completedId);

        List<PrepareGroup> groupList = groupNotify.remove(completedId);
        if (groupList != null)
        {
            for (PrepareGroup group: groupList)
            {
                group.prepareComplete(completedId);
            }
        }

        if (outstanding.size() == 0)
        {

            state.getStage(Stage.MUTATION).submit(new ExecuteTask(state, id));

//            // FIXME: this sucks
//            for (Pair<Stage, Runnable> pair: postComplete)
//            {
//                if (pair.left != null)
//                {
//                    state.getStage(pair.left).submit(pair.right);
//                }
//                else
//                {
//                    pair.right.run();
//                }
//            }
        }
    }
//
//    /**
//     *
//     * @param runnable
//     * @return
//     */
//    // FIXME: this sucks
//    public synchronized boolean addCompleteRunnable(Stage stage, Runnable runnable)
//    {
//        if (completed)
//            return false;
//
//        postComplete.add(Pair.create(stage, runnable));
//        return true;
//    }

    public synchronized boolean addCompleteGroup(UUID toPrepare, PrepareGroup group)
    {
        if (!outstanding.contains(toPrepare))
            return false;

        List<PrepareGroup> groupList = groupNotify.get(toPrepare);
        if (groupList == null)
        {
            groupList = new LinkedList<>();
            groupNotify.put(toPrepare, groupList);
        }
        groupList.add(group);
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
