package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.concurrent.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * A group of prepare phases that an instance execution is waiting on,
 *
 * When all of the prepare phases in this group have completed, another
 * execution task will be submitted. Just because a PrepareGroup has
 * 'completed' doesn't mean that all of it's instances were committed.
 *
 * If an prepare task couldn't find an instance, or a trypreaccept phase
 * needed to wait for other instances to commit, the prepare phase for
 * that instance will be considered completed, with the assumption that
 * the neccesary information will be available next time around.
 */
public class PrepareGroup implements ICommitCallback
{
    private static final Logger logger = LoggerFactory.getLogger(EpaxosState.class);

    private final EpaxosState state;
    private final UUID id;
    private final Set<UUID> uncommitted;
    private final Set<UUID> outstanding;
    private final Map<UUID, List<PrepareGroup>> groupNotify = new HashMap<>();

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
                if (instance != null && instance.getState().atLeast(Instance.State.COMMITTED))
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
        }
    }

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
