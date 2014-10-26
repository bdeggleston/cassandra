package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

public class ExecuteTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(ExecuteTask.class);

    private final EpaxosState state;
    private final UUID iid;

    public ExecuteTask(EpaxosState state, UUID iid)
    {
        this.state = state;
        this.iid = iid;
    }

    @Override
    public void run()
    {
        logger.debug("Running execution phase for instance {}", iid);
        ReadWriteLock lock = state.getInstanceLock(iid);

        ExecutionSorter executionSorter;
        lock.writeLock().lock();
        try
        {
            Instance instance = state.loadInstance(iid);
            assert instance.getState().atLeast(Instance.State.COMMITTED);
            executionSorter = new ExecutionSorter(instance, state);
            executionSorter.buildGraph();
        }
        finally
        {
            lock.writeLock().unlock();
        }

        if (executionSorter.uncommitted.size() > 0)
        {
            logger.debug("Uncommitted ({}) instances found while attempting to execute {}",
                         executionSorter.uncommitted.size(), iid);
            PrepareGroup prepareGroup = new PrepareGroup(state, iid, executionSorter.uncommitted);
            prepareGroup.schedule();
        }
        else
        {
            for (UUID iid : executionSorter.getOrder())
            {
                lock.writeLock().lock();
                Instance toExecute = state.loadInstance(iid);
                try
                {
                    if (toExecute.getState() == Instance.State.EXECUTED)
                    {
                        if (toExecute.getId().equals(iid))
                        {
                            return;
                        }
                        else
                        {
                            continue;
                        }
                    }

                    assert toExecute.getState() == Instance.State.COMMITTED;

                    try
                    {
                        state.executeInstance(toExecute);
                    }
                    catch (InvalidRequestException | WriteTimeoutException | ReadTimeoutException e)
                    {
                        throw new RuntimeException(e);
                    }
                    toExecute.setExecuted();
                    state.saveInstance(toExecute);

                    // TODO: why not just eagerly execute everything?
                    if (toExecute.getId().equals(iid))
                        return;

                }
                finally
                {
                    lock.writeLock().unlock();
                }
            }
        }
    }
}
