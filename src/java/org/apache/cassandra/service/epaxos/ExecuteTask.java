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
    private final UUID id;

    public ExecuteTask(EpaxosState state, UUID id)
    {
        this.state = state;
        this.id = id;
    }

    @Override
    public void run()
    {
        logger.debug("Running execution phase for instance {}", id);
        ReadWriteLock lock = state.getInstanceLock(id);

        ExecutionSorter executionSorter;
        lock.writeLock().lock();
        try
        {
            Instance instance = state.loadInstance(id);
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
                         executionSorter.uncommitted.size(), id);
            PrepareGroup prepareGroup = new PrepareGroup(state, id, executionSorter.uncommitted);
            prepareGroup.schedule();
        }
        else
        {
            for (UUID toExecuteId : executionSorter.getOrder())
            {
                lock = state.getInstanceLock(toExecuteId);
                lock.writeLock().lock();
                Instance toExecute = state.loadInstance(toExecuteId);
                try
                {
                    if (toExecute.getState() == Instance.State.EXECUTED)
                    {
                        if (toExecute.getId().equals(id))
                        {
                            int x = 1;
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
                    if (toExecute.getId().equals(id))
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
