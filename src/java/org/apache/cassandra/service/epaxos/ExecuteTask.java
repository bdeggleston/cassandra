package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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
        Instance instance = state.getInstanceCopy(id);

        logger.debug("Running execution phase for instance {}, with deps: {}", id, instance.getDependencies());

        if (instance.getState() == Instance.State.EXECUTED)
        {
            logger.debug("Instance {} already executed", id);
            return;
        }

        assert instance.getState().atLeast(Instance.State.COMMITTED);
        ExecutionSorter executionSorter = new ExecutionSorter(instance, state);
        executionSorter.buildGraph();

        if (executionSorter.uncommitted.size() > 0)
        {
            logger.debug("Uncommitted ({}) instances found while attempting to execute {}:\n\t{}",
                         executionSorter.uncommitted.size(), id, executionSorter.uncommitted);
            PrepareGroup prepareGroup = new PrepareGroup(state, id, executionSorter.uncommitted);
            prepareGroup.schedule();
        }
        else
        {
            List<UUID> executionOrder = executionSorter.getOrder();
            for (UUID toExecuteId : executionOrder)
            {
                ReadWriteLock lock = state.getInstanceLock(toExecuteId);
                lock.writeLock().lock();
                Instance toExecute = state.loadInstance(toExecuteId);
                try
                {
                    if (toExecute.getState() == Instance.State.EXECUTED)
                    {
                        continue;
                    }

                    assert toExecute.getState() == Instance.State.COMMITTED;

                    ReplayPosition position = null;
                    try
                    {
                        if (!instance.skipExecution() && state.canExecute(instance))
                        {
                            position = state.executeInstance(toExecute);
                        }
                        else if (instance.getType() != Instance.Type.QUERY)
                        {
                            state.maybeSetResultFuture(instance.getId(), null);
                            logger.debug("Skipping execution for {}.", instance);
                        }

                        if (instance.getType() == Instance.Type.QUERY)
                        {
                            state.tokenStateManager.maybeRecordSerialInstance((QueryInstance) instance);
                        }
                    }
                    catch (InvalidRequestException | WriteTimeoutException | ReadTimeoutException e)
                    {
                        throw new RuntimeException(e);
                    }
                    toExecute.setExecuted(state.getCurrentEpoch(instance));
                    state.recordExecuted(toExecute, position);
                    state.saveInstance(toExecute);

                }
                finally
                {
                    lock.writeLock().unlock();
                }
            }
        }
    }
}
