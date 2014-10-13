package org.apache.cassandra.service.epaxos;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Builds and sorts the dependency graph to determine the execution
 * order.
 *
 * Also records strongly connected components onto instances
 */
class ExecutionSorter
{
    private final DependencyGraph dependencyGraph = new DependencyGraph();
    public final Set<UUID> uncommitted = new HashSet<>();
    private final Set<UUID> requiredInstances = new HashSet<>();

    private final Instance target;
    private final Set<UUID> targetDeps;
    private final EpaxosManager.IAccessor accessor;

    ExecutionSorter(Instance target, EpaxosManager.IAccessor accessor)
    {
        this.target = target;
        targetDeps = target.getDependencies();
        this.accessor = accessor;
    }

    private void addInstance(Instance instance)
    {
        Set<UUID> deps;
        Instance.State state;
        Set<UUID> stronglyConnected;
        ReadWriteLock lock = accessor.getLock(instance);
        lock.readLock().lock();
        try
        {
            deps = instance.getDependencies();
            state = instance.getState();
            stronglyConnected = instance.getStronglyConnected();
        }
        finally
        {
            lock.readLock().lock();
        }

        // if the instance is already executed, and it's not a dependency
        // of the target execution instance, only add it to the dep graph
        // if it's connected to an uncommitted instance, since that will
        // make it part of a strongly connected component of at least one
        // unexecuted instance, and will therefore affect the execution
        // ordering
        if (state == Instance.State.EXECUTED)
        {
            if (!targetDeps.contains(instance.getId()))
            {
                boolean connected = false;
                for (UUID dep: deps)
                {
                    boolean notExecuted = accessor.loadInstance(dep).getState() != Instance.State.EXECUTED;
                    boolean targetDep = targetDeps.contains(dep);
                    boolean required = requiredInstances.contains(dep);
                    connected |= notExecuted || targetDep || required;
                }
                if (!connected)
                    return;
            }

        }
        else if (state != Instance.State.COMMITTED)
        {
            uncommitted.add(instance.getId());
        }

        if (stronglyConnected != null)
            requiredInstances.addAll(stronglyConnected);

        dependencyGraph.addVertex(instance.getId(), deps);
        for (UUID dep: deps)
        {
            if (dependencyGraph.contains(dep))
                continue;

            Instance depInst = accessor.loadInstance(dep);
            assert depInst != null;

            addInstance(depInst);
        }
    }

    public void buildGraph()
    {
        addInstance(target);
    }

    public List<UUID> getOrder()
    {
        List<List<UUID>> scc = dependencyGraph.getExecutionOrder();

        // record the strongly connected components on the instances.
        // As instances are executed, they will stop being added to the depGraph for sorting.
        // However, if an instance that's not added to the dep graph is part of a strongly
        // connected component, it will affect the execution order by breaking the component.
        if (uncommitted.size() > 0)
        {
            for (List<UUID> component: scc)
            {
                if (component.size() > 1)
                {
                    // we're not using a lock, or persisting because:
                    // 1) the strongly connected ids will always be the same, and will always be computed
                    //    so writing the same value multiple times isn't a big deal.
                    // 2) the instance will be persisted when it's marked as executed. If it doesn't make
                    //    it that far, the strongly connected set will be computed again
                    Set<UUID> componentSet = ImmutableSet.copyOf(component);
                    for (UUID iid: component)
                        accessor.loadInstance(iid).setStronglyConnected(componentSet);
                }

            }
        }

        return Lists.newArrayList(Iterables.concat(scc));
    }
}
