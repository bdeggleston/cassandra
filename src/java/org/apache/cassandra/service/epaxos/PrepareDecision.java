package org.apache.cassandra.service.epaxos;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class PrepareDecision
{
    public final Instance.State state;
    public final Set<UUID> deps;
    public final List<Set<UUID>> tryPreacceptOrder;
    public final boolean commitNoop;

    public PrepareDecision(Instance.State state, Set<UUID> deps)
    {
        this(state, deps, null, false);
    }

    public PrepareDecision(Instance.State state, Set<UUID> deps, List<Set<UUID>> tryPreacceptOrder, boolean commitNoop)
    {
        this.state = state;
        this.deps = deps != null ? ImmutableSet.copyOf(deps) : null;
        this.tryPreacceptOrder = tryPreacceptOrder;
        this.commitNoop = commitNoop;
    }
}
