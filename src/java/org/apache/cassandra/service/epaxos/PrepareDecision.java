package org.apache.cassandra.service.epaxos;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class PrepareDecision
{
    public final Instance.State state;
    public final Set<UUID> deps;
    public final List<TryPreacceptAttempt> tryPreacceptAttempts;
    public final boolean commitNoop;

    public PrepareDecision(Instance.State state, Set<UUID> deps)
    {
        this(state, deps, null, false);
    }

    public PrepareDecision(Instance.State state, Set<UUID> deps, List<TryPreacceptAttempt> tryPreacceptAttempts, boolean commitNoop)
    {
        this.state = state;
        this.deps = deps != null ? ImmutableSet.copyOf(deps) : null;
        this.tryPreacceptAttempts = tryPreacceptAttempts;
        this.commitNoop = commitNoop;
    }
}
