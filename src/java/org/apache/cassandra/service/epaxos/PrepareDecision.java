package org.apache.cassandra.service.epaxos;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class PrepareDecision
{
    public final Instance.State state;
    public final Set<UUID> deps;
    public final boolean vetoed;
    public final int ballot;
    public final List<TryPreacceptAttempt> tryPreacceptAttempts;
    public final boolean commitNoop;

    public PrepareDecision(Instance.State state, Set<UUID> deps, boolean vetoed, int ballot)
    {
        this(state, deps, vetoed, ballot, null, false);
    }

    public PrepareDecision(Instance.State state, Set<UUID> deps, boolean vetoed, int ballot, List<TryPreacceptAttempt> tryPreacceptAttempts, boolean commitNoop)
    {
        this.state = state;
        this.deps = deps != null ? ImmutableSet.copyOf(deps) : null;
        this.vetoed = vetoed;
        this.ballot = ballot;
        this.tryPreacceptAttempts = tryPreacceptAttempts;
        this.commitNoop = commitNoop;
    }

    @Override
    public String toString()
    {
        return "PrepareDecision{" +
                "state=" + state +
                ", tryPreacceptAttempts=" + (tryPreacceptAttempts != null ? tryPreacceptAttempts.size() : 0) +
                ", commitNoop=" + commitNoop +
                '}';
    }
}
