package org.apache.cassandra.service.epaxos;

import java.util.Set;
import java.util.UUID;

public class AcceptDecision
{
    public final boolean acceptNeeded;
    public final Set<UUID> acceptDeps;

    public AcceptDecision(boolean acceptNeeded, Set<UUID> acceptDeps)
    {
        this.acceptNeeded = acceptNeeded;
        this.acceptDeps = acceptDeps;
    }
}
