package org.apache.cassandra.service.epaxos;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class AcceptDecision
{
    public final boolean acceptNeeded;
    public final Set<UUID> acceptDeps;
    public final Map<InetAddress, Set<UUID>> missingInstances;

    public AcceptDecision(boolean acceptNeeded, Set<UUID> acceptDeps, Map<InetAddress, Set<UUID>> missingInstances)
    {
        this.acceptNeeded = acceptNeeded;
        this.acceptDeps = acceptDeps;
        this.missingInstances = missingInstances;
    }
}
