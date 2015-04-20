package org.apache.cassandra.service.epaxos;

import java.net.InetAddress;
import java.util.Set;
import java.util.UUID;

public class TryPreacceptAttempt
{
    public final Set<UUID> dependencies;
    public final Set<InetAddress> toConvince;
    public final int requiredConvinced;
    public final Set<InetAddress> agreeingEndpoints;
    public final boolean agreedWithLeader;
    public final boolean vetoed;

    public TryPreacceptAttempt(Set<UUID> dependencies, Set<InetAddress> toConvince, int requiredConvinced, Set<InetAddress> agreeingEndpoints, boolean agreedWithLeader, boolean vetoed)
    {
        this.dependencies = dependencies;
        this.toConvince = toConvince;
        this.requiredConvinced = requiredConvinced;
        this.agreeingEndpoints = agreeingEndpoints;
        this.agreedWithLeader = agreedWithLeader;
        this.vetoed = vetoed;
    }
}
