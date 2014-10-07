package org.apache.cassandra.service.epaxos;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class PreacceptResponse
{
    public final boolean successful;
    public final Set<UUID> dependencies;
    public final List<Instance> missingInstances;

    public PreacceptResponse(boolean successful, Set<UUID> dependencies, List<Instance> missingInstances)
    {
        this.successful = successful;
        this.dependencies = dependencies;
        this.missingInstances = missingInstances;
    }

}
