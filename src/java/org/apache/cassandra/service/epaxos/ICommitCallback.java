package org.apache.cassandra.service.epaxos;

import java.util.UUID;

/**
 * Implemented by classes that need to be
 * notified of instances being committed
 */
public interface ICommitCallback
{
    public void instanceCommitted(UUID id);
}
