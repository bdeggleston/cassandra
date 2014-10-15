package org.apache.cassandra.service.epaxos.exceptions;

import org.apache.cassandra.service.epaxos.Instance;

/**
 * Raised when a prepare phase cannot be completed for an instance yet
 */
public class PrepareAbortException extends Exception
{
    public PrepareAbortException(Instance instance, String reason)
    {
        super(String.format("Prepare aborted for: %s. ", instance.getId()) + reason);
    }
}
