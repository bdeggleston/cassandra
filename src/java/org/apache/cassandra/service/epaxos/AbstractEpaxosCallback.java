package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.service.paxos.AbstractPaxosCallback;

public abstract class AbstractEpaxosCallback<T> extends AbstractPaxosCallback<T>
{

    protected final EpaxosManager.ParticipantInfo participantInfo;

    protected AbstractEpaxosCallback(EpaxosManager.ParticipantInfo participantInfo)
    {
        super(participantInfo.quorumSize, participantInfo.consistencyLevel);
        this.participantInfo = participantInfo;
    }

    /**
     * counts the local node as a response
     */
    public void countLocal()
    {
        latch.countDown();
    }
}
