package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.service.paxos.AbstractPaxosCallback;

public abstract class AbstractEpaxosCallback<T> extends AbstractPaxosCallback<T>
{

    protected final EpaxosService.ParticipantInfo participantInfo;

    protected AbstractEpaxosCallback(EpaxosService.ParticipantInfo participantInfo)
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
