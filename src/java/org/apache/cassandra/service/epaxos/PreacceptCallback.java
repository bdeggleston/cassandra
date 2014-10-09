package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Maps;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.paxos.AbstractPaxosCallback;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class PreacceptCallback extends AbstractPaxosCallback<PreacceptResponse>
{
    private final Instance instance;
    private final EpaxosManager.ParticipantInfo participantInfo;
    private final Map<InetAddress, PreacceptResponse> responses;

    public PreacceptCallback(Instance instance, EpaxosManager.ParticipantInfo participantInfo)
    {
        super(participantInfo.quorumSize, participantInfo.consistencyLevel);
        this.instance = instance;
        this.participantInfo = participantInfo;
        this.responses = Maps.newConcurrentMap();
    }

    @Override
    public void response(MessageIn<PreacceptResponse> msg)
    {
        PreacceptResponse response = msg.payload;
        responses.put(msg.from, response);

        latch.countDown();
    }
}
