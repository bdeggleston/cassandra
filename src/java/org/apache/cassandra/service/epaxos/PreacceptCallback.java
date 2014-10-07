package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Maps;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.paxos.AbstractPaxosCallback;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class PreacceptCallback extends AbstractPaxosCallback<PreacceptCallback>
{
    private final Set<UUID> dependencies;
    private final EpaxosManager.ParticipantInfo participantInfo;
    private final Map<InetAddress, Set<UUID>> responses;

    public PreacceptCallback(Set<UUID> dependencies, EpaxosManager.ParticipantInfo participantInfo)
    {
        super(participantInfo.quorumSize, participantInfo.consistencyLevel);
        this.dependencies = dependencies;
        this.participantInfo = participantInfo;
        this.responses = Maps.newConcurrentMap();
    }

    @Override
    public void response(MessageIn<PreacceptCallback> msg)
    {

    }
}
