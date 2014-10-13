package org.apache.cassandra.service.epaxos.integration;

import com.google.common.collect.Lists;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class Cluster
{
    public final Messenger messenger = new Messenger();

    private final List<Node> nodes;
    private final int replicationFactor;

    public Cluster(int replicationFactor)
    {
        assert replicationFactor >= 3;
        this.replicationFactor = replicationFactor;
        nodes = Lists.newArrayListWithCapacity(replicationFactor);

        for (int i=0; i<replicationFactor; i++)
        {
            try
            {
                nodes.add(new Node(InetAddress.getByAddress(new byte[] {127, 0, 0, (byte) i}), messenger));
            }
            catch (UnknownHostException e)
            {
                throw new AssertionError(e);
            };
        }
    }
}
