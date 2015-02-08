package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.service.IEndpointLifecycleSubscriber;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import java.net.InetAddress;
import java.util.*;

public class EndpointListener implements IEndpointLifecycleSubscriber
{
    private final EpaxosState state;
    private final TokenStateManager tokenStateManager;

    public EndpointListener(EpaxosState state, TokenStateManager tokenStateManager)
    {
        this.state = state;
        this.tokenStateManager = tokenStateManager;
    }

    protected InetAddress getLocalEndpoint()
    {
        return FBUtilities.getLocalAddress();
    }

    protected Collection<Token> getLocalTokens()
    {
        return StorageService.instance.getLocalTokens();
    }

    protected List<Token> getAllTokens()
    {
        return StorageService.instance.getTokenMetadata().sortedTokens();
    }

    void maybeUpdateTokenStates()
    {
        // TODO: for each cfId epaxos manages, determin if the token set for our covered ranges has changed
        Set<UUID> cfIds = tokenStateManager.getAllManagedCfIds();

        for (UUID cfId: cfIds)
        {
            Keyspace keyspace = Keyspace.open(Schema.instance.getCF(cfId).left);
            AbstractReplicationStrategy rs = keyspace.getReplicationStrategy();
            Set<Token> currentManagedTokens = Sets.newHashSet(tokenStateManager.getManagedTokensForCf(cfId));
            Set<Token> expectedManagedTokens = new HashSet<>();
            for (Token token: getAllTokens())
            {
                if (rs.getNaturalEndpoints(token).contains(getLocalEndpoint()))
                {
                    expectedManagedTokens.add(token);
                }
            }

            // we're only adding new tokens, not removing old ones
            Set<Token> toAdd = Sets.difference(currentManagedTokens, expectedManagedTokens);
            if (toAdd.size() > 0)
            {

            }

        }
    }

    public void onJoinCluster(InetAddress endpoint)
    {
        maybeUpdateTokenStates();
    }

    public void onLeaveCluster(InetAddress endpoint)
    {
        maybeUpdateTokenStates();
    }

    public void onMove(InetAddress endpoint)
    {
        maybeUpdateTokenStates();
    }

    public void onUp(InetAddress endpoint) {}

    public void onDown(InetAddress endpoint) {}
}
