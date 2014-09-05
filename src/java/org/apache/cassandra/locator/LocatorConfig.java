package org.apache.cassandra.locator;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.KeyspaceManager;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

public class LocatorConfig
{
    public static final LocatorConfig instance = new LocatorConfig();

    private TokenMetadata tokenMetadata;

    public LocatorConfig()
    {
        this.tokenMetadata = new TokenMetadata();
    }

    public IPartitioner getPartitioner()
    {
        return DatabaseDescriptor.instance.getPartitioner();
    }

    public TokenMetadata getTokenMetadata()
    {
        return tokenMetadata;
    }

    public void setTokenMetadataUnsafe(TokenMetadata tokenMetadata)
    {
        this.tokenMetadata = tokenMetadata;
    }

    /**
     * Get the "primary ranges" for the specified keyspace and endpoint.
     * "Primary ranges" are the ranges that the node is responsible for storing replica primarily.
     * The node that stores replica primarily is defined as the first node returned
     * by {@link AbstractReplicationStrategy#calculateNaturalEndpoints}.
     *
     * @param keyspace
     * @param ep endpoint we are interested in.
     * @return primary ranges for the specified endpoint.
     */
    public Collection<Range<Token>> getPrimaryRangesForEndpoint(String keyspace, InetAddress ep)
    {
        AbstractReplicationStrategy strategy = KeyspaceManager.instance.open(keyspace).getReplicationStrategy();
        Collection<Range<Token>> primaryRanges = new HashSet<>();
        TokenMetadata metadata = tokenMetadata.cloneOnlyTokenMap();
        for (Token token : metadata.sortedTokens())
        {
            List<InetAddress> endpoints = strategy.calculateNaturalEndpoints(token, metadata);
            if (endpoints.size() > 0 && endpoints.get(0).equals(ep))
                primaryRanges.add(new Range<>(metadata.getPredecessor(token), token, getPartitioner()));
        }
        return primaryRanges;
    }

    public Collection<Range<Token>> getLocalRanges(String keyspaceName)
    {
        return getRangesForEndpoint(keyspaceName, DatabaseDescriptor.instance.getBroadcastAddress());
    }

    public Collection<Range<Token>> getLocalPrimaryRanges(String keyspace)
    {
        return getPrimaryRangesForEndpoint(keyspace, DatabaseDescriptor.instance.getBroadcastAddress());
    }


    /**
     * Previously, primary range is the range that the node is responsible for and calculated
     * only from the token assigned to the node.
     * But this does not take replication strategy into account, and therefore returns insufficient
     * range especially using NTS with replication only to certain DC(see CASSANDRA-5424).
     *
     * @deprecated
     * @param ep endpoint we are interested in.
     * @return range for the specified endpoint.
     */
    @Deprecated
    @VisibleForTesting
    public Range<Token> getPrimaryRangeForEndpoint(InetAddress ep)
    {
        return tokenMetadata.getPrimaryRangeFor(tokenMetadata.getToken(ep));
    }

    /**
     * Get all ranges an endpoint is responsible for (by keyspace)
     * @param ep endpoint we are interested in.
     * @return ranges for the specified endpoint.
     */
    public Collection<Range<Token>> getRangesForEndpoint(String keyspaceName, InetAddress ep)
    {
        return KeyspaceManager.instance.open(keyspaceName).getReplicationStrategy().getAddressRanges().get(ep);
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspaceName keyspace name also known as keyspace
     * @param cf Column family name
     * @param key key for which we need to find the endpoint
     * @return the endpoint responsible for this key
     */
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, String cf, String key)
    {
        CFMetaData cfMetaData = Schema.instance.getKSMetaData(keyspaceName).cfMetaData().get(cf);
        return getNaturalEndpoints(keyspaceName, getPartitioner().getToken(cfMetaData.getKeyValidator().fromString(key)));
    }

    public List<InetAddress> getNaturalEndpoints(String keyspaceName, ByteBuffer key)
    {
        return getNaturalEndpoints(keyspaceName, getPartitioner().getToken(key));
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspaceName keyspace name also known as keyspace
     * @param pos position for which we need to find the endpoint
     * @return the endpoint responsible for this token
     */
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, RingPosition pos)
    {
        return KeyspaceManager.instance.open(keyspaceName).getReplicationStrategy().getNaturalEndpoints(pos);
    }

    /**
     * This method attempts to return N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspace keyspace name also known as keyspace
     * @param key key for which we need to find the endpoint
     * @return the endpoint responsible for this key
     */
    public List<InetAddress> getLiveNaturalEndpoints(Keyspace keyspace, ByteBuffer key)
    {
        return getLiveNaturalEndpoints(keyspace, getPartitioner().decorateKey(key));
    }

    public List<InetAddress> getLiveNaturalEndpoints(Keyspace keyspace, RingPosition pos)
    {
        List<InetAddress> endpoints = keyspace.getReplicationStrategy().getNaturalEndpoints(pos);
        List<InetAddress> liveEps = new ArrayList<>(endpoints.size());

        for (InetAddress endpoint : endpoints)
        {
            if (FailureDetector.instance.isAlive(endpoint))
                liveEps.add(endpoint);
        }

        return liveEps;
    }


}
