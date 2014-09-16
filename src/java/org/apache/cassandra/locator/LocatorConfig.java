package org.apache.cassandra.locator;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.KeyspaceManager;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.StorageServiceExecutors;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

public class LocatorConfig
{
    private static final Logger logger = LoggerFactory.getLogger(LocatorConfig.class);

    private final Config conf;
    private TokenMetadata tokenMetadata;
    private IPartitioner<?> partitioner;
    private final String partitionerName;
    private IEndpointSnitch snitch;
    private final EndpointSnitchInfo endpointSnitchInfo;

    private String localDC;
    private Comparator<InetAddress> localComparator;

    private final DatabaseDescriptor databaseDescriptor;

    public LocatorConfig(Config conf, DatabaseDescriptor databaseDescriptor) throws ConfigurationException
    {
        this.databaseDescriptor = databaseDescriptor;

        this.conf = conf;

        partitioner = createPartitioner(System.getProperty("cassandra.partitioner", this.conf.partitioner));
        partitionerName = partitioner.getClass().getCanonicalName();
        if (this.conf.initial_token != null)
            for (String token : tokensFromString(this.conf.initial_token))
                partitioner.getTokenFactory().validate(token);

        snitch = createEndpointSnitch();
        tokenMetadata = new TokenMetadata(databaseDescriptor.getFailureDetector(), this);
        endpointSnitchInfo = EndpointSnitchInfo.create(snitch, databaseDescriptor.shouldInitializeJMX());

        localDC = snitch.getDatacenter(getBroadcastAddress());
        localComparator = new Comparator<InetAddress>()
        {
            public int compare(InetAddress endpoint1, InetAddress endpoint2)
            {
                boolean local1 = localDC.equals(snitch.getDatacenter(endpoint1));
                boolean local2 = localDC.equals(snitch.getDatacenter(endpoint2));
                if (local1 && !local2)
                    return -1;
                if (local2 && !local1)
                    return 1;
                return 0;
            }
        };
    }

    public IPartitioner<?> createPartitioner(String name) throws ConfigurationException
    {
        String className = name;

        if (className == null)
            throw new ConfigurationException("Missing directive: partitioner");

        if (!className.contains("."))
            className = "org.apache.cassandra.dht." + className;
        Class<IPartitioner> cls = FBUtilities.classForName(className, "partitioner");
        Constructor<IPartitioner> constructor = FBUtilities.getConstructor(cls, LocatorConfig.class);
        try
        {
            return constructor != null ? constructor.newInstance(this) : cls.newInstance();
        }
        catch (InvocationTargetException | InstantiationException | IllegalAccessException e)
        {
            String msg = String.format(
                    "Error instantiating IAuthorizer: %s" +
                            "IAuthorizer implementations must support empty constructors, \n" +
                            "or constructors taking a single org.apache.cassandra.auth.Auth argument",
                    className);
            throw new ConfigurationException(msg, e);
        }
    }

    private IEndpointSnitch createEndpointSnitch() throws ConfigurationException
    {
        String className = conf.endpoint_snitch;
        if (!className.contains("."))
            className = "org.apache.cassandra.locator." + className;
        Class<IEndpointSnitch> cls = FBUtilities.classForName(className, "snitch");
        Constructor<IEndpointSnitch> constructor = FBUtilities.getConstructor(cls, LocatorConfig.class);
        try
        {
            return constructor != null ? constructor.newInstance(this) : cls.newInstance();
        }
        catch (InvocationTargetException | InstantiationException | IllegalAccessException e)
        {
            String msg = String.format(
                    "Error instantiating IAuthorizer: %s" +
                            "IAuthorizer implementations must support empty constructors, \n" +
                            "or constructors taking a single org.apache.cassandra.auth.Auth argument",
                    className);
            throw new ConfigurationException(msg, e);
        }
    }

    public IPartitioner getPartitioner()
    {
        return partitioner;
    }

    /* For tests ONLY, don't use otherwise or all hell will break loose */
    public void setPartitioner(IPartitioner<?> newPartitioner)
    {
        partitioner = newPartitioner;
    }

    public String getPartitionerName()
    {
        return partitionerName;
    }

    public Collection<String> getInitialTokens()
    {
        return tokensFromString(System.getProperty("cassandra.initial_token", conf.initial_token));
    }

    public Collection<String> getReplaceTokens()
    {
        return tokensFromString(System.getProperty("cassandra.replace_token", null));
    }

    public Collection<String> tokensFromString(String tokenString)
    {
        List<String> tokens = new ArrayList<String>();
        if (tokenString != null)
            for (String token : tokenString.split(","))
                tokens.add(token.replaceAll("^\\s+", "").replaceAll("\\s+$", ""));
        return tokens;
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
        AbstractReplicationStrategy strategy = databaseDescriptor.getKeyspaceManager().open(keyspace).getReplicationStrategy();
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
        return getRangesForEndpoint(keyspaceName, getBroadcastAddress());
    }

    public Collection<Range<Token>> getLocalPrimaryRanges(String keyspace)
    {
        return getPrimaryRangesForEndpoint(keyspace, getBroadcastAddress());
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
        return databaseDescriptor.getKeyspaceManager().open(keyspaceName).getReplicationStrategy().getAddressRanges().get(ep);
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
        CFMetaData cfMetaData = databaseDescriptor.getSchema().getKSMetaData(keyspaceName).cfMetaData().get(cf);
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
        return databaseDescriptor.getKeyspaceManager().open(keyspaceName).getReplicationStrategy().getNaturalEndpoints(pos);
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
            if (databaseDescriptor.getFailureDetector().isAlive(endpoint))
                liveEps.add(endpoint);
        }

        return liveEps;
    }

    public IEndpointSnitch getEndpointSnitch()
    {
        return snitch;
    }

    public void setEndpointSnitch(IEndpointSnitch eps)
    {
        snitch = eps;
    }

    public String getLocalDC()
    {
        return localDC;
    }

    public Comparator<InetAddress> getLocalComparator()
    {
        return localComparator;
    }

    public InetAddress getListenAddress()
    {
        return databaseDescriptor.getListenAddress();
    }

    public InetAddress getLocalAddress()
    {
        return databaseDescriptor.getLocalAddress();
    }

    public InetAddress getBroadcastAddress()
    {
        return databaseDescriptor.getBroadcastAddress();
    }

    // endpoint snitch dependency getters

    public Gossiper getGossiper()
    {
        return databaseDescriptor.getGossiper();
    }

    public SystemKeyspace getSystemKeyspace()
    {
        return databaseDescriptor.getSystemKeyspace();
    }

    public MessagingService getMessagingService()
    {
        return databaseDescriptor.getMessagingService();
    }

    public DatabaseDescriptor getDatabaseDescriptor()
    {
        return databaseDescriptor;
    }

    public StorageService getStorageService()
    {
        return databaseDescriptor.getStorageService();
    }

    public StorageServiceExecutors getStorageServiceExecutors()
    {
        return databaseDescriptor.getStorageServiceExecutors();
    }

    public KeyspaceManager getKeyspaceManager()
    {
        return databaseDescriptor.getKeyspaceManager();
    }

    public Schema getSchema()
    {
        return databaseDescriptor.getSchema();
    }
}
