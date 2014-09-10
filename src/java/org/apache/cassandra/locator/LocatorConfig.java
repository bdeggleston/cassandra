package org.apache.cassandra.locator;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.KeyspaceManager;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.FailureDetector;
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
    public static final LocatorConfig instance;

    static
    {
        LocatorConfig locatorConfig = null;
        try
        {
            locatorConfig = new LocatorConfig();
        }
        catch (ConfigurationException e)
        {
            logger.error("Fatal configuration error", e);
            System.err.println(e.getMessage() + "\nFatal configuration error; unable to start. See log for stacktrace.");
            System.exit(1);
        }
        catch (Exception e)
        {
            logger.error("Fatal error during configuration loading", e);
            System.err.println(e.getMessage() + "\nFatal error during configuration loading; unable to start. See log for stacktrace.");
            System.exit(1);
        }
        instance = locatorConfig;
    }

    private final Config conf;
    private TokenMetadata tokenMetadata;
    private IPartitioner<?> partitioner;
    private final String partitionerName;
    private IEndpointSnitch snitch;
    private final EndpointSnitchInfo endpointSnitchInfo;

    private String localDC;
    private Comparator<InetAddress> localComparator;

    public LocatorConfig() throws ConfigurationException
    {
        conf = DatabaseDescriptor.instance.getConfig();

        partitioner = createPartitioner();
        partitionerName = partitioner.getClass().getCanonicalName();
        if (conf.initial_token != null)
            for (String token : tokensFromString(conf.initial_token))
                partitioner.getTokenFactory().validate(token);

        snitch = createEndpointSnitch();
        tokenMetadata = new TokenMetadata(FailureDetector.instance, this);
        endpointSnitchInfo = EndpointSnitchInfo.create(snitch);

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

    private IPartitioner<?> createPartitioner() throws ConfigurationException
    {
        if (conf.partitioner == null)
        {
            throw new ConfigurationException("Missing directive: partitioner");
        }
        try
        {
            return FBUtilities.newPartitioner(System.getProperty("cassandra.partitioner", conf.partitioner));
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Invalid partitioner class " + conf.partitioner);
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
        return DatabaseDescriptor.instance.getListenAddress();
    }

    public InetAddress getLocalAddress()
    {
        return DatabaseDescriptor.instance.getLocalAddress();
    }

    public InetAddress getBroadcastAddress()
    {
        return DatabaseDescriptor.instance.getBroadcastAddress();
    }

    // endpoint snitch dependency getters

    public Gossiper getGossiper()
    {
        return Gossiper.instance;
    }

    public SystemKeyspace getSystemKeyspace()
    {
        return SystemKeyspace.instance;
    }

    public MessagingService getMessagingService()
    {
        return MessagingService.instance;
    }

    public DatabaseDescriptor getDatabaseDescriptor()
    {
        return DatabaseDescriptor.instance;
    }

    public StorageService getStorageService()
    {
        return StorageService.instance;
    }

    public StorageServiceExecutors getStorageServiceExecutors()
    {
        return StorageServiceExecutors.instance;
    }

    public KeyspaceManager getKeyspaceManager()
    {
        return KeyspaceManager.instance;
    }
}
