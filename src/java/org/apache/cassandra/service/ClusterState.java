package org.apache.cassandra.service;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.KeyspaceManager;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * partitioner, token metatdata, bootstrap state
 */
public class ClusterState
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterState.class);

    public static final ClusterState instance = new ClusterState(DatabaseDescriptor.instance, Schema.instance, FailureDetector.instance, CommitLog.instance, StorageServiceTasks.instance);

//    static
//    {
//        instance = new ClusterState(DatabaseDescriptor.instance, Schema.instance, FailureDetector.instance, CommitLog.instance, StorageServiceTasks.instance);
//
////        // FIXME: forcing KeyspaceManager to initialize before ClusterState does anything
////        KeyspaceManager keyspaceManager = KeyspaceManager.instance;
//    }

    /* This abstraction maintains the token/endpoint metadata information */
    private TokenMetadata tokenMetadata = new TokenMetadata();

    /* Are we starting this node in bootstrap mode? */
    private boolean isBootstrapMode;
    private volatile KeyspaceManager keyspaceManager = null;  // not the best

    private final DatabaseDescriptor databaseDescriptor;
    private final Schema schema;
    private final IFailureDetector failureDetector;
    private final CommitLog commitLog;
    private final StorageServiceTasks storageServiceTasks;

    public ClusterState(DatabaseDescriptor databaseDescriptor, Schema schema, IFailureDetector failureDetector, CommitLog commitLog, StorageServiceTasks storageServiceTasks)
    {
        assert databaseDescriptor != null;
        assert schema != null;
        assert failureDetector != null;
        assert commitLog != null;
        assert storageServiceTasks != null;

        this.databaseDescriptor = databaseDescriptor;
        this.schema = schema;
        this.failureDetector = failureDetector;
        this.commitLog = commitLog;
        this.storageServiceTasks = storageServiceTasks;
    }

    public void setKeyspaceManager(KeyspaceManager keyspaceManager)
    {
        assert keyspaceManager != null;
        assert this.keyspaceManager == null;
        this.keyspaceManager = keyspaceManager;
    }

    public TokenMetadata getTokenMetadata()
    {
        return tokenMetadata;
    }

    // Never ever do this at home. Used by tests.
    TokenMetadata setTokenMetadataUnsafe(TokenMetadata tmd)
    {
        TokenMetadata old = tokenMetadata;
        tokenMetadata = tmd;
        return old;
    }

    /**
     * Previously, primary range is the range that the node is responsible for and calculated
     * only from the token assigned to the node.
     * But this does not take replication strategy into account, and therefore returns insufficient
     * range especially using NTS with replication only to certain DC(see CASSANDRA-5424).
     *
     * @param ep endpoint we are interested in.
     * @return range for the specified endpoint.
     * @deprecated
     */
    @Deprecated
    @VisibleForTesting
    public Range<Token> getPrimaryRangeForEndpoint(InetAddress ep)
    {
        return tokenMetadata.getPrimaryRangeFor(tokenMetadata.getToken(ep));
    }

    /**
     * Get the "primary ranges" for the specified keyspace and endpoint.
     * "Primary ranges" are the ranges that the node is responsible for storing replica primarily.
     * The node that stores replica primarily is defined as the first node returned
     * by {@link AbstractReplicationStrategy#calculateNaturalEndpoints}.
     *
     * @param keyspace
     * @param ep       endpoint we are interested in.
     * @return primary ranges for the specified endpoint.
     */
    public Collection<Range<Token>> getPrimaryRangesForEndpoint(String keyspace, InetAddress ep)
    {
        assert keyspaceManager != null;
        AbstractReplicationStrategy strategy = keyspaceManager.open(keyspace).getReplicationStrategy();
        Collection<Range<Token>> primaryRanges = new HashSet<>();
        TokenMetadata metadata = tokenMetadata.cloneOnlyTokenMap();
        for (Token token : metadata.sortedTokens())
        {
            List<InetAddress> endpoints = strategy.calculateNaturalEndpoints(token, metadata);
            if (endpoints.size() > 0 && endpoints.get(0).equals(ep))
                primaryRanges.add(new Range<>(metadata.getPredecessor(token), token));
        }
        return primaryRanges;
    }


    /**
     * Get all ranges an endpoint is responsible for (by keyspace)
     *
     * @param ep endpoint we are interested in.
     * @return ranges for the specified endpoint.
     */
    Collection<Range<Token>> getRangesForEndpoint(String keyspaceName, InetAddress ep)
    {
        assert keyspaceManager != null;
        return keyspaceManager.open(keyspaceName).getReplicationStrategy().getAddressRanges().get(ep);
    }


    public Collection<Range<Token>> getLocalRanges(String keyspaceName)
    {
        return getRangesForEndpoint(keyspaceName, FBUtilities.getBroadcastAddress());
    }

    public Collection<Range<Token>> getLocalPrimaryRanges(String keyspace)
    {
        return getPrimaryRangesForEndpoint(keyspace, FBUtilities.getBroadcastAddress());
    }


    public IPartitioner getPartitioner()
    {
        return databaseDescriptor.getPartitioner();
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspaceName keyspace name also known as keyspace
     * @param cf           Column family name
     * @param key          key for which we need to find the endpoint
     * @return the endpoint responsible for this key
     */
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, String cf, String key)
    {
        CFMetaData cfMetaData = schema.getKSMetaData(keyspaceName).cfMetaData().get(cf);
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
     * @param pos          position for which we need to find the endpoint
     * @return the endpoint responsible for this token
     */
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, RingPosition pos)
    {
        assert keyspaceManager != null;
        return keyspaceManager.open(keyspaceName).getReplicationStrategy().getNaturalEndpoints(pos);
    }

    /**
     * This method attempts to return N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspace keyspace name also known as keyspace
     * @param key      key for which we need to find the endpoint
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
            if (failureDetector.isAlive(endpoint))
                liveEps.add(endpoint);
        }

        return liveEps;
    }

    public boolean isBootstrapMode()
    {
        return isBootstrapMode;
    }

    public void setBootstrapMode()
    {
        isBootstrapMode = true;
    }

    public void finishBootstrapping()
    {
        isBootstrapMode = false;
    }


    public CommitLog getCommitLog()
    {
        return commitLog;
    }

    public long getReadRpcTimeout()
    {
        return databaseDescriptor.getReadRpcTimeout();
    }

    public int getFlushWriters()
    {
        return databaseDescriptor.getFlushWriters();
    }

    public boolean isAutoSnapshot()
    {
        return databaseDescriptor.isAutoSnapshot();
    }

    public String getSavedCachesLocation()
    {
        return databaseDescriptor.getSavedCachesLocation();
    }

    public int getCompactionThroughputMbPerSec()
    {
        return databaseDescriptor.getCompactionThroughputMbPerSec();
    }

    public int getConcurrentCompactors()
    {
        return databaseDescriptor.getConcurrentCompactors();
    }

    public DebuggableScheduledThreadPoolExecutor getScheduledTasksExecutor()
    {
        return storageServiceTasks.scheduledTasks;
    }

    public DebuggableScheduledThreadPoolExecutor getTasksExecutor()
    {
        return storageServiceTasks.tasks;
    }

    public DebuggableScheduledThreadPoolExecutor getOptionalTasksExecutor()
    {
        return storageServiceTasks.optionalTasks;
    }

}
