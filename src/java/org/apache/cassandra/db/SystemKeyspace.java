/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.openmbean.*;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import org.apache.cassandra.config.*;
import org.apache.cassandra.locator.LocatorConfig;
import org.apache.cassandra.tracing.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.compaction.CompactionHistoryTabularData;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.thrift.cassandraConstants;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.*;

public class SystemKeyspace
{
    private static final Logger logger = LoggerFactory.getLogger(SystemKeyspace.class);

    public static final SystemKeyspace instance;
    static
    {
        instance = new SystemKeyspace();
        KSMetaDataFactory ksmdf = KSMetaDataFactory.instance;
    }

    // see CFMetaData for schema definitions
    public static final String PEERS_CF = "peers";
    public static final String PEER_EVENTS_CF = "peer_events";
    public static final String LOCAL_CF = "local";
    public static final String INDEX_CF = "IndexInfo";
    public static final String HINTS_CF = "hints";
    public static final String RANGE_XFERS_CF = "range_xfers";
    public static final String BATCHLOG_CF = "batchlog";
    // see layout description in the DefsTables class header
    public static final String SCHEMA_KEYSPACES_CF = "schema_keyspaces";
    public static final String SCHEMA_COLUMNFAMILIES_CF = "schema_columnfamilies";
    public static final String SCHEMA_COLUMNS_CF = "schema_columns";
    public static final String SCHEMA_TRIGGERS_CF = "schema_triggers";
    public static final String SCHEMA_USER_TYPES_CF = "schema_usertypes";
    public static final String SCHEMA_FUNCTIONS_CF = "schema_functions";
    public static final String COMPACTION_LOG = "compactions_in_progress";
    public static final String PAXOS_CF = "paxos";
    public static final String SSTABLE_ACTIVITY_CF = "sstable_activity";
    public static final String COMPACTION_HISTORY_CF = "compaction_history";

    private static final String LOCAL_KEY = "local";

    public static final List<String> allSchemaCfs = Arrays.asList(SCHEMA_KEYSPACES_CF,
                                                                  SCHEMA_COLUMNFAMILIES_CF,
                                                                  SCHEMA_COLUMNS_CF,
                                                                  SCHEMA_TRIGGERS_CF,
                                                                  SCHEMA_USER_TYPES_CF,
                                                                  SCHEMA_FUNCTIONS_CF);

    private final AtomicReference<UUID> localHostId = new AtomicReference<>();

    private volatile Map<UUID, Pair<ReplayPosition, Long>> truncationRecords;

    public enum BootstrapState
    {
        NEEDS_BOOTSTRAP,
        COMPLETED,
        IN_PROGRESS
    }

    private DecoratedKey decorate(ByteBuffer key)
    {
        return LocatorConfig.instance.getPartitioner().decorateKey(key);
    }

    public void finishStartup()
    {
        setupVersion();

        migrateIndexInterval();
        migrateCachingOption();
        // add entries to system schema columnfamilies for the hardcoded system definitions
        for (String ksname : Schema.systemKeyspaceNames)
        {
            KSMetaData ksmd = Schema.instance.getKSMetaData(ksname);

            // delete old, possibly obsolete entries in schema columnfamilies
            for (String cfname : Arrays.asList(SystemKeyspace.SCHEMA_KEYSPACES_CF, SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF, SystemKeyspace.SCHEMA_COLUMNS_CF))
                QueryProcessor.instance.executeOnceInternal(String.format("DELETE FROM system.%s WHERE keyspace_name = ?", cfname), ksmd.name);

            // (+1 to timestamp to make sure we don't get shadowed by the tombstones we just added)
            ksmd.toSchema(FBUtilities.timestampMicros() + 1).apply();
        }
    }

    private void setupVersion()
    {
        String req = "INSERT INTO system.%s (key, release_version, cql_version, thrift_version, native_protocol_version, data_center, rack, partitioner) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        IEndpointSnitch snitch = LocatorConfig.instance.getEndpointSnitch();
        QueryProcessor.instance.executeOnceInternal(String.format(req, LOCAL_CF),
                                                    LOCAL_KEY,
                                                    FBUtilities.getReleaseVersionString(),
                                                    QueryProcessor.CQL_VERSION.toString(),
                                                    cassandraConstants.VERSION,
                                                    String.valueOf(Server.CURRENT_VERSION),
                                                    snitch.getDatacenter(DatabaseDescriptor.instance.getBroadcastAddress()),
                                                    snitch.getRack(DatabaseDescriptor.instance.getBroadcastAddress()),
                                                    LocatorConfig.instance.getPartitioner().getClass().getName());
    }

    // TODO: In 3.0, remove this and the index_interval column from system.schema_columnfamilies
    /** Migrates index_interval values to min_index_interval and sets index_interval to null */
    private void migrateIndexInterval()
    {
        for (UntypedResultSet.Row row : QueryProcessor.instance.executeOnceInternal(String.format("SELECT * FROM system.%s", SCHEMA_COLUMNFAMILIES_CF)))
        {
            if (!row.has("index_interval"))
                continue;

            logger.debug("Migrating index_interval to min_index_interval");

            CFMetaData table = CFMetaDataFactory.instance.fromSchema(row);
            String query = String.format("SELECT writetime(type) FROM system.%s WHERE keyspace_name = ? AND columnfamily_name = ?", SCHEMA_COLUMNFAMILIES_CF);
            long timestamp = QueryProcessor.instance.executeOnceInternal(query, table.ksName, table.cfName).one().getLong("writetime(type)");
            try
            {
                table.toSchema(timestamp).apply();
            }
            catch (ConfigurationException e)
            {
                // shouldn't happen
            }
        }
    }

    private void migrateCachingOption()
    {
        for (UntypedResultSet.Row row : QueryProcessor.instance.executeOnceInternal(String.format("SELECT * FROM system.%s", SCHEMA_COLUMNFAMILIES_CF)))
        {
            if (!row.has("caching"))
                continue;

            if (!CachingOptions.isLegacy(row.getString("caching")))
                continue;
            try
            {
                CachingOptions caching = CachingOptions.fromString(row.getString("caching"));
                CFMetaData table = CFMetaDataFactory.instance.fromSchema(row);
                logger.info("Migrating caching option {} to {} for {}.{}", row.getString("caching"), caching.toString(), table.ksName, table.cfName);
                String query = String.format("SELECT writetime(type) FROM system.%s WHERE keyspace_name = ? AND columnfamily_name = ?", SCHEMA_COLUMNFAMILIES_CF);
                long timestamp = QueryProcessor.instance.executeOnceInternal(query, table.ksName, table.cfName).one().getLong("writetime(type)");
                table.toSchema(timestamp).apply();
            }
            catch (ConfigurationException e)
            {
                // shouldn't happen
            }
        }
    }

    /**
     * Write compaction log, except columfamilies under system keyspace.
     *
     * @param cfs
     * @param toCompact sstables to compact
     * @return compaction task id or null if cfs is under system keyspace
     */
    public UUID startCompaction(ColumnFamilyStore cfs, Iterable<SSTableReader> toCompact)
    {
        if (Keyspace.SYSTEM_KS.equals(cfs.keyspace.getName()))
            return null;

        UUID compactionId = UUIDGen.getTimeUUID();
        Iterable<Integer> generations = Iterables.transform(toCompact, new Function<SSTableReader, Integer>()
        {
            public Integer apply(SSTableReader sstable)
            {
                return sstable.descriptor.generation;
            }
        });
        String req = "INSERT INTO system.%s (id, keyspace_name, columnfamily_name, inputs) VALUES (?, ?, ?, ?)";
        QueryProcessor.instance.executeInternal(String.format(req, COMPACTION_LOG), compactionId, cfs.keyspace.getName(), cfs.name, Sets.newHashSet(generations));
        forceBlockingFlush(COMPACTION_LOG);
        return compactionId;
    }

    /**
     * Deletes the entry for this compaction from the set of compactions in progress.  The compaction does not need
     * to complete successfully for this to be called.
     * @param taskId what was returned from {@code startCompaction}
     */
    public void finishCompaction(UUID taskId)
    {
        assert taskId != null;

        QueryProcessor.instance.executeInternal(String.format("DELETE FROM system.%s WHERE id = ?", COMPACTION_LOG), taskId);
        forceBlockingFlush(COMPACTION_LOG);
    }

    /**
     * Returns a Map whose keys are KS.CF pairs and whose values are maps from sstable generation numbers to the
     * task ID of the compaction they were participating in.
     */
    public Map<Pair<String, String>, Map<Integer, UUID>> getUnfinishedCompactions()
    {
        String req = "SELECT * FROM system.%s";
        UntypedResultSet resultSet = QueryProcessor.instance.executeInternal(String.format(req, COMPACTION_LOG));

        Map<Pair<String, String>, Map<Integer, UUID>> unfinishedCompactions = new HashMap<>();
        for (UntypedResultSet.Row row : resultSet)
        {
            String keyspace = row.getString("keyspace_name");
            String columnfamily = row.getString("columnfamily_name");
            Set<Integer> inputs = row.getSet("inputs", Int32Type.instance);
            UUID taskID = row.getUUID("id");

            Pair<String, String> kscf = Pair.create(keyspace, columnfamily);
            Map<Integer, UUID> generationToTaskID = unfinishedCompactions.get(kscf);
            if (generationToTaskID == null)
                generationToTaskID = new HashMap<>(inputs.size());

            for (Integer generation : inputs)
                generationToTaskID.put(generation, taskID);

            unfinishedCompactions.put(kscf, generationToTaskID);
        }
        return unfinishedCompactions;
    }

    public void discardCompactionsInProgress()
    {
        ColumnFamilyStore compactionLog = KeyspaceManager.instance.open(Keyspace.SYSTEM_KS).getColumnFamilyStore(COMPACTION_LOG);
        compactionLog.truncateBlocking();
    }

    public void updateCompactionHistory(String ksname,
                                               String cfname,
                                               long compactedAt,
                                               long bytesIn,
                                               long bytesOut,
                                               Map<Integer, Long> rowsMerged)
    {
        // don't write anything when the history table itself is compacted, since that would in turn cause new compactions
        if (ksname.equals("system") && cfname.equals(COMPACTION_HISTORY_CF))
            return;
        String req = "INSERT INTO system.%s (id, keyspace_name, columnfamily_name, compacted_at, bytes_in, bytes_out, rows_merged) VALUES (?, ?, ?, ?, ?, ?, ?)";
        QueryProcessor.instance.executeInternal(String.format(req, COMPACTION_HISTORY_CF), UUIDGen.getTimeUUID(), ksname, cfname, ByteBufferUtil.bytes(compactedAt), bytesIn, bytesOut, rowsMerged);
    }

    public TabularData getCompactionHistory() throws OpenDataException
    {
        UntypedResultSet queryResultSet = QueryProcessor.instance.executeInternal(String.format("SELECT * from system.%s", COMPACTION_HISTORY_CF));
        return CompactionHistoryTabularData.from(queryResultSet);
    }

    public synchronized void saveTruncationRecord(ColumnFamilyStore cfs, long truncatedAt, ReplayPosition position)
    {
        String req = "UPDATE system.%s SET truncated_at = truncated_at + ? WHERE key = '%s'";
        QueryProcessor.instance.executeInternal(String.format(req, LOCAL_CF, LOCAL_KEY), truncationAsMapEntry(cfs, truncatedAt, position));
        truncationRecords = null;
        forceBlockingFlush(LOCAL_CF);
    }

    /**
     * This method is used to remove information about truncation time for specified column family
     */
    public synchronized void removeTruncationRecord(UUID cfId)
    {
        String req = "DELETE truncated_at[?] from system.%s WHERE key = '%s'";
        QueryProcessor.instance.executeInternal(String.format(req, LOCAL_CF, LOCAL_KEY), cfId);
        truncationRecords = null;
        forceBlockingFlush(LOCAL_CF);
    }

    private Map<UUID, ByteBuffer> truncationAsMapEntry(ColumnFamilyStore cfs, long truncatedAt, ReplayPosition position)
    {
        DataOutputBuffer out = new DataOutputBuffer();
        try
        {
            ReplayPosition.serializer.serialize(position, out);
            out.writeLong(truncatedAt);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return Collections.<UUID, ByteBuffer>singletonMap(cfs.metadata.cfId, ByteBuffer.wrap(out.getData(), 0, out.getLength()));
    }

    public ReplayPosition getTruncatedPosition(UUID cfId)
    {
        Pair<ReplayPosition, Long> record = getTruncationRecord(cfId);
        return record == null ? null : record.left;
    }

    public long getTruncatedAt(UUID cfId)
    {
        Pair<ReplayPosition, Long> record = getTruncationRecord(cfId);
        return record == null ? Long.MIN_VALUE : record.right;
    }

    private synchronized Pair<ReplayPosition, Long> getTruncationRecord(UUID cfId)
    {
        if (truncationRecords == null)
            truncationRecords = readTruncationRecords();
        return truncationRecords.get(cfId);
    }

    private Map<UUID, Pair<ReplayPosition, Long>> readTruncationRecords()
    {
        UntypedResultSet rows = QueryProcessor.instance.executeInternal(String.format("SELECT truncated_at FROM system.%s WHERE key = '%s'", LOCAL_CF, LOCAL_KEY));

        Map<UUID, Pair<ReplayPosition, Long>> records = new HashMap<>();

        if (!rows.isEmpty() && rows.one().has("truncated_at"))
        {
            Map<UUID, ByteBuffer> map = rows.one().getMap("truncated_at", UUIDType.instance, BytesType.instance);
            for (Map.Entry<UUID, ByteBuffer> entry : map.entrySet())
                records.put(entry.getKey(), truncationRecordFromBlob(entry.getValue()));
        }

        return records;
    }

    private Pair<ReplayPosition, Long> truncationRecordFromBlob(ByteBuffer bytes)
    {
        try
        {
            DataInputStream in = new DataInputStream(ByteBufferUtil.inputStream(bytes));
            return Pair.create(ReplayPosition.serializer.deserialize(in), in.available() > 0 ? in.readLong() : Long.MIN_VALUE);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Record tokens being used by another node
     */
    public synchronized void updateTokens(InetAddress ep, Collection<Token> tokens)
    {
        if (ep.equals(DatabaseDescriptor.instance.getBroadcastAddress()))
        {
            removeEndpoint(ep);
            return;
        }

        String req = "INSERT INTO system.%s (peer, tokens) VALUES (?, ?)";
        QueryProcessor.instance.executeInternal(String.format(req, PEERS_CF), ep, tokensAsSet(tokens));
    }

    public synchronized void updatePreferredIP(InetAddress ep, InetAddress preferred_ip)
    {
        String req = "INSERT INTO system.%s (peer, preferred_ip) VALUES (?, ?)";
        QueryProcessor.instance.executeInternal(String.format(req, PEERS_CF), ep, preferred_ip);
        forceBlockingFlush(PEERS_CF);
    }

    public synchronized void updatePeerInfo(InetAddress ep, String columnName, Object value)
    {
        if (ep.equals(DatabaseDescriptor.instance.getBroadcastAddress()))
            return;

        String req = "INSERT INTO system.%s (peer, %s) VALUES (?, ?)";
        QueryProcessor.instance.executeInternal(String.format(req, PEERS_CF, columnName), ep, value);
    }

    public synchronized void updateHintsDropped(InetAddress ep, UUID timePeriod, int value)
    {
        // with 30 day TTL
        String req = "UPDATE system.%s USING TTL 2592000 SET hints_dropped[ ? ] = ? WHERE peer = ?";
        QueryProcessor.instance.executeInternal(String.format(req, PEER_EVENTS_CF), timePeriod, value, ep);
    }

    public synchronized void updateSchemaVersion(UUID version)
    {
        String req = "INSERT INTO system.%s (key, schema_version) VALUES ('%s', ?)";
        QueryProcessor.instance.executeInternal(String.format(req, LOCAL_CF, LOCAL_KEY), version);
    }

    private Set<String> tokensAsSet(Collection<Token> tokens)
    {
        Token.TokenFactory factory = LocatorConfig.instance.getPartitioner().getTokenFactory();
        Set<String> s = new HashSet<>(tokens.size());
        for (Token tk : tokens)
            s.add(factory.toString(tk));
        return s;
    }

    private Collection<Token> deserializeTokens(Collection<String> tokensStrings)
    {
        Token.TokenFactory factory = LocatorConfig.instance.getPartitioner().getTokenFactory();
        List<Token> tokens = new ArrayList<Token>(tokensStrings.size());
        for (String tk : tokensStrings)
            tokens.add(factory.fromString(tk));
        return tokens;
    }

    /**
     * Remove stored tokens being used by another node
     */
    public synchronized void removeEndpoint(InetAddress ep)
    {
        String req = "DELETE FROM system.%s WHERE peer = ?";
        QueryProcessor.instance.executeInternal(String.format(req, PEERS_CF), ep);
    }

    /**
     * This method is used to update the System Keyspace with the new tokens for this node
    */
    public synchronized void updateTokens(Collection<Token> tokens)
    {
        assert !tokens.isEmpty() : "removeEndpoint should be used instead";
        String req = "INSERT INTO system.%s (key, tokens) VALUES ('%s', ?)";
        QueryProcessor.instance.executeInternal(String.format(req, LOCAL_CF, LOCAL_KEY), tokensAsSet(tokens));
        forceBlockingFlush(LOCAL_CF);
    }

    /**
     * Convenience method to update the list of tokens in the local system keyspace.
     *
     * @param addTokens tokens to add
     * @param rmTokens tokens to remove
     * @return the collection of persisted tokens
     */
    public synchronized Collection<Token> updateLocalTokens(Collection<Token> addTokens, Collection<Token> rmTokens)
    {
        Collection<Token> tokens = getSavedTokens();
        tokens.removeAll(rmTokens);
        tokens.addAll(addTokens);
        updateTokens(tokens);
        return tokens;
    }

    public void forceBlockingFlush(String cfname)
    {
        if (!Boolean.getBoolean("cassandra.unsafesystem"))
            FBUtilities.waitOnFuture(KeyspaceManager.instance.open(Keyspace.SYSTEM_KS).getColumnFamilyStore(cfname).forceFlush());
    }

    /**
     * Return a map of stored tokens to IP addresses
     *
     */
    public SetMultimap<InetAddress, Token> loadTokens()
    {
        SetMultimap<InetAddress, Token> tokenMap = HashMultimap.create();
        for (UntypedResultSet.Row row : QueryProcessor.instance.executeInternal("SELECT peer, tokens FROM system." + PEERS_CF))
        {
            InetAddress peer = row.getInetAddress("peer");
            if (row.has("tokens"))
                tokenMap.putAll(peer, deserializeTokens(row.getSet("tokens", UTF8Type.instance)));
        }

        return tokenMap;
    }

    /**
     * Return a map of store host_ids to IP addresses
     *
     */
    public Map<InetAddress, UUID> loadHostIds()
    {
        Map<InetAddress, UUID> hostIdMap = new HashMap<InetAddress, UUID>();
        for (UntypedResultSet.Row row : QueryProcessor.instance.executeInternal("SELECT peer, host_id FROM system." + PEERS_CF))
        {
            InetAddress peer = row.getInetAddress("peer");
            if (row.has("host_id"))
            {
                hostIdMap.put(peer, row.getUUID("host_id"));
            }
        }
        return hostIdMap;
    }

    public InetAddress getPreferredIP(InetAddress ep)
    {
        String req = "SELECT preferred_ip FROM system.%s WHERE peer=?";
        UntypedResultSet result = QueryProcessor.instance.executeInternal(String.format(req, PEERS_CF), ep);
        if (!result.isEmpty() && result.one().has("preferred_ip"))
            return result.one().getInetAddress("preferred_ip");
        return null;
    }

    /**
     * Return a map of IP addresses containing a map of dc and rack info
     */
    public Map<InetAddress, Map<String,String>> loadDcRackInfo()
    {
        Map<InetAddress, Map<String, String>> result = new HashMap<InetAddress, Map<String, String>>();
        for (UntypedResultSet.Row row : QueryProcessor.instance.executeInternal("SELECT peer, data_center, rack from system." + PEERS_CF))
        {
            InetAddress peer = row.getInetAddress("peer");
            if (row.has("data_center") && row.has("rack"))
            {
                Map<String, String> dcRack = new HashMap<String, String>();
                dcRack.put("data_center", row.getString("data_center"));
                dcRack.put("rack", row.getString("rack"));
                result.put(peer, dcRack);
            }
        }
        return result;
    }

    /**
     * One of three things will happen if you try to read the system keyspace:
     * 1. files are present and you can read them: great
     * 2. no files are there: great (new node is assumed)
     * 3. files are present but you can't read them: bad
     * @throws ConfigurationException
     */
    public void checkHealth() throws ConfigurationException
    {
        Keyspace keyspace;
        try
        {
            keyspace = KeyspaceManager.instance.open(Keyspace.SYSTEM_KS);
        }
        catch (AssertionError err)
        {
            // this happens when a user switches from OPP to RP.
            ConfigurationException ex = new ConfigurationException("Could not read system keyspace!");
            ex.initCause(err);
            throw ex;
        }
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(LOCAL_CF);

        String req = "SELECT cluster_name FROM system.%s WHERE key='%s'";
        UntypedResultSet result = QueryProcessor.instance.executeInternal(String.format(req, LOCAL_CF, LOCAL_KEY));

        if (result.isEmpty() || !result.one().has("cluster_name"))
        {
            // this is a brand new node
            if (!cfs.getSSTables().isEmpty())
                throw new ConfigurationException("Found system keyspace files, but they couldn't be loaded!");

            // no system files.  this is a new node.
            req = "INSERT INTO system.%s (key, cluster_name) VALUES ('%s', ?)";
            QueryProcessor.instance.executeInternal(String.format(req, LOCAL_CF, LOCAL_KEY), DatabaseDescriptor.instance.getClusterName());
            return;
        }

        String savedClusterName = result.one().getString("cluster_name");
        if (!DatabaseDescriptor.instance.getClusterName().equals(savedClusterName))
            throw new ConfigurationException("Saved cluster name " + savedClusterName + " != configured name " + DatabaseDescriptor.instance.getClusterName());
    }

    public Collection<Token> getSavedTokens()
    {
        String req = "SELECT tokens FROM system.%s WHERE key='%s'";
        UntypedResultSet result = QueryProcessor.instance.executeInternal(String.format(req, LOCAL_CF, LOCAL_KEY));
        return result.isEmpty() || !result.one().has("tokens")
             ? Collections.<Token>emptyList()
             : deserializeTokens(result.one().<String>getSet("tokens", UTF8Type.instance));
    }

    public int incrementAndGetGeneration()
    {
        String req = "SELECT gossip_generation FROM system.%s WHERE key='%s'";
        UntypedResultSet result = QueryProcessor.instance.executeInternal(String.format(req, LOCAL_CF, LOCAL_KEY));

        int generation;
        if (result.isEmpty() || !result.one().has("gossip_generation"))
        {
            // seconds-since-epoch isn't a foolproof new generation
            // (where foolproof is "guaranteed to be larger than the last one seen at this ip address"),
            // but it's as close as sanely possible
            generation = (int) (System.currentTimeMillis() / 1000);
        }
        else
        {
            // Other nodes will ignore gossip messages about a node that have a lower generation than previously seen.
            final int storedGeneration = result.one().getInt("gossip_generation") + 1;
            final int now = (int) (System.currentTimeMillis() / 1000);
            if (storedGeneration >= now)
            {
                logger.warn("Using stored Gossip Generation {} as it is greater than current system time {}.  See CASSANDRA-3654 if you experience problems",
                            storedGeneration, now);
                generation = storedGeneration;
            }
            else
            {
                generation = now;
            }
        }

        req = "INSERT INTO system.%s (key, gossip_generation) VALUES ('%s', ?)";
        QueryProcessor.instance.executeInternal(String.format(req, LOCAL_CF, LOCAL_KEY), generation);
        forceBlockingFlush(LOCAL_CF);

        return generation;
    }

    public BootstrapState getBootstrapState()
    {
        String req = "SELECT bootstrapped FROM system.%s WHERE key='%s'";
        UntypedResultSet result = QueryProcessor.instance.executeInternal(String.format(req, LOCAL_CF, LOCAL_KEY));

        if (result.isEmpty() || !result.one().has("bootstrapped"))
            return BootstrapState.NEEDS_BOOTSTRAP;

        return BootstrapState.valueOf(result.one().getString("bootstrapped"));
    }

    public boolean bootstrapComplete()
    {
        return getBootstrapState() == BootstrapState.COMPLETED;
    }

    public boolean bootstrapInProgress()
    {
        return getBootstrapState() == BootstrapState.IN_PROGRESS;
    }

    public void setBootstrapState(BootstrapState state)
    {
        String req = "INSERT INTO system.%s (key, bootstrapped) VALUES ('%s', ?)";
        QueryProcessor.instance.executeInternal(String.format(req, LOCAL_CF, LOCAL_KEY), state.name());
        forceBlockingFlush(LOCAL_CF);
    }

    public boolean isIndexBuilt(String keyspaceName, String indexName)
    {
        ColumnFamilyStore cfs = KeyspaceManager.instance.open(Keyspace.SYSTEM_KS).getColumnFamilyStore(INDEX_CF);
        QueryFilter filter = QueryFilter.getNamesFilter(decorate(ByteBufferUtil.bytes(keyspaceName)),
                                                        INDEX_CF,
                                                        FBUtilities.singleton(cfs.getComparator().makeCellName(indexName), cfs.getComparator()),
                                                        System.currentTimeMillis(), DBConfig.instance);
        return ColumnFamilyStore.removeDeleted(cfs.getColumnFamily(filter), Integer.MAX_VALUE) != null;
    }

    public void setIndexBuilt(String keyspaceName, String indexName)
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(Keyspace.SYSTEM_KS, INDEX_CF, Schema.instance, DBConfig.instance);
        cf.addColumn(new BufferCell(cf.getComparator().makeCellName(indexName), ByteBufferUtil.EMPTY_BYTE_BUFFER, FBUtilities.timestampMicros()));
        MutationFactory.instance.create(Keyspace.SYSTEM_KS, ByteBufferUtil.bytes(keyspaceName), cf).apply();
    }

    public void setIndexRemoved(String keyspaceName, String indexName)
    {
        Mutation mutation = MutationFactory.instance.create(Keyspace.SYSTEM_KS, ByteBufferUtil.bytes(keyspaceName));
        mutation.delete(INDEX_CF, CFMetaDataFactory.instance.IndexCf.comparator.makeCellName(indexName), FBUtilities.timestampMicros());
        mutation.apply();
    }

    /**
     * Read the host ID from the system keyspace, creating (and storing) one if
     * none exists.
     */
    public UUID getLocalHostId()
    {
        UUID id = localHostId.get();
        if (id != null)
            return id;

        String req = "SELECT host_id FROM system.%s WHERE key='%s'";
        UntypedResultSet result = QueryProcessor.instance.executeInternal(String.format(req, LOCAL_CF, LOCAL_KEY));

        // Look up the Host UUID (return it if found)
        if (!result.isEmpty() && result.one().has("host_id"))
        {
            id = result.one().getUUID("host_id");
            localHostId.set(id);
            return id;
        }

        // ID not found, generate a new one, persist, and then return it.
        UUID hostId = UUID.randomUUID();
        logger.warn("No host ID found, created {} (Note: This should happen exactly once per node).", hostId);
        return setLocalHostId(hostId);
    }

    /**
     * Sets the local host ID explicitly.  Should only be called outside of SystemTable when replacing a node.
     */
    public UUID setLocalHostId(UUID hostId)
    {
        if (localHostId.compareAndSet(null, hostId))
        {
            String req = "INSERT INTO system.%s (key, host_id) VALUES ('%s', ?)";
            QueryProcessor.instance.executeInternal(String.format(req, LOCAL_CF, LOCAL_KEY), hostId);
        }
        return localHostId.get();
    }

    /**
     * @param cfName The name of the ColumnFamily responsible for part of the schema (keyspace, ColumnFamily, columns)
     * @return CFS responsible to hold low-level serialized schema
     */
    public ColumnFamilyStore schemaCFS(String cfName)
    {
        return KeyspaceManager.instance.open(Keyspace.SYSTEM_KS).getColumnFamilyStore(cfName);
    }

    public List<Row> serializedSchema()
    {
        List<Row> schema = new ArrayList<>();

        for (String cf : allSchemaCfs)
            schema.addAll(serializedSchema(cf));

        return schema;
    }

    /**
     * @param schemaCfName The name of the ColumnFamily responsible for part of the schema (keyspace, ColumnFamily, columns)
     * @return low-level schema representation (each row represents individual Keyspace or ColumnFamily)
     */
    public List<Row> serializedSchema(String schemaCfName)
    {
        Token minToken = LocatorConfig.instance.getPartitioner().getMinimumToken();

        return schemaCFS(schemaCfName).getRangeSlice(new Range<RowPosition>(minToken.minKeyBound(), minToken.maxKeyBound(), LocatorConfig.instance.getPartitioner()),
                                                     null,
                                                     new IdentityQueryFilter(DatabaseDescriptor.instance, Tracing.instance, DBConfig.instance),
                                                     Integer.MAX_VALUE,
                                                     System.currentTimeMillis());
    }

    public Collection<Mutation> serializeSchema()
    {
        Map<DecoratedKey, Mutation> mutationMap = new HashMap<>();

        for (String cf : allSchemaCfs)
            serializeSchema(mutationMap, cf);

        return mutationMap.values();
    }

    private void serializeSchema(Map<DecoratedKey, Mutation> mutationMap, String schemaCfName)
    {
        for (Row schemaRow : serializedSchema(schemaCfName))
        {
            if (Schema.ignoredSchemaRow(schemaRow))
                continue;

            Mutation mutation = mutationMap.get(schemaRow.key);
            if (mutation == null)
            {
                mutation = MutationFactory.instance.create(Keyspace.SYSTEM_KS, schemaRow.key.getKey());
                mutationMap.put(schemaRow.key, mutation);
            }

            mutation.add(schemaRow.cf);
        }
    }

    public Map<DecoratedKey, ColumnFamily> getSchema(String cfName)
    {
        Map<DecoratedKey, ColumnFamily> schema = new HashMap<>();

        for (Row schemaEntity : serializedSchema(cfName))
            schema.put(schemaEntity.key, schemaEntity.cf);

        return schema;
    }

    public Map<DecoratedKey, ColumnFamily> getSchema(String schemaCfName, Set<String> keyspaces)
    {
        Map<DecoratedKey, ColumnFamily> schema = new HashMap<>();

        for (String keyspace : keyspaces)
        {
            Row schemaEntity = readSchemaRow(schemaCfName, keyspace);
            if (schemaEntity.cf != null)
                schema.put(schemaEntity.key, schemaEntity.cf);
        }

        return schema;
    }

    public ByteBuffer getSchemaKSKey(String ksName)
    {
        return AsciiType.instance.fromString(ksName);
    }

    /**
     * Fetches a subset of schema (table data, columns metadata or triggers) for the keyspace.
     *
     * @param schemaCfName the schema table to get the data from (schema_keyspaces, schema_columnfamilies, schema_columns or schema_triggers)
     * @param ksName the keyspace of the tables we are interested in
     * @return a Row containing the schema data of a particular type for the keyspace
     */
    public Row readSchemaRow(String schemaCfName, String ksName)
    {
        DecoratedKey key = LocatorConfig.instance.getPartitioner().decorateKey(getSchemaKSKey(ksName));

        ColumnFamilyStore schemaCFS = schemaCFS(schemaCfName);
        ColumnFamily result = schemaCFS.getColumnFamily(QueryFilter.getIdentityFilter(key, schemaCfName, System.currentTimeMillis(), DatabaseDescriptor.instance, Tracing.instance, DBConfig.instance));

        return new Row(key, result);
    }

    /**
     * Fetches a subset of schema (table data, columns metadata or triggers) for the keyspace+table pair.
     *
     * @param schemaCfName the schema table to get the data from (schema_columnfamilies, schema_columns or schema_triggers)
     * @param ksName the keyspace of the table we are interested in
     * @param cfName the table we are interested in
     * @return a Row containing the schema data of a particular type for the table
     */
    public Row readSchemaRow(String schemaCfName, String ksName, String cfName)
    {
        DecoratedKey key = LocatorConfig.instance.getPartitioner().decorateKey(getSchemaKSKey(ksName));
        ColumnFamilyStore schemaCFS = schemaCFS(schemaCfName);
        Composite prefix = schemaCFS.getComparator().make(cfName);
        ColumnFamily cf = schemaCFS.getColumnFamily(key,
                                                    prefix,
                                                    prefix.end(),
                                                    false,
                                                    Integer.MAX_VALUE,
                                                    System.currentTimeMillis());
        return new Row(key, cf);
    }

    public PaxosState loadPaxosState(ByteBuffer key, CFMetaData metadata)
    {
        String req = "SELECT * FROM system.%s WHERE row_key = ? AND cf_id = ?";
        UntypedResultSet results = QueryProcessor.instance.executeInternal(String.format(req, PAXOS_CF), key, metadata.cfId);
        if (results.isEmpty())
            return new PaxosState(key, metadata, MutationFactory.instance, DBConfig.instance);
        UntypedResultSet.Row row = results.one();
        Commit promised = row.has("in_progress_ballot")
                        ? new Commit(key, row.getUUID("in_progress_ballot"), ArrayBackedSortedColumns.factory.create(metadata, DBConfig.instance), MutationFactory.instance)
                        : Commit.emptyCommit(key, metadata, MutationFactory.instance, DBConfig.instance);
        // either we have both a recently accepted ballot and update or we have neither
        Commit accepted = row.has("proposal")
                        ? new Commit(key, row.getUUID("proposal_ballot"), ColumnFamily.fromBytes(row.getBytes("proposal"), DBConfig.instance.columnFamilySerializer), MutationFactory.instance)
                        : Commit.emptyCommit(key, metadata, MutationFactory.instance, DBConfig.instance);
        // either most_recent_commit and most_recent_commit_at will both be set, or neither
        Commit mostRecent = row.has("most_recent_commit")
                          ? new Commit(key, row.getUUID("most_recent_commit_at"), ColumnFamily.fromBytes(row.getBytes("most_recent_commit"), DBConfig.instance.columnFamilySerializer), MutationFactory.instance)
                          : Commit.emptyCommit(key, metadata, MutationFactory.instance, DBConfig.instance);
        return new PaxosState(promised, accepted, mostRecent);
    }

    public void savePaxosPromise(Commit promise)
    {
        String req = "UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET in_progress_ballot = ? WHERE row_key = ? AND cf_id = ?";
        QueryProcessor.instance.executeInternal(String.format(req, PAXOS_CF),
                                                UUIDGen.microsTimestamp(promise.ballot),
                                                paxosTtl(promise.update.metadata),
                                                promise.ballot,
                                                promise.key,
                                                promise.update.id());
    }

    public void savePaxosProposal(Commit proposal)
    {
        QueryProcessor.instance.executeInternal(String.format("UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET proposal_ballot = ?, proposal = ? WHERE row_key = ? AND cf_id = ?", PAXOS_CF),
                                                UUIDGen.microsTimestamp(proposal.ballot),
                                                paxosTtl(proposal.update.metadata),
                                                proposal.ballot,
                                                proposal.update.toBytes(),
                                                proposal.key,
                                                proposal.update.id());
    }

    private int paxosTtl(CFMetaData metadata)
    {
        // keep paxos state around for at least 3h
        return Math.max(3 * 3600, metadata.getGcGraceSeconds());
    }

    public void savePaxosCommit(Commit commit)
    {
        // We always erase the last proposal (with the commit timestamp to no erase more recent proposal in case the commit is old)
        // even though that's really just an optimization  since SP.beginAndRepairPaxos will exclude accepted proposal older than the mrc.
        String cql = "UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET proposal_ballot = null, proposal = null, most_recent_commit_at = ?, most_recent_commit = ? WHERE row_key = ? AND cf_id = ?";
        QueryProcessor.instance.executeInternal(String.format(cql, PAXOS_CF),
                                                UUIDGen.microsTimestamp(commit.ballot),
                                                paxosTtl(commit.update.metadata),
                                                commit.ballot,
                                                commit.update.toBytes(),
                                                commit.key,
                                                commit.update.id());
    }

    /**
     * Returns a RestorableMeter tracking the average read rate of a particular SSTable, restoring the last-seen rate
     * from values in system.sstable_activity if present.
     * @param keyspace the keyspace the sstable belongs to
     * @param table the table the sstable belongs to
     * @param generation the generation number for the sstable
     */
    public RestorableMeter getSSTableReadMeter(String keyspace, String table, int generation)
    {
        String cql = "SELECT * FROM system.%s WHERE keyspace_name=? and columnfamily_name=? and generation=?";
        UntypedResultSet results = QueryProcessor.instance.executeInternal(String.format(cql, SSTABLE_ACTIVITY_CF), keyspace, table, generation);

        if (results.isEmpty())
            return new RestorableMeter();

        UntypedResultSet.Row row = results.one();
        double m15rate = row.getDouble("rate_15m");
        double m120rate = row.getDouble("rate_120m");
        return new RestorableMeter(m15rate, m120rate);
    }

    /**
     * Writes the current read rates for a given SSTable to system.sstable_activity
     */
    public void persistSSTableReadMeter(String keyspace, String table, int generation, RestorableMeter meter)
    {
        // Store values with a one-day TTL to handle corner cases where cleanup might not occur
        String cql = "INSERT INTO system.%s (keyspace_name, columnfamily_name, generation, rate_15m, rate_120m) VALUES (?, ?, ?, ?, ?) USING TTL 864000";
        QueryProcessor.instance.executeInternal(String.format(cql, SSTABLE_ACTIVITY_CF),
                                                keyspace,
                                                table,
                                                generation,
                                                meter.fifteenMinuteRate(),
                                                meter.twoHourRate());
    }

    /**
     * Clears persisted read rates from system.sstable_activity for SSTables that have been deleted.
     */
    public void clearSSTableReadMeter(String keyspace, String table, int generation)
    {
        String cql = "DELETE FROM system.%s WHERE keyspace_name=? AND columnfamily_name=? and generation=?";
        QueryProcessor.instance.executeInternal(String.format(cql, SSTABLE_ACTIVITY_CF), keyspace, table, generation);
    }
}
