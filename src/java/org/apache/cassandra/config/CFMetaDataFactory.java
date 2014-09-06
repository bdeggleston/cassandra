package org.apache.cassandra.config;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class CFMetaDataFactory
{
    private static final Logger logger = LoggerFactory.getLogger(CFMetaData.class);

    public static final CFMetaDataFactory instance = new CFMetaDataFactory();

    public final CFMetaData IndexCf = compile("CREATE TABLE \"" + SystemKeyspace.INDEX_CF + "\" ("
                                                      + "table_name text,"
                                                      + "index_name text,"
                                                      + "PRIMARY KEY (table_name, index_name)"
                                                      + ") WITH COMPACT STORAGE AND COMMENT='indexes that have been completed'",
                                              QueryProcessor.instance);

    public final CFMetaData SchemaKeyspacesCf = compile("CREATE TABLE " + SystemKeyspace.SCHEMA_KEYSPACES_CF + " ("
                                                                + "keyspace_name text PRIMARY KEY,"
                                                                + "durable_writes boolean,"
                                                                + "strategy_class text,"
                                                                + "strategy_options text"
                                                                + ") WITH COMPACT STORAGE AND COMMENT='keyspace definitions' AND gc_grace_seconds=604800",
                                                        QueryProcessor.instance);

    public final CFMetaData SchemaColumnFamiliesCf = compile("CREATE TABLE " + SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF + " ("
                                                                     + "keyspace_name text,"
                                                                     + "columnfamily_name text,"
                                                                     + "cf_id uuid," // post-2.1 UUID cfid
                                                                     + "type text,"
                                                                     + "is_dense boolean,"
                                                                     + "comparator text,"
                                                                     + "subcomparator text,"
                                                                     + "comment text,"
                                                                     + "read_repair_chance double,"
                                                                     + "local_read_repair_chance double,"
                                                                     + "gc_grace_seconds int,"
                                                                     + "default_validator text,"
                                                                     + "key_validator text,"
                                                                     + "min_compaction_threshold int,"
                                                                     + "max_compaction_threshold int,"
                                                                     + "memtable_flush_period_in_ms int,"
                                                                     + "key_aliases text,"
                                                                     + "bloom_filter_fp_chance double,"
                                                                     + "caching text,"
                                                                     + "default_time_to_live int,"
                                                                     + "compaction_strategy_class text,"
                                                                     + "compression_parameters text,"
                                                                     + "value_alias text,"
                                                                     + "column_aliases text,"
                                                                     + "compaction_strategy_options text,"
                                                                     + "speculative_retry text,"
                                                                     + "index_interval int,"
                                                                     + "min_index_interval int,"
                                                                     + "max_index_interval int,"
                                                                     + "dropped_columns map<text, bigint>,"
                                                                     + "PRIMARY KEY (keyspace_name, columnfamily_name)"
                                                                     + ") WITH COMMENT='ColumnFamily definitions' AND gc_grace_seconds=604800",
                                                             QueryProcessor.instance);

    public final CFMetaData SchemaColumnsCf = compile("CREATE TABLE " + SystemKeyspace.SCHEMA_COLUMNS_CF + " ("
                                                              + "keyspace_name text,"
                                                              + "columnfamily_name text,"
                                                              + "column_name text,"
                                                              + "validator text,"
                                                              + "index_type text,"
                                                              + "index_options text,"
                                                              + "index_name text,"
                                                              + "component_index int,"
                                                              + "type text,"
                                                              + "PRIMARY KEY(keyspace_name, columnfamily_name, column_name)"
                                                              + ") WITH COMMENT='ColumnFamily column attributes' AND gc_grace_seconds=604800",
                                                      QueryProcessor.instance);

    public final CFMetaData SchemaTriggersCf = compile("CREATE TABLE " + SystemKeyspace.SCHEMA_TRIGGERS_CF + " ("
                                                               + "keyspace_name text,"
                                                               + "columnfamily_name text,"
                                                               + "trigger_name text,"
                                                               + "trigger_options map<text, text>,"
                                                               + "PRIMARY KEY (keyspace_name, columnfamily_name, trigger_name)"
                                                               + ") WITH COMMENT='triggers metadata table' AND gc_grace_seconds=604800",
                                                       QueryProcessor.instance);

    public final CFMetaData SchemaUserTypesCf = compile("CREATE TABLE " + SystemKeyspace.SCHEMA_USER_TYPES_CF + " ("
                                                                + "keyspace_name text,"
                                                                + "type_name text,"
                                                                + "field_names list<text>,"
                                                                + "field_types list<text>,"
                                                                + "PRIMARY KEY (keyspace_name, type_name)"
                                                                + ") WITH COMMENT='Defined user types' AND gc_grace_seconds=604800",
                                                        QueryProcessor.instance);

    public final CFMetaData SchemaFunctionsCf = compile("CREATE TABLE " + SystemKeyspace.SCHEMA_FUNCTIONS_CF + " ("
                                                                + "namespace text,"
                                                                + "name text,"
                                                                + "signature blob,"
                                                                + "argument_names list<text>,"
                                                                + "argument_types list<text>,"
                                                                + "return_type text,"
                                                                + "deterministic boolean,"
                                                                + "language text,"
                                                                + "body text,"
                                                                + "primary key ((namespace, name), signature)"
                                                                + ") WITH COMMENT='user defined functions' AND gc_grace_seconds=604800",
                                                        QueryProcessor.instance);

    public final CFMetaData HintsCf = compile("CREATE TABLE " + SystemKeyspace.HINTS_CF + " ("
                                                      + "target_id uuid,"
                                                      + "hint_id timeuuid,"
                                                      + "message_version int,"
                                                      + "mutation blob,"
                                                      + "PRIMARY KEY (target_id, hint_id, message_version)"
                                                      + ") WITH COMPACT STORAGE "
                                                      + "AND COMPACTION={'class' : 'SizeTieredCompactionStrategy', 'enabled' : false} "
                                                      + "AND COMMENT='hints awaiting delivery'"
                                                      + "AND gc_grace_seconds=0",
                                              QueryProcessor.instance);

    public final CFMetaData PeersCf = compile("CREATE TABLE " + SystemKeyspace.PEERS_CF + " ("
                                                      + "peer inet PRIMARY KEY,"
                                                      + "host_id uuid,"
                                                      + "tokens set<varchar>,"
                                                      + "schema_version uuid,"
                                                      + "release_version text,"
                                                      + "rpc_address inet,"
                                                      + "preferred_ip inet,"
                                                      + "data_center text,"
                                                      + "rack text"
                                                      + ") WITH COMMENT='known peers in the cluster'",
                                              QueryProcessor.instance);

    public final CFMetaData PeerEventsCf = compile("CREATE TABLE " + SystemKeyspace.PEER_EVENTS_CF + " ("
                                                           + "peer inet PRIMARY KEY,"
                                                           + "hints_dropped map<uuid, int>"
                                                           + ") WITH COMMENT='cf contains events related to peers'",
                                                   QueryProcessor.instance);

    public final CFMetaData LocalCf = compile("CREATE TABLE " + SystemKeyspace.LOCAL_CF + " ("
                                                      + "key text PRIMARY KEY,"
                                                      + "tokens set<varchar>,"
                                                      + "cluster_name text,"
                                                      + "gossip_generation int,"
                                                      + "bootstrapped text,"
                                                      + "host_id uuid,"
                                                      + "release_version text,"
                                                      + "thrift_version text,"
                                                      + "cql_version text,"
                                                      + "native_protocol_version text,"
                                                      + "data_center text,"
                                                      + "rack text,"
                                                      + "partitioner text,"
                                                      + "schema_version uuid,"
                                                      + "truncated_at map<uuid, blob>"
                                                      + ") WITH COMMENT='information about the local node'",
                                              QueryProcessor.instance);

    public final CFMetaData TraceSessionsCf = compile("CREATE TABLE " + Tracing.SESSIONS_CF + " ("
                                                              + "session_id uuid PRIMARY KEY,"
                                                              + "coordinator inet,"
                                                              + "request text,"
                                                              + "started_at timestamp,"
                                                              + "parameters map<text, text>,"
                                                              + "duration int"
                                                              + ") WITH COMMENT='traced sessions'",
                                                      Tracing.TRACE_KS,
                                                      QueryProcessor.instance);

    public final CFMetaData TraceEventsCf = compile("CREATE TABLE " + Tracing.EVENTS_CF + " ("
                                                            + "session_id uuid,"
                                                            + "event_id timeuuid,"
                                                            + "source inet,"
                                                            + "thread text,"
                                                            + "activity text,"
                                                            + "source_elapsed int,"
                                                            + "PRIMARY KEY (session_id, event_id)"
                                                            + ")",
                                                    Tracing.TRACE_KS,
                                                    QueryProcessor.instance);

    public final CFMetaData BatchlogCf = compile("CREATE TABLE " + SystemKeyspace.BATCHLOG_CF + " ("
                                                         + "id uuid PRIMARY KEY,"
                                                         + "written_at timestamp,"
                                                         + "data blob,"
                                                         + "version int,"
                                                         + ") WITH COMMENT='uncommited batches' AND gc_grace_seconds=0 "
                                                         + "AND COMPACTION={'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 2}",
                                                 QueryProcessor.instance);

    public final CFMetaData RangeXfersCf = compile("CREATE TABLE " + SystemKeyspace.RANGE_XFERS_CF + " ("
                                                           + "token_bytes blob PRIMARY KEY,"
                                                           + "requested_at timestamp"
                                                           + ") WITH COMMENT='ranges requested for transfer here'",
                                                   QueryProcessor.instance);

    public final CFMetaData CompactionLogCf = compile("CREATE TABLE " + SystemKeyspace.COMPACTION_LOG + " ("
                                                              + "id uuid PRIMARY KEY,"
                                                              + "keyspace_name text,"
                                                              + "columnfamily_name text,"
                                                              + "inputs set<int>"
                                                              + ") WITH COMMENT='unfinished compactions'",
                                                      QueryProcessor.instance);

    public final CFMetaData PaxosCf = compile("CREATE TABLE " + SystemKeyspace.PAXOS_CF + " ("
                                                      + "row_key blob,"
                                                      + "cf_id UUID,"
                                                      + "in_progress_ballot timeuuid,"
                                                      + "proposal_ballot timeuuid,"
                                                      + "proposal blob,"
                                                      + "most_recent_commit_at timeuuid,"
                                                      + "most_recent_commit blob,"
                                                      + "PRIMARY KEY (row_key, cf_id)"
                                                      + ") WITH COMMENT='in-progress paxos proposals' "
                                                      + "AND COMPACTION={'class' : 'LeveledCompactionStrategy'}",
                                              QueryProcessor.instance);

    public final CFMetaData SSTableActivityCF = compile("CREATE TABLE " + SystemKeyspace.SSTABLE_ACTIVITY_CF + " ("
                                                                + "keyspace_name text,"
                                                                + "columnfamily_name text,"
                                                                + "generation int,"
                                                                + "rate_15m double,"
                                                                + "rate_120m double,"
                                                                + "PRIMARY KEY ((keyspace_name, columnfamily_name, generation))"
                                                                + ") WITH COMMENT='historic sstable read rates'",
                                                        QueryProcessor.instance);

    public final CFMetaData CompactionHistoryCf = compile("CREATE TABLE " + SystemKeyspace.COMPACTION_HISTORY_CF + " ("
                                                                  + "id uuid,"
                                                                  + "keyspace_name text,"
                                                                  + "columnfamily_name text,"
                                                                  + "compacted_at timestamp,"
                                                                  + "bytes_in bigint,"
                                                                  + "bytes_out bigint,"
                                                                  + "rows_merged map<int, bigint>,"
                                                                  + "PRIMARY KEY (id)"
                                                                  + ") WITH COMMENT='show all compaction history' AND DEFAULT_TIME_TO_LIVE=604800",
                                                          QueryProcessor.instance);

    public CFMetaData create(String keyspace, String name, ColumnFamilyType columnFamilyType, CellNameType cellNameType)
    {
        return new CFMetaData(keyspace, name, columnFamilyType, cellNameType, SystemKeyspace.instance, Schema.instance, ColumnFamilyStoreManager.instance,
                              KeyspaceManager.instance, this, MutationFactory.instance);
    }

    public CFMetaData denseCFMetaData(String keyspace, String name, AbstractType<?> comp, AbstractType<?> subcc)
    {
        CellNameType cellNameType = CellNames.fromAbstractType(makeRawAbstractType(comp, subcc), true);
        return create(keyspace, name, subcc == null ? ColumnFamilyType.Standard : ColumnFamilyType.Super, cellNameType);
    }

    public CFMetaData sparseCFMetaData(String keyspace, String name, AbstractType<?> comp)
    {
        CellNameType cellNameType = CellNames.fromAbstractType(comp, false);
        return create(keyspace, name, ColumnFamilyType.Standard, cellNameType);
    }

    public CFMetaData denseCFMetaData(String keyspace, String name, AbstractType<?> comp)
    {
        return denseCFMetaData(keyspace, name, comp, null);
    }

    public CFMetaData compile(String cql, QueryProcessor queryProcessor)
    {
        return compile(cql, Keyspace.SYSTEM_KS, queryProcessor);
    }

    public CFMetaData compile(String cql, String keyspace, QueryProcessor queryProcessor)
    {
        try
        {
            CFStatement parsed = (CFStatement) queryProcessor.parseStatement(cql);
            parsed.prepareKeyspace(keyspace);
            CreateTableStatement statement = (CreateTableStatement) parsed.prepare().statement;
            CFMetaData cfm = newSystemMetadata(keyspace, statement.columnFamily(), "", statement.comparator);
            statement.applyPropertiesTo(cfm);
            return cfm.rebuild();
        }
        catch (RequestValidationException e)
        {
            throw new RuntimeException(e);
        }
    }

    private CFMetaData newSystemMetadata(String keyspace, String cfName, String comment, CellNameType comparator)
    {
        return new CFMetaData(keyspace, cfName, ColumnFamilyType.Standard, comparator, CFMetaData.generateLegacyCfId(keyspace, cfName),
                              SystemKeyspace.instance, Schema.instance, ColumnFamilyStoreManager.instance, KeyspaceManager.instance, this, MutationFactory.instance)
                .comment(comment)
                .readRepairChance(0)
                .dcLocalReadRepairChance(0)
                .gcGraceSeconds(0)
                .memtableFlushPeriod(3600 * 1000);
    }

    /**
     * Creates CFMetaData for secondary index CF.
     * Secondary index CF has the same CF ID as parent's.
     *
     * @param parent Parent CF where secondary index is created
     * @param info Column definition containing secondary index definition
     * @param indexComparator Comparator for secondary index
     * @return CFMetaData for secondary index
     */
    public CFMetaData newIndexMetadata(CFMetaData parent, ColumnDefinition info, CellNameType indexComparator)
    {
        // Depends on parent's cache setting, turn on its index CF's cache.
        // Row caching is never enabled; see CASSANDRA-5732
        CachingOptions indexCaching = parent.getCaching().keyCache.isEnabled()
                ? CachingOptions.KEYS_ONLY
                : CachingOptions.NONE;

        return new CFMetaData(parent.ksName, parent.indexColumnFamilyName(info), ColumnFamilyType.Standard, indexComparator, parent.cfId,
                              SystemKeyspace.instance, Schema.instance, ColumnFamilyStoreManager.instance, KeyspaceManager.instance, this, MutationFactory.instance)
                .keyValidator(info.type)
                .readRepairChance(0.0)
                .dcLocalReadRepairChance(0.0)
                .gcGraceSeconds(0)
                .caching(indexCaching)
                .speculativeRetry(parent.getSpeculativeRetry())
                .compactionStrategyClass(parent.compactionStrategyClass)
                .compactionStrategyOptions(parent.compactionStrategyOptions)
                .reloadSecondaryIndexMetadata(parent)
                .rebuild();
    }

    public CFMetaData fromThrift(CfDef cf_def) throws InvalidRequestException, ConfigurationException
    {
        return internalFromThrift(cf_def, Collections.<ColumnDefinition>emptyList());
    }

    public CFMetaData fromThriftForUpdate(CfDef cf_def, CFMetaData toUpdate) throws InvalidRequestException, ConfigurationException
    {
        return internalFromThrift(cf_def, toUpdate.allColumns());
    }

    // Convert a thrift CfDef, given a list of ColumnDefinitions to copy over to the created CFMetadata before the CQL metadata are rebuild
    private CFMetaData internalFromThrift(CfDef cf_def, Collection<ColumnDefinition> previousCQLMetadata) throws InvalidRequestException, ConfigurationException
    {
        ColumnFamilyType cfType = ColumnFamilyType.create(cf_def.column_type);
        if (cfType == null)
            throw new InvalidRequestException("Invalid column type " + cf_def.column_type);

        CFMetaData.applyImplicitDefaults(cf_def);

        try
        {
            AbstractType<?> rawComparator = TypeParser.parse(cf_def.comparator_type);
            AbstractType<?> subComparator = cfType == ColumnFamilyType.Standard
                    ? null
                    : cf_def.subcomparator_type == null ? BytesType.instance : TypeParser.parse(cf_def.subcomparator_type);

            AbstractType<?> fullRawComparator = makeRawAbstractType(rawComparator, subComparator);

            AbstractType<?> keyValidator = cf_def.isSetKey_validation_class() ? TypeParser.parse(cf_def.key_validation_class) : null;

            // Convert the REGULAR definitions from the input CfDef
            List<ColumnDefinition> defs = ColumnDefinition.fromThrift(cf_def.keyspace, cf_def.name, rawComparator, subComparator, cf_def.column_metadata);

            // Add the keyAlias if there is one, since that's on CQL metadata that thrift can actually change (for
            // historical reasons)
            boolean hasKeyAlias = cf_def.isSetKey_alias() && keyValidator != null && !(keyValidator instanceof CompositeType);
            if (hasKeyAlias)
                defs.add(ColumnDefinition.partitionKeyDef(cf_def.keyspace, cf_def.name, cf_def.key_alias, keyValidator, null));

            // Now add any CQL metadata that we want to copy, skipping the keyAlias if there was one
            for (ColumnDefinition def : previousCQLMetadata)
            {
                // isPartOfCellName basically means 'is not just a CQL metadata'
                if (def.isPartOfCellName())
                    continue;

                if (def.kind == ColumnDefinition.Kind.PARTITION_KEY && hasKeyAlias)
                    continue;

                defs.add(def);
            }

            CellNameType comparator = CellNames.fromAbstractType(fullRawComparator, CFMetaData.isDense(fullRawComparator, defs));

            UUID cfId = Schema.instance.getId(cf_def.keyspace, cf_def.name);
            if (cfId == null)
                cfId = UUIDGen.getTimeUUID();

            CFMetaData newCFMD = new CFMetaData(cf_def.keyspace, cf_def.name, cfType, comparator, cfId,
                                                SystemKeyspace.instance, Schema.instance, ColumnFamilyStoreManager.instance, KeyspaceManager.instance, this, MutationFactory.instance);

            newCFMD.addAllColumnDefinitions(defs);

            if (keyValidator != null)
                newCFMD.keyValidator(keyValidator);
            if (cf_def.isSetGc_grace_seconds())
                newCFMD.gcGraceSeconds(cf_def.gc_grace_seconds);
            if (cf_def.isSetMin_compaction_threshold())
                newCFMD.minCompactionThreshold(cf_def.min_compaction_threshold);
            if (cf_def.isSetMax_compaction_threshold())
                newCFMD.maxCompactionThreshold(cf_def.max_compaction_threshold);
            if (cf_def.isSetCompaction_strategy())
                newCFMD.compactionStrategyClass(CFMetaData.createCompactionStrategy(cf_def.compaction_strategy));
            if (cf_def.isSetCompaction_strategy_options())
                newCFMD.compactionStrategyOptions(new HashMap<>(cf_def.compaction_strategy_options));
            if (cf_def.isSetBloom_filter_fp_chance())
                newCFMD.bloomFilterFpChance(cf_def.bloom_filter_fp_chance);
            if (cf_def.isSetMemtable_flush_period_in_ms())
                newCFMD.memtableFlushPeriod(cf_def.memtable_flush_period_in_ms);
            if (cf_def.isSetCaching() || cf_def.isSetCells_per_row_to_cache())
                newCFMD.caching(CachingOptions.fromThrift(cf_def.caching, cf_def.cells_per_row_to_cache));
            if (cf_def.isSetRead_repair_chance())
                newCFMD.readRepairChance(cf_def.read_repair_chance);
            if (cf_def.isSetDefault_time_to_live())
                newCFMD.defaultTimeToLive(cf_def.default_time_to_live);
            if (cf_def.isSetDclocal_read_repair_chance())
                newCFMD.dcLocalReadRepairChance(cf_def.dclocal_read_repair_chance);
            if (cf_def.isSetMin_index_interval())
                newCFMD.minIndexInterval(cf_def.min_index_interval);
            if (cf_def.isSetMax_index_interval())
                newCFMD.maxIndexInterval(cf_def.max_index_interval);
            if (cf_def.isSetSpeculative_retry())
                newCFMD.speculativeRetry(CFMetaData.SpeculativeRetry.fromString(cf_def.speculative_retry));
            if (cf_def.isSetTriggers())
                newCFMD.triggers(TriggerDefinition.fromThrift(cf_def.triggers, this));

            return newCFMD.comment(cf_def.comment)
                    .defaultValidator(TypeParser.parse(cf_def.default_validation_class))
                    .compressionParameters(CompressionParameters.create(cf_def.compression_options))
                    .rebuild();
        }
        catch (SyntaxException | MarshalException e)
        {
            throw new ConfigurationException(e.getMessage());
        }
    }

    private Map<String, ByteBuffer> convertThriftCqlRow(CqlRow row)
    {
        Map<String, ByteBuffer> m = new HashMap<>();
        for (org.apache.cassandra.thrift.Column column : row.getColumns())
            m.put(UTF8Type.instance.getString(column.bufferForName()), column.value);
        return m;
    }

    /**
     * Create CFMetaData from thrift {@link CqlRow} that contains columns from schema_columnfamilies.
     *
     * @param columnsRes CqlRow containing columns from schema_columnfamilies.
     * @return CFMetaData derived from CqlRow
     */
    public CFMetaData fromThriftCqlRow(CqlRow cf, CqlResult columnsRes)
    {
        UntypedResultSet.Row cfRow = new UntypedResultSet.Row(convertThriftCqlRow(cf));

        List<Map<String, ByteBuffer>> cols = new ArrayList<>(columnsRes.rows.size());
        for (CqlRow row : columnsRes.rows)
            cols.add(convertThriftCqlRow(row));
        UntypedResultSet colsRow = UntypedResultSet.create(cols);

        return fromSchemaNoTriggers(cfRow, colsRow);
    }

    /**
     * Deserialize CF metadata from low-level representation
     *
     * @return Thrift-based metadata deserialized from schema
     */
    public CFMetaData fromSchema(UntypedResultSet.Row result)
    {
        String ksName = result.getString("keyspace_name");
        String cfName = result.getString("columnfamily_name");

        Row serializedColumns = SystemKeyspace.instance.readSchemaRow(SystemKeyspace.SCHEMA_COLUMNS_CF, ksName, cfName);
        CFMetaData cfm = fromSchemaNoTriggers(result, ColumnDefinition.resultify(serializedColumns));

        Row serializedTriggers = SystemKeyspace.instance.readSchemaRow(SystemKeyspace.SCHEMA_TRIGGERS_CF, ksName, cfName);
        addTriggerDefinitionsFromSchema(cfm, serializedTriggers, QueryProcessor.instance);

        return cfm;
    }

    public CFMetaData fromSchema(Row row)
    {
        UntypedResultSet.Row result = QueryProcessor.instance.resultify("SELECT * FROM system.schema_columnfamilies", row).one();
        return fromSchema(result);
    }

    // Package protected for use by tests
    @VisibleForTesting
    CFMetaData fromSchemaNoTriggers(UntypedResultSet.Row result, UntypedResultSet serializedColumnDefinitions)
    {
        try
        {
            String ksName = result.getString("keyspace_name");
            String cfName = result.getString("columnfamily_name");

            AbstractType<?> rawComparator = TypeParser.parse(result.getString("comparator"));
            AbstractType<?> subComparator = result.has("subcomparator") ? TypeParser.parse(result.getString("subcomparator")) : null;
            ColumnFamilyType cfType = ColumnFamilyType.valueOf(result.getString("type"));

            AbstractType<?> fullRawComparator = makeRawAbstractType(rawComparator, subComparator);

            List<ColumnDefinition> columnDefs = ColumnDefinition.fromSchema(serializedColumnDefinitions,
                                                                            ksName,
                                                                            cfName,
                                                                            fullRawComparator,
                                                                            cfType == ColumnFamilyType.Super);

            boolean isDense = result.has("is_dense")
                    ? result.getBoolean("is_dense")
                    : CFMetaData.isDense(fullRawComparator, columnDefs);

            CellNameType comparator = CellNames.fromAbstractType(fullRawComparator, isDense);

            // if we are upgrading, we use id generated from names initially
            UUID cfId = result.has("cf_id")
                    ? result.getUUID("cf_id")
                    : CFMetaData.generateLegacyCfId(ksName, cfName);

            CFMetaData cfm = new CFMetaData(ksName, cfName, cfType, comparator, cfId, SystemKeyspace.instance, Schema.instance,
                                            ColumnFamilyStoreManager.instance, KeyspaceManager.instance, this, MutationFactory.instance);
            cfm.setDense(isDense);

            cfm.readRepairChance(result.getDouble("read_repair_chance"));
            cfm.dcLocalReadRepairChance(result.getDouble("local_read_repair_chance"));
            cfm.gcGraceSeconds(result.getInt("gc_grace_seconds"));
            cfm.defaultValidator(TypeParser.parse(result.getString("default_validator")));
            cfm.keyValidator(TypeParser.parse(result.getString("key_validator")));
            cfm.minCompactionThreshold(result.getInt("min_compaction_threshold"));
            cfm.maxCompactionThreshold(result.getInt("max_compaction_threshold"));
            if (result.has("comment"))
                cfm.comment(result.getString("comment"));
            if (result.has("bloom_filter_fp_chance"))
                cfm.bloomFilterFpChance(result.getDouble("bloom_filter_fp_chance"));
            if (result.has("memtable_flush_period_in_ms"))
                cfm.memtableFlushPeriod(result.getInt("memtable_flush_period_in_ms"));
            cfm.caching(CachingOptions.fromString(result.getString("caching")));
            if (result.has("default_time_to_live"))
                cfm.defaultTimeToLive(result.getInt("default_time_to_live"));
            if (result.has("speculative_retry"))
                cfm.speculativeRetry(CFMetaData.SpeculativeRetry.fromString(result.getString("speculative_retry")));
            cfm.compactionStrategyClass(CFMetaData.createCompactionStrategy(result.getString("compaction_strategy_class")));
            cfm.compressionParameters(CompressionParameters.create(FBUtilities.fromJsonMap(result.getString("compression_parameters"))));
            cfm.compactionStrategyOptions(FBUtilities.fromJsonMap(result.getString("compaction_strategy_options")));

            // migrate old index_interval values to min_index_interval, if present
            if (result.has("min_index_interval"))
                cfm.minIndexInterval(result.getInt("min_index_interval"));
            else if (result.has("index_interval"))
                cfm.minIndexInterval(result.getInt("index_interval"));
            if (result.has("max_index_interval"))
                cfm.maxIndexInterval(result.getInt("max_index_interval"));

            /*
             * The info previously hold by key_aliases, column_aliases and value_alias is now stored in columnMetadata (because 1) this
             * make more sense and 2) this allow to store indexing information).
             * However, for upgrade sake we need to still be able to read those old values. Moreover, we cannot easily
             * remove those old columns once "converted" to columnMetadata because that would screw up nodes that may
             * not have upgraded. So for now we keep the both info and in sync, even though its redundant.
             */
            if (result.has("key_aliases"))
                cfm.addColumnMetadataFromAliases(aliasesFromStrings(FBUtilities.fromJsonList(result.getString("key_aliases"))), cfm.getKeyValidator(), ColumnDefinition.Kind.PARTITION_KEY);
            if (result.has("column_aliases"))
                cfm.addColumnMetadataFromAliases(aliasesFromStrings(FBUtilities.fromJsonList(result.getString("column_aliases"))), cfm.comparator.asAbstractType(), ColumnDefinition.Kind.CLUSTERING_COLUMN);
            if (result.has("value_alias"))
                cfm.addColumnMetadataFromAliases(Collections.singletonList(result.getBytes("value_alias")), cfm.getDefaultValidator(), ColumnDefinition.Kind.COMPACT_VALUE);

            if (result.has("dropped_columns"))
                cfm.droppedColumns(convertDroppedColumns(result.getMap("dropped_columns", UTF8Type.instance, LongType.instance)));

            for (ColumnDefinition cd : columnDefs)
                cfm.addOrReplaceColumnDefinition(cd);

            return cfm.rebuild();
        }
        catch (SyntaxException | ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    static AbstractType<?> makeRawAbstractType(AbstractType<?> comparator, AbstractType<?> subComparator)
    {
        return subComparator == null ? comparator : CompositeType.getInstance(Arrays.asList(comparator, subComparator));
    }

    private static List<ByteBuffer> aliasesFromStrings(List<String> aliases)
    {
        List<ByteBuffer> rawAliases = new ArrayList<>(aliases.size());
        for (String alias : aliases)
            rawAliases.add(UTF8Type.instance.decompose(alias));
        return rawAliases;
    }

    private static Map<ColumnIdentifier, Long> convertDroppedColumns(Map<String, Long> raw)
    {
        Map<ColumnIdentifier, Long> converted = Maps.newHashMap();
        for (Map.Entry<String, Long> entry : raw.entrySet())
            converted.put(new ColumnIdentifier(entry.getKey(), true), entry.getValue());
        return converted;
    }

    void addTriggerDefinitionsFromSchema(CFMetaData cfDef, Row serializedTriggerDefinitions, QueryProcessor queryProcessor)
    {
        for (TriggerDefinition td : TriggerDefinition.fromSchema(serializedTriggerDefinitions, queryProcessor, this))
            cfDef.getTriggers().put(td.name, td);
    }

}
