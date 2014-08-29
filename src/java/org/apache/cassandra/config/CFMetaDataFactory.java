package org.apache.cassandra.config;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.SystemKeyspace;
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
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class CFMetaDataFactory
{
    private static final Logger logger = LoggerFactory.getLogger(CFMetaData.class);

    public static final CFMetaDataFactory instance = new CFMetaDataFactory();

    public CFMetaData denseCFMetaData(String keyspace, String name, AbstractType<?> comp, AbstractType<?> subcc)
    {
        CellNameType cellNameType = CellNames.fromAbstractType(CFMetaData.makeRawAbstractType(comp, subcc), true);
        return new CFMetaData(keyspace, name, subcc == null ? ColumnFamilyType.Standard : ColumnFamilyType.Super, cellNameType);
    }

    public CFMetaData sparseCFMetaData(String keyspace, String name, AbstractType<?> comp)
    {
        CellNameType cellNameType = CellNames.fromAbstractType(comp, false);
        return new CFMetaData(keyspace, name, ColumnFamilyType.Standard, cellNameType);
    }

    public CFMetaData denseCFMetaData(String keyspace, String name, AbstractType<?> comp)
    {
        return denseCFMetaData(keyspace, name, comp, null);
    }

    public static CFMetaData compile(String cql, QueryProcessor queryProcessor)
    {
        return compile(cql, Keyspace.SYSTEM_KS, queryProcessor);
    }

    public static CFMetaData compile(String cql, String keyspace, QueryProcessor queryProcessor)
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

    private static CFMetaData newSystemMetadata(String keyspace, String cfName, String comment, CellNameType comparator)
    {
        return new CFMetaData(keyspace, cfName, ColumnFamilyType.Standard, comparator, CFMetaData.generateLegacyCfId(keyspace, cfName))
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

        return new CFMetaData(parent.ksName, parent.indexColumnFamilyName(info), ColumnFamilyType.Standard, indexComparator, parent.cfId)
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

            AbstractType<?> fullRawComparator = CFMetaData.makeRawAbstractType(rawComparator, subComparator);

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

            CFMetaData newCFMD = new CFMetaData(cf_def.keyspace, cf_def.name, cfType, comparator, cfId);

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
                newCFMD.triggers(TriggerDefinition.fromThrift(cf_def.triggers));

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

        return CFMetaData.fromSchemaNoTriggers(cfRow, colsRow);
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
        CFMetaData cfm = CFMetaData.fromSchemaNoTriggers(result, ColumnDefinition.resultify(serializedColumns));

        Row serializedTriggers = SystemKeyspace.instance.readSchemaRow(SystemKeyspace.SCHEMA_TRIGGERS_CF, ksName, cfName);
        CFMetaData.addTriggerDefinitionsFromSchema(cfm, serializedTriggers, QueryProcessor.instance);

        return cfm;
    }

    public CFMetaData fromSchema(Row row)
    {
        UntypedResultSet.Row result = QueryProcessor.instance.resultify("SELECT * FROM system.schema_columnfamilies", row).one();
        return fromSchema(result);
    }

}
