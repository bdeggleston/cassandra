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

package org.apache.cassandra.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.CFPropDefs;
import org.apache.cassandra.db.CompactTables;
import org.apache.cassandra.db.LegacyLayout;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

/**
 * Created by beggleston on 6/18/15.
 */
public class CFMetaDataFactory
{
    public static final CFMetaDataFactory instance;
    static
    {
        String className = System.getProperty("cassandra.cfmetadatafactory", CFMetaDataFactory.class.getName());
        CFMetaDataFactory factory;
        try
        {
            factory = (CFMetaDataFactory) FBUtilities.classForName(className, "CFMetaDataFactory").newInstance();
        }
        catch (InstantiationException | IllegalAccessException e)
        {
            throw new IllegalArgumentException(e);
        }
        instance = factory;
    }

    public Builder createBuilder(String keyspace, String table, boolean isDense, boolean isCompound, boolean isSuper, boolean isCounter)
    {
        return new Builder(keyspace, table, isDense, isCompound, isSuper, isCounter);
    }

    public final Builder createBuilder(String keyspace, String table)
    {
        return createBuilder(keyspace, table, false, true, false);
    }

    public final Builder createBuilder(String keyspace, String table, boolean isDense, boolean isCompound, boolean isCounter)
    {
        return createBuilder(keyspace, table, isDense, isCompound, false, isCounter);
    }

    public final Builder createDenseBuilder(String keyspace, String table, boolean isCompound, boolean isCounter)
    {
        return createBuilder(keyspace, table, true, isCompound, isCounter);
    }

    public final static class Builder
    {
        private final String keyspace;
        private final String table;
        private final boolean isDense;
        private final boolean isCompound;
        private final boolean isSuper;
        private final boolean isCounter;

        private UUID tableId;

        private final List<Pair<ColumnIdentifier, AbstractType>> partitionKeys = new ArrayList<>();
        private final List<Pair<ColumnIdentifier, AbstractType>> clusteringColumns = new ArrayList<>();
        private final List<Pair<ColumnIdentifier, AbstractType>> staticColumns = new ArrayList<>();
        private final List<Pair<ColumnIdentifier, AbstractType>> regularColumns = new ArrayList<>();

        private Builder(String keyspace, String table, boolean isDense, boolean isCompound, boolean isSuper, boolean isCounter)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.isDense = isDense;
            this.isCompound = isCompound;
            this.isSuper = isSuper;
            this.isCounter = isCounter;
        }

        public Builder withId(UUID tableId)
        {
            this.tableId = tableId;
            return this;
        }

        public Builder addPartitionKey(String name, AbstractType type)
        {
            return addPartitionKey(ColumnIdentifier.getInterned(name, false), type);
        }

        public Builder addPartitionKey(ColumnIdentifier name, AbstractType type)
        {
            this.partitionKeys.add(Pair.create(name, type));
            return this;
        }

        public Builder addClusteringColumn(String name, AbstractType type)
        {
            return addClusteringColumn(ColumnIdentifier.getInterned(name, false), type);
        }

        public Builder addClusteringColumn(ColumnIdentifier name, AbstractType type)
        {
            this.clusteringColumns.add(Pair.create(name, type));
            return this;
        }

        public Builder addRegularColumn(String name, AbstractType type)
        {
            return addRegularColumn(ColumnIdentifier.getInterned(name, false), type);
        }

        public Builder addRegularColumn(ColumnIdentifier name, AbstractType type)
        {
            this.regularColumns.add(Pair.create(name, type));
            return this;
        }

        public boolean hasRegulars()
        {
            return !this.regularColumns.isEmpty();
        }

        public Builder addStaticColumn(String name, AbstractType type)
        {
            return addStaticColumn(ColumnIdentifier.getInterned(name, false), type);
        }

        public Builder addStaticColumn(ColumnIdentifier name, AbstractType type)
        {
            this.staticColumns.add(Pair.create(name, type));
            return this;
        }

        public Set<String> usedColumnNames()
        {
            Set<String> usedNames = new HashSet<>();
            for (Pair<ColumnIdentifier, AbstractType> p : partitionKeys)
                usedNames.add(p.left.toString());
            for (Pair<ColumnIdentifier, AbstractType> p : clusteringColumns)
                usedNames.add(p.left.toString());
            for (Pair<ColumnIdentifier, AbstractType> p : staticColumns)
                usedNames.add(p.left.toString());
            for (Pair<ColumnIdentifier, AbstractType> p : regularColumns)
                usedNames.add(p.left.toString());
            return usedNames;
        }

        public CFMetaData build()
        {
            if (tableId == null)
                tableId = UUIDGen.getTimeUUID();

            List<ColumnDefinition> partitions = new ArrayList<>(partitionKeys.size());
            List<ColumnDefinition> clusterings = new ArrayList<>(clusteringColumns.size());
            PartitionColumns.Builder builder = PartitionColumns.builder();

            for (int i = 0; i < partitionKeys.size(); i++)
            {
                Pair<ColumnIdentifier, AbstractType> p = partitionKeys.get(i);
                Integer componentIndex = partitionKeys.size() == 1 ? null : i;
                partitions.add(new ColumnDefinition(keyspace, table, p.left, p.right, componentIndex, ColumnDefinition.Kind.PARTITION_KEY));
            }

            for (int i = 0; i < clusteringColumns.size(); i++)
            {
                Pair<ColumnIdentifier, AbstractType> p = clusteringColumns.get(i);
                clusterings.add(new ColumnDefinition(keyspace, table, p.left, p.right, i, ColumnDefinition.Kind.CLUSTERING_COLUMN));
            }

            for (int i = 0; i < regularColumns.size(); i++)
            {
                Pair<ColumnIdentifier, AbstractType> p = regularColumns.get(i);
                builder.add(new ColumnDefinition(keyspace, table, p.left, p.right, null, ColumnDefinition.Kind.REGULAR));
            }

            for (int i = 0; i < staticColumns.size(); i++)
            {
                Pair<ColumnIdentifier, AbstractType> p = staticColumns.get(i);
                builder.add(new ColumnDefinition(keyspace, table, p.left, p.right, null, ColumnDefinition.Kind.STATIC));
            }

            return instance.newCFMetaData(keyspace,
                                          table,
                                          tableId,
                                          isSuper,
                                          isCounter,
                                          isDense,
                                          isCompound,
                                          partitions,
                                          clusterings,
                                          builder.build());
        }
    }

    public CFMetaData newCFMetaData(String keyspace,
                                    String name, UUID cfId,
                                    boolean isSuper,
                                    boolean isCounter,
                                    boolean isDense,
                                    boolean isCompound,
                                    List<ColumnDefinition> partitionKeyColumns,
                                    List<ColumnDefinition> clusteringColumns,
                                    PartitionColumns partitionColumns)
    {
        return new CFMetaData(keyspace,
                              name,
                              cfId,
                              isSuper,
                              isCounter,
                              isDense,
                              isCompound,
                              partitionKeyColumns,
                              clusteringColumns,
                              partitionColumns);
    }

    public CFPropDefs newPropDefs()
    {
        return new CFPropDefs();
    }

    public void populateSchemaRowUpdate(RowUpdateBuilder adder, CFMetaData table, long timestamp, boolean withColumnsAndTriggers, Mutation mutation)
    {
        adder.add("cf_id", table.cfId);
        adder.add("type", table.isSuper() ? "Super" : "Standard");

        if (table.isSuper())
        {
            // We need to continue saving the comparator and subcomparator separatly, otherwise
            // we won't know at deserialization if the subcomparator should be taken into account
            // TODO: we should implement an on-start migration if we want to get rid of that.
            adder.add("comparator", table.comparator.subtype(0).toString());
            adder.add("subcomparator", ((MapType)table.compactValueColumn().type).getKeysType().toString());
        }
        else
        {
            adder.add("comparator", LegacyLayout.makeLegacyComparator(table).toString());
        }

        adder.add("bloom_filter_fp_chance", table.getBloomFilterFpChance());
        adder.add("caching", table.getCaching().toString());
        adder.add("comment", table.getComment());
        adder.add("compaction_strategy_class", table.compactionStrategyClass.getName());
        adder.add("compaction_strategy_options", FBUtilities.json(table.compactionStrategyOptions));
        adder.add("compression_parameters", FBUtilities.json(table.compressionParameters.asThriftOptions()));
        adder.add("default_time_to_live", table.getDefaultTimeToLive());
        adder.add("gc_grace_seconds", table.getGcGraceSeconds());
        adder.add("key_validator", table.getKeyValidator().toString());
        adder.add("local_read_repair_chance", table.getDcLocalReadRepairChance());
        adder.add("max_compaction_threshold", table.getMaxCompactionThreshold());
        adder.add("max_index_interval", table.getMaxIndexInterval());
        adder.add("memtable_flush_period_in_ms", table.getMemtableFlushPeriod());
        adder.add("min_compaction_threshold", table.getMinCompactionThreshold());
        adder.add("min_index_interval", table.getMinIndexInterval());
        adder.add("read_repair_chance", table.getReadRepairChance());
        adder.add("speculative_retry", table.getSpeculativeRetry().toString());

        for (Map.Entry<ColumnIdentifier, CFMetaData.DroppedColumn> entry : table.getDroppedColumns().entrySet())
        {
            String name = entry.getKey().toString();
            CFMetaData.DroppedColumn column = entry.getValue();
            adder.addMapEntry("dropped_columns", name, column.droppedTime);
            if (column.type != null)
                adder.addMapEntry("dropped_columns_types", name, column.type.toString());
        }

        adder.add("is_dense", table.isDense());

        adder.add("default_validator", table.makeLegacyDefaultValidator().toString());

    }

    public void populateColumnRowUpdate(RowUpdateBuilder adder, CFMetaData table, ColumnDefinition column, long timestamp, Mutation mutation)
    {

        adder.add("validator", column.type.toString());
        adder.add("type", serializeKind(column.kind, table.isDense()));
        adder.add("component_index", column.isOnAllComponents() ? null : column.position());
        adder.add("index_name", column.getIndexName());
        adder.add("index_type", column.getIndexType() == null ? null : column.getIndexType().toString());
        adder.add("index_options", FBUtilities.json(column.getIndexOptions()));
    }

    public CFMetaData createTableFromTableRowAndColumnRows(UntypedResultSet.Row result, UntypedResultSet serializedColumnDefinitions)
    {
        String ksName = result.getString("keyspace_name");
        String cfName = result.getString("columnfamily_name");

        AbstractType<?> rawComparator = TypeParser.parse(result.getString("comparator"));
        AbstractType<?> subComparator = result.has("subcomparator") ? TypeParser.parse(result.getString("subcomparator")) : null;

        boolean isSuper = result.getString("type").toLowerCase().equals("super");
        boolean isDense = result.getBoolean("is_dense");
        boolean isCompound = rawComparator instanceof CompositeType;

        // We don't really use the default validator but as we have it for backward compatibility, we use it to know if it's a counter table
        AbstractType<?> defaultValidator = TypeParser.parse(result.getString("default_validator"));
        boolean isCounter =  defaultValidator instanceof CounterColumnType;

        // if we are upgrading, we use id generated from names initially
        UUID cfId = result.has("cf_id")
                    ? result.getUUID("cf_id")
                    : CFMetaData.generateLegacyCfId(ksName, cfName);

        boolean isCQLTable = !isSuper && !isDense && isCompound;
        boolean isStaticCompactTable = !isDense && !isCompound;

        // Internally, compact tables have a specific layout, see CompactTables. But when upgrading from
        // previous versions, they may not have the expected schema, so detect if we need to upgrade and do
        // it in createColumnsFromColumnRows.
        // We can remove this once we don't support upgrade from versions < 3.0.
        boolean needsUpgrade = isCQLTable ? false : checkNeedsUpgrade(serializedColumnDefinitions, isSuper, isStaticCompactTable);

        List<ColumnDefinition> columnDefs = createColumnsFromColumnRows(serializedColumnDefinitions,
                                                                        ksName,
                                                                        cfName,
                                                                        rawComparator,
                                                                        subComparator,
                                                                        isSuper,
                                                                        isCQLTable,
                                                                        isStaticCompactTable,
                                                                        needsUpgrade);

        if (needsUpgrade)
            addDefinitionForUpgrade(columnDefs, ksName, cfName, isStaticCompactTable, isSuper, rawComparator, subComparator, defaultValidator);

        CFMetaData cfm = CFMetaData.create(ksName, cfName, cfId, isDense, isCompound, isSuper, isCounter, columnDefs);

        cfm.readRepairChance(result.getDouble("read_repair_chance"));
        cfm.dcLocalReadRepairChance(result.getDouble("local_read_repair_chance"));
        cfm.gcGraceSeconds(result.getInt("gc_grace_seconds"));
        cfm.minCompactionThreshold(result.getInt("min_compaction_threshold"));
        cfm.maxCompactionThreshold(result.getInt("max_compaction_threshold"));
        if (result.has("comment"))
            cfm.comment(result.getString("comment"));
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

        if (result.has("min_index_interval"))
            cfm.minIndexInterval(result.getInt("min_index_interval"));

        if (result.has("max_index_interval"))
            cfm.maxIndexInterval(result.getInt("max_index_interval"));

        if (result.has("bloom_filter_fp_chance"))
            cfm.bloomFilterFpChance(result.getDouble("bloom_filter_fp_chance"));
        else
            cfm.bloomFilterFpChance(cfm.getBloomFilterFpChance());

        if (result.has("dropped_columns"))
        {
            Map<String, String> types = result.has("dropped_columns_types")
                                        ? result.getMap("dropped_columns_types", UTF8Type.instance, UTF8Type.instance)
                                        : Collections.<String, String>emptyMap();
            addDroppedColumns(cfm, result.getMap("dropped_columns", UTF8Type.instance, LongType.instance), types);
        }

        return cfm;
    }

    private List<ColumnDefinition> createColumnsFromColumnRows(UntypedResultSet rows,
                                                                      String keyspace,
                                                                      String table,
                                                                      AbstractType<?> rawComparator,
                                                                      AbstractType<?> rawSubComparator,
                                                                      boolean isSuper,
                                                                      boolean isCQLTable,
                                                                      boolean isStaticCompactTable,
                                                                      boolean needsUpgrade)
    {
        List<ColumnDefinition> columns = new ArrayList<>();
        for (UntypedResultSet.Row row : rows)
            columns.add(createColumnFromColumnRow(row, keyspace, table, rawComparator, rawSubComparator, isSuper, isCQLTable, isStaticCompactTable, needsUpgrade));
        return columns;
    }

    private ColumnDefinition createColumnFromColumnRow(UntypedResultSet.Row row,
                                                       String keyspace,
                                                       String table,
                                                       AbstractType<?> rawComparator,
                                                       AbstractType<?> rawSubComparator,
                                                       boolean isSuper,
                                                       boolean isCQLTable,
                                                       boolean isStaticCompactTable,
                                                       boolean needsUpgrade)
    {
        ColumnDefinition.Kind kind = deserializeKind(row.getString("type"));
        if (needsUpgrade && isStaticCompactTable && kind == ColumnDefinition.Kind.REGULAR)
            kind = ColumnDefinition.Kind.STATIC;

        Integer componentIndex = null;
        if (row.has("component_index"))
            componentIndex = row.getInt("component_index");

        // Note: we save the column name as string, but we should not assume that it is an UTF8 name, we
        // we need to use the comparator fromString method
        AbstractType<?> comparator = isCQLTable
                                     ? UTF8Type.instance
                                     : CompactTables.columnDefinitionComparator(kind, isSuper, rawComparator, rawSubComparator);
        ColumnIdentifier name = ColumnIdentifier.getInterned(comparator.fromString(row.getString("column_name")), comparator);

        AbstractType<?> validator = parseType(row.getString("validator"));

        IndexType indexType = null;
        if (row.has("index_type"))
            indexType = IndexType.valueOf(row.getString("index_type"));

        Map<String, String> indexOptions = null;
        if (row.has("index_options"))
            indexOptions = FBUtilities.fromJsonMap(row.getString("index_options"));

        String indexName = null;
        if (row.has("index_name"))
            indexName = row.getString("index_name");

        return new ColumnDefinition(keyspace, table, name, validator, indexType, indexOptions, indexName, componentIndex, kind);
    }

    // Should only be called on compact tables
    private static boolean checkNeedsUpgrade(UntypedResultSet defs, boolean isSuper, boolean isStaticCompactTable)
    {
        if (isSuper)
        {
            // Check if we've added the "supercolumn map" column yet or not
            for (UntypedResultSet.Row row : defs)
            {
                if (row.getString("column_name").isEmpty())
                    return false;
            }
            return true;
        }

        // For static compact tables, we need to upgrade if the regular definitions haven't been converted to static yet,
        // i.e. if we don't have a static definition yet.
        if (isStaticCompactTable)
            return !hasKind(defs, ColumnDefinition.Kind.STATIC);

        // For dense compact tables, we need to upgrade if we don't have a compact value definition
        return !hasKind(defs, ColumnDefinition.Kind.REGULAR);
    }

    private static void addDefinitionForUpgrade(List<ColumnDefinition> defs,
                                                String ksName,
                                                String cfName,
                                                boolean isStaticCompactTable,
                                                boolean isSuper,
                                                AbstractType<?> rawComparator,
                                                AbstractType<?> subComparator,
                                                AbstractType<?> defaultValidator)
    {
        CompactTables.DefaultNames names = CompactTables.defaultNameGenerator(defs);

        if (isSuper)
        {
            defs.add(ColumnDefinition.regularDef(ksName, cfName, CompactTables.SUPER_COLUMN_MAP_COLUMN_STR, MapType.getInstance(subComparator, defaultValidator, true), null));
        }
        else if (isStaticCompactTable)
        {
            defs.add(ColumnDefinition.clusteringKeyDef(ksName, cfName, names.defaultClusteringName(), rawComparator, null));
            defs.add(ColumnDefinition.regularDef(ksName, cfName, names.defaultCompactValueName(), defaultValidator, null));
        }
        else
        {
            // For dense compact tables, we get here if we don't have a compact value column, in which case we should add it
            // (we use EmptyType to recognize that the compact value was not declared by the use (see CreateTableStatement too))
            defs.add(ColumnDefinition.regularDef(ksName, cfName, names.defaultCompactValueName(), EmptyType.instance, null));
        }
    }

    private static void addDroppedColumns(CFMetaData cfm, Map<String, Long> droppedTimes, Map<String, String> types)
    {
        for (Map.Entry<String, Long> entry : droppedTimes.entrySet())
        {
            String name = entry.getKey();
            long time = entry.getValue();
            AbstractType<?> type = types.containsKey(name) ? TypeParser.parse(types.get(name)) : null;
            cfm.getDroppedColumns().put(ColumnIdentifier.getInterned(name, true), new CFMetaData.DroppedColumn(type, time));
        }
    }

    private static AbstractType<?> parseType(String str)
    {
        return TypeParser.parse(str);
    }

    private static boolean hasKind(UntypedResultSet defs, ColumnDefinition.Kind kind)
    {
        for (UntypedResultSet.Row row : defs)
        {
            if (deserializeKind(row.getString("type")) == kind)
                return true;
        }
        return false;
    }

    public static String serializeKind(ColumnDefinition.Kind kind, boolean isDense)
    {
        // For backward compatibility, we special case CLUSTERING_COLUMN and the case where the table is dense.
        if (kind == ColumnDefinition.Kind.CLUSTERING_COLUMN)
            return "clustering_key";

        if (kind == ColumnDefinition.Kind.REGULAR && isDense)
            return "compact_value";

        return kind.toString().toLowerCase();
    }

    public static ColumnDefinition.Kind deserializeKind(String kind)
    {
        if (kind.equalsIgnoreCase("clustering_key"))
            return ColumnDefinition.Kind.CLUSTERING_COLUMN;
        if (kind.equalsIgnoreCase("compact_value"))
            return ColumnDefinition.Kind.REGULAR;
        return Enum.valueOf(ColumnDefinition.Kind.class, kind.toUpperCase());
    }

}
