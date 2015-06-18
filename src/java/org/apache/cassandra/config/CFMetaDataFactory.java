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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.TableAttributes;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.SpeculativeRetryParam;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

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

    public Builder createBuilder(String keyspace, String table)
    {
        return createBuilder(keyspace, table, false, true, false);
    }

    public Builder createBuilder(String keyspace, String table, boolean isDense, boolean isCompound, boolean isCounter)
    {
        return createBuilder(keyspace, table, isDense, isCompound, false, isCounter);
    }

    public Builder createBuilder(String keyspace, String table, boolean isDense, boolean isCompound, boolean isSuper, boolean isCounter)
    {
        return new Builder(keyspace, table, isDense, isCompound, isSuper, isCounter, false, DatabaseDescriptor.getPartitioner());
    }

    public Builder createViewBuilder(String keyspace, String table)
    {
        return new Builder(keyspace, table, false, true, false, false, true, DatabaseDescriptor.getPartitioner());
    }

    public Builder createDenseBuilder(String keyspace, String table, boolean isCompound, boolean isCounter)
    {
        return createBuilder(keyspace, table, true, isCompound, isCounter);
    }

    public Builder createSuperBuilder(String keyspace, String table, boolean isCounter)
    {
        return createBuilder(keyspace, table, false, false, true, isCounter);
    }

    public final static class Builder
    {
        private final String keyspace;
        private final String table;
        private final boolean isDense;
        private final boolean isCompound;
        private final boolean isSuper;
        private final boolean isCounter;
        private final boolean isMaterializedView;
        private IPartitioner partitioner;

        private UUID tableId;

        private final List<Pair<ColumnIdentifier, AbstractType>> partitionKeys = new ArrayList<>();
        private final List<Pair<ColumnIdentifier, AbstractType>> clusteringColumns = new ArrayList<>();
        private final List<Pair<ColumnIdentifier, AbstractType>> staticColumns = new ArrayList<>();
        private final List<Pair<ColumnIdentifier, AbstractType>> regularColumns = new ArrayList<>();

        private Builder(String keyspace, String table, boolean isDense, boolean isCompound, boolean isSuper,
                        boolean isCounter, boolean isMaterializedView, IPartitioner partitioner)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.isDense = isDense;
            this.isCompound = isCompound;
            this.isSuper = isSuper;
            this.isCounter = isCounter;
            this.isMaterializedView = isMaterializedView;
            this.partitioner = partitioner;
        }

        public Builder withId(UUID tableId)
        {
            this.tableId = tableId;
            return this;
        }

        public Builder withPartitioner(IPartitioner partitioner)
        {
            this.partitioner = partitioner;
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
                clusterings.add(new ColumnDefinition(keyspace, table, p.left, p.right, i, ColumnDefinition.Kind.CLUSTERING));
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
                                          isMaterializedView,
                                          partitions,
                                          clusterings,
                                          builder.build(),
                                          partitioner);
        }
    }

    public CFMetaData newCFMetaData(String keyspace,
                                    String name, UUID cfId,
                                    boolean isSuper,
                                    boolean isCounter,
                                    boolean isDense,
                                    boolean isCompound,
                                    boolean isMaterializedView,
                                    List<ColumnDefinition> partitionKeyColumns,
                                    List<ColumnDefinition> clusteringColumns,
                                    PartitionColumns partitionColumns,
                                    IPartitioner partitioner)
    {
        return new CFMetaData(keyspace,
                              name,
                              cfId,
                              isSuper,
                              isCounter,
                              isDense,
                              isCompound,
                              isMaterializedView,
                              partitionKeyColumns,
                              clusteringColumns,
                              partitionColumns,
                              partitioner);
    }

    public TableAttributes newTableAttributes()
    {
        return new TableAttributes();
    }

    public void addTableParamsToSchemaMutation(TableParams params, RowUpdateBuilder adder)
    {
        adder.add("bloom_filter_fp_chance", params.bloomFilterFpChance)
             .add("comment", params.comment)
             .add("dclocal_read_repair_chance", params.dcLocalReadRepairChance)
             .add("default_time_to_live", params.defaultTimeToLive)
             .add("gc_grace_seconds", params.gcGraceSeconds)
             .add("max_index_interval", params.maxIndexInterval)
             .add("memtable_flush_period_in_ms", params.memtableFlushPeriodInMs)
             .add("min_index_interval", params.minIndexInterval)
             .add("read_repair_chance", params.readRepairChance)
             .add("speculative_retry", params.speculativeRetry.toString())
             .map("caching", params.caching.asMap())
             .map("compaction", params.compaction.asMap())
             .map("compression", params.compression.asMap());
    }

    public TableParams createTableParamsFromRow(UntypedResultSet.Row row)
    {
        return TableParams.builder()
                          .bloomFilterFpChance(row.getDouble("bloom_filter_fp_chance"))
                          .caching(CachingParams.fromMap(row.getTextMap("caching")))
                          .comment(row.getString("comment"))
                          .compaction(CompactionParams.fromMap(row.getTextMap("compaction")))
                          .compression(CompressionParams.fromMap(row.getTextMap("compression")))
                          .dcLocalReadRepairChance(row.getDouble("dclocal_read_repair_chance"))
                          .defaultTimeToLive(row.getInt("default_time_to_live"))
                          .gcGraceSeconds(row.getInt("gc_grace_seconds"))
                          .maxIndexInterval(row.getInt("max_index_interval"))
                          .memtableFlushPeriodInMs(row.getInt("memtable_flush_period_in_ms"))
                          .minIndexInterval(row.getInt("min_index_interval"))
                          .readRepairChance(row.getDouble("read_repair_chance"))
                          .speculativeRetry(SpeculativeRetryParam.fromString(row.getString("speculative_retry")))
                          .build();
    }
}
