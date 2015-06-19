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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.CFPropDefs;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.LegacySchemaTables;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Adds ddl/schema support for nachos
 */
public class NachoAwareCFMetaDataFactory extends CFMetaDataFactory
{
    public static final String KW_NACHOS = "nachos";  // the keyword to be used with the 'WITH' statement
    public static final String COLUMN = "nacho_strategy";  // the schema column the data will be saved in, and the cqlsh name
    public static final String QUESO_FACTOR = "queso_factor";
    public static final String TORTILLA_SOMBRERO = "tortilla_sombrero";

    private static final Set<String> VALID_OPTS = ImmutableSet.<String>builder().add(QUESO_FACTOR).add(TORTILLA_SOMBRERO).build();

    static
    {
        CFPropDefs.keywords.add(KW_NACHOS);
    }

    @Override
    public CFMetaData newCFMetaData(String keyspace, String name, UUID cfId, boolean isSuper, boolean isCounter, boolean isDense, boolean isCompound, List<ColumnDefinition> partitionKeyColumns, List<ColumnDefinition> clusteringColumns, PartitionColumns partitionColumns)
    {
        CFMetaData cfm = new NachoCFMetaData(keyspace, name, cfId, isSuper, isCounter, isDense, isCompound, partitionKeyColumns, clusteringColumns, partitionColumns);

        // add field to table schema table
        if (keyspace.equals(SystemKeyspace.NAME) && name.equals(LegacySchemaTables.COLUMNFAMILIES))
        {
            ColumnDefinition def = ColumnDefinition.regularDef(cfm, UTF8Type.instance.fromString(COLUMN), UTF8Type.instance, 0);
            cfm.addColumnDefinition(def);
            cfm.rebuild();
        }

        return cfm;
    }

    @Override
    public void populateSchemaRowUpdate(RowUpdateBuilder adder, CFMetaData table, long timestamp, boolean withColumnsAndTriggers, Mutation mutation)
    {
        super.populateSchemaRowUpdate(adder, table, timestamp, withColumnsAndTriggers, mutation);
        if (table instanceof NachoCFMetaData)
        {
            NachoCFMetaData ncfm = (NachoCFMetaData) table;
            if (ncfm.isNachoMode())
            {
                Map<String, String> nachOptions = new HashMap<>();
                nachOptions.put(QUESO_FACTOR, Integer.toString(Math.max(1, ncfm.getNachoQuesoFactor())));
                nachOptions.put(TORTILLA_SOMBRERO, Boolean.toString(ncfm.isTortillaSombrero()));
                adder.add(COLUMN, FBUtilities.json(nachOptions));
            }
            else
            {
                adder.delete(COLUMN);
            }
        }
    }

    @Override
    public CFMetaData createTableFromTableRowAndColumnRows(UntypedResultSet.Row result, UntypedResultSet serializedColumnDefinitions)
    {
        NachoCFMetaData cfm = (NachoCFMetaData) super.createTableFromTableRowAndColumnRows(result, serializedColumnDefinitions);
        if (result.has(COLUMN))
        {
            Map<String, String> nachOptions = FBUtilities.fromJsonMap(result.getString(COLUMN));
            cfm.setNachoMode(true);
            cfm.setNachoQuesoFactor(nachOptions.containsKey(QUESO_FACTOR) ? Integer.parseInt(nachOptions.get(QUESO_FACTOR)) : 1);
            cfm.setTortillaSombrero(nachOptions.containsKey(TORTILLA_SOMBRERO) && Boolean.parseBoolean(nachOptions.get(TORTILLA_SOMBRERO)));
        }
        return cfm;
    }

    public static class NachoCFProps extends CFPropDefs
    {
        @Override
        public void validate() throws ConfigurationException, SyntaxException
        {
            super.validate();
            if (hasProperty(KW_NACHOS))
            {
                validateNachoOptions();
            }
        }

        private void validateNachoOptions()
        {
            Map<String, String> nachOptions = getMap(KW_NACHOS);

            for (String key: nachOptions.keySet())
            {
                if (!VALID_OPTS.contains(key))
                    throw new ConfigurationException("'" + key + "' is  not a valid nacho option");

            }

            if (nachOptions.containsKey(QUESO_FACTOR))
            {
                try
                {
                    int qf = Integer.parseInt(nachOptions.get(QUESO_FACTOR).toLowerCase());

                    if (qf < 1)
                        throw new ConfigurationException(KW_NACHOS + "." + QUESO_FACTOR + " must be >0");
                }
                catch (NumberFormatException e)
                {
                    throw new ConfigurationException("Invalid value for " + KW_NACHOS + "." + QUESO_FACTOR, e);
                }
            }

            if (nachOptions.containsKey(TORTILLA_SOMBRERO))
            {
                if (!Sets.newHashSet("true", "false").contains(nachOptions.get(TORTILLA_SOMBRERO).toLowerCase()))
                    throw new ConfigurationException("Invalid value for " + KW_NACHOS + "." + TORTILLA_SOMBRERO + ", true/false required");

            }
        }

        @Override
        public void applyToCFMetadata(CFMetaData cfm) throws ConfigurationException, SyntaxException
        {
            super.applyToCFMetadata(cfm);

            if (cfm instanceof NachoCFMetaData)
            {
                if (hasProperty(KW_NACHOS))
                {
                    NachoCFMetaData ncfm = (NachoCFMetaData) cfm;
                    Map<String, String> nachOptions = getMap(KW_NACHOS);

                    ncfm.setNachoMode(true);
                    ncfm.setNachoQuesoFactor(nachOptions.containsKey(QUESO_FACTOR) ? Integer.parseInt(nachOptions.get(QUESO_FACTOR)) : 1);
                    ncfm.setTortillaSombrero(nachOptions.containsKey(TORTILLA_SOMBRERO) && Boolean.parseBoolean(nachOptions.get(TORTILLA_SOMBRERO).toLowerCase()));
                }
            }
            else
            {
                assert !hasProperty(KW_NACHOS);
            }
        }
    }

    @Override
    public CFPropDefs newPropDefs()
    {
        return new NachoCFProps();
    }
}

