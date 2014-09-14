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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.cassandra.config.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.functions.Functions;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * SCHEMA_{KEYSPACES, COLUMNFAMILIES, COLUMNS}_CF are used to store Keyspace/ColumnFamily attributes to make schema
 * load/distribution easy, it replaces old mechanism when local migrations where serialized, stored in system.Migrations
 * and used for schema distribution.
 *
 * SCHEMA_KEYSPACES_CF layout:
 *
 * <key (AsciiType)>
 *   ascii => json_serialized_value
 *   ...
 * </key>
 *
 * Where <key> is a name of keyspace e.g. "ks".
 *
 * SCHEMA_COLUMNFAMILIES_CF layout:
 *
 * <key (AsciiType)>
 *     composite(ascii, ascii) => json_serialized_value
 * </key>
 *
 * Where <key> is a name of keyspace e.g. "ks"., first component of the column name is name of the ColumnFamily, last
 * component is the name of the ColumnFamily attribute.
 *
 * SCHEMA_COLUMNS_CF layout:
 *
 * <key (AsciiType)>
 *     composite(ascii, ascii, ascii) => json_serialized value
 * </key>
 *
 * Where <key> is a name of keyspace e.g. "ks".
 *
 * Cell names where made composite to support 3-level nesting which represents following structure:
 * "ColumnFamily name":"column name":"column attribute" => "value"
 *
 * Example of schema (using CLI):
 *
 * schema_keyspaces
 * ----------------
 * RowKey: ks
 *  => (column=durable_writes, value=true, timestamp=1327061028312185000)
 *  => (column=name, value="ks", timestamp=1327061028312185000)
 *  => (column=replication_factor, value=0, timestamp=1327061028312185000)
 *  => (column=strategy_class, value="org.apache.cassandra.locator.NetworkTopologyStrategy", timestamp=1327061028312185000)
 *  => (column=strategy_options, value={"datacenter1":"1"}, timestamp=1327061028312185000)
 *
 * schema_columnfamilies
 * ---------------------
 * RowKey: ks
 *  => (column=cf:bloom_filter_fp_chance, value=0.0, timestamp=1327061105833119000)
 *  => (column=cf:caching, value="NONE", timestamp=1327061105833119000)
 *  => (column=cf:column_type, value="Standard", timestamp=1327061105833119000)
 *  => (column=cf:comment, value="ColumnFamily", timestamp=1327061105833119000)
 *  => (column=cf:default_validation_class, value="org.apache.cassandra.db.marshal.BytesType", timestamp=1327061105833119000)
 *  => (column=cf:gc_grace_seconds, value=864000, timestamp=1327061105833119000)
 *  => (column=cf:id, value=1000, timestamp=1327061105833119000)
 *  => (column=cf:key_alias, value="S0VZ", timestamp=1327061105833119000)
 *  ... part of the output omitted.
 *
 * schema_columns
 * --------------
 * RowKey: ks
 *  => (column=cf:c:index_name, value=null, timestamp=1327061105833119000)
 *  => (column=cf:c:index_options, value=null, timestamp=1327061105833119000)
 *  => (column=cf:c:index_type, value=null, timestamp=1327061105833119000)
 *  => (column=cf:c:name, value="aGVsbG8=", timestamp=1327061105833119000)
 *  => (column=cf:c:validation_class, value="org.apache.cassandra.db.marshal.AsciiType", timestamp=1327061105833119000)
 */
public class DefsTables
{
    private static final Logger logger = LoggerFactory.getLogger(DefsTables.class);

    public static final DefsTables instance = new DefsTables(DatabaseDescriptor.instance);

    private final DatabaseDescriptor databaseDescriptor;

    public DefsTables(DatabaseDescriptor databaseDescriptor)
    {
        this.databaseDescriptor = databaseDescriptor;
    }

    /**
     * Load keyspace definitions for the system keyspace (system.SCHEMA_KEYSPACES_CF)
     *
     * @return Collection of found keyspace definitions
     */
    public Collection<KSMetaData> loadFromKeyspace()
    {
        List<Row> serializedSchema = databaseDescriptor.getSystemKeyspace().serializedSchema(SystemKeyspace.SCHEMA_KEYSPACES_CF);

        List<KSMetaData> keyspaces = new ArrayList<KSMetaData>(serializedSchema.size());

        for (Row row : serializedSchema)
        {
            if (Schema.invalidSchemaRow(row) || Schema.ignoredSchemaRow(row))
                continue;

            keyspaces.add(databaseDescriptor.getKSMetaDataFactory().fromSchema(row, serializedColumnFamilies(row.key), serializedUserTypes(row.key)));
        }

        return keyspaces;
    }

    private Row serializedColumnFamilies(DecoratedKey ksNameKey)
    {
        ColumnFamilyStore cfsStore = databaseDescriptor.getSystemKeyspace().schemaCFS(SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF);
        return new Row(ksNameKey, cfsStore.getColumnFamily(QueryFilter.getIdentityFilter(ksNameKey,
                                                                                         SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF,
                                                                                         System.currentTimeMillis(),
                                                                                         databaseDescriptor,
                                                                                         databaseDescriptor.getTracing(),
                                                                                         databaseDescriptor.getDBConfig())));
    }

    private Row serializedUserTypes(DecoratedKey ksNameKey)
    {
        ColumnFamilyStore cfsStore = databaseDescriptor.getSystemKeyspace().schemaCFS(SystemKeyspace.SCHEMA_USER_TYPES_CF);
        return new Row(ksNameKey, cfsStore.getColumnFamily(QueryFilter.getIdentityFilter(ksNameKey,
                                                                                         SystemKeyspace.SCHEMA_USER_TYPES_CF,
                                                                                         System.currentTimeMillis(),
                                                                                         databaseDescriptor,
                                                                                         databaseDescriptor.getTracing(),
                                                                                         databaseDescriptor.getDBConfig())));
    }

    /**
     * Merge remote schema in form of mutations with local and mutate ks/cf metadata objects
     * (which also involves fs operations on add/drop ks/cf)
     *
     * @param mutations the schema changes to apply
     *
     * @throws ConfigurationException If one of metadata attributes has invalid value
     * @throws IOException If data was corrupted during transportation or failed to apply fs operations
     */
    public synchronized void mergeSchema(Collection<Mutation> mutations) throws ConfigurationException, IOException
    {
        mergeSchemaInternal(mutations, true);
        databaseDescriptor.getSchema().updateVersionAndAnnounce();
    }

    public synchronized void mergeSchemaInternal(Collection<Mutation> mutations, boolean doFlush) throws IOException
    {
        // compare before/after schemas of the affected keyspaces only
        Set<String> keyspaces = new HashSet<>(mutations.size());
        for (Mutation mutation : mutations)
            keyspaces.add(ByteBufferUtil.string(mutation.key()));

        // current state of the schema
        Map<DecoratedKey, ColumnFamily> oldKeyspaces = databaseDescriptor.getSystemKeyspace().getSchema(SystemKeyspace.SCHEMA_KEYSPACES_CF, keyspaces);
        Map<DecoratedKey, ColumnFamily> oldColumnFamilies = databaseDescriptor.getSystemKeyspace().getSchema(SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF, keyspaces);
        Map<DecoratedKey, ColumnFamily> oldTypes = databaseDescriptor.getSystemKeyspace().getSchema(SystemKeyspace.SCHEMA_USER_TYPES_CF, keyspaces);
        Map<DecoratedKey, ColumnFamily> oldFunctions = databaseDescriptor.getSystemKeyspace().getSchema(SystemKeyspace.SCHEMA_FUNCTIONS_CF);

        for (Mutation mutation : mutations)
            mutation.apply();

        if (doFlush && !databaseDescriptor.getStorageService().isClientMode())
            flushSchemaCFs();

        // with new data applied
        Map<DecoratedKey, ColumnFamily> newKeyspaces = databaseDescriptor.getSystemKeyspace().getSchema(SystemKeyspace.SCHEMA_KEYSPACES_CF, keyspaces);
        Map<DecoratedKey, ColumnFamily> newColumnFamilies = databaseDescriptor.getSystemKeyspace().getSchema(SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF, keyspaces);
        Map<DecoratedKey, ColumnFamily> newTypes = databaseDescriptor.getSystemKeyspace().getSchema(SystemKeyspace.SCHEMA_USER_TYPES_CF, keyspaces);
        Map<DecoratedKey, ColumnFamily> newFunctions = databaseDescriptor.getSystemKeyspace().getSchema(SystemKeyspace.SCHEMA_FUNCTIONS_CF);

        Set<String> keyspacesToDrop = mergeKeyspaces(oldKeyspaces, newKeyspaces);
        mergeColumnFamilies(oldColumnFamilies, newColumnFamilies);
        mergeTypes(oldTypes, newTypes);
        mergeFunctions(oldFunctions, newFunctions);

        // it is safe to drop a keyspace only when all nested ColumnFamilies where deleted
        for (String keyspaceToDrop : keyspacesToDrop)
            dropKeyspace(keyspaceToDrop);
    }

    private Set<String> mergeKeyspaces(Map<DecoratedKey, ColumnFamily> old, Map<DecoratedKey, ColumnFamily> updated)
    {
        // calculate the difference between old and new states (note that entriesOnlyLeft() will be always empty)
        MapDifference<DecoratedKey, ColumnFamily> diff = Maps.difference(old, updated);

        /**
         * At first step we check if any new keyspaces were added.
         */
        for (Map.Entry<DecoratedKey, ColumnFamily> entry : diff.entriesOnlyOnRight().entrySet())
        {
            ColumnFamily ksAttrs = entry.getValue();

            // we don't care about nested ColumnFamilies here because those are going to be processed separately
            if (ksAttrs.hasColumns())
                addKeyspace(databaseDescriptor.getKSMetaDataFactory().fromSchema(new Row(entry.getKey(), entry.getValue()), Collections.<CFMetaData>emptyList(), new UTMetaData()));
        }

        /**
         * At second step we check if there were any keyspaces re-created, in this context
         * re-created means that they were previously deleted but still exist in the low-level schema as empty keys
         */

        Map<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> modifiedEntries = diff.entriesDiffering();

        // instead of looping over all modified entries and skipping processed keys all the time
        // we would rather store "left to process" items and iterate over them removing already met keys
        List<DecoratedKey> leftToProcess = new ArrayList<DecoratedKey>(modifiedEntries.size());

        for (Map.Entry<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> entry : modifiedEntries.entrySet())
        {
            ColumnFamily prevValue = entry.getValue().leftValue();
            ColumnFamily newValue = entry.getValue().rightValue();

            if (!prevValue.hasColumns())
            {
                addKeyspace(databaseDescriptor.getKSMetaDataFactory().fromSchema(new Row(entry.getKey(), newValue), Collections.<CFMetaData>emptyList(), new UTMetaData()));
                continue;
            }

            leftToProcess.add(entry.getKey());
        }

        if (leftToProcess.size() == 0)
            return Collections.emptySet();

        /**
         * At final step we updating modified keyspaces and saving keyspaces drop them later
         */

        Set<String> keyspacesToDrop = new HashSet<String>();

        for (DecoratedKey key : leftToProcess)
        {
            MapDifference.ValueDifference<ColumnFamily> valueDiff = modifiedEntries.get(key);

            ColumnFamily newState = valueDiff.rightValue();

            if (newState.hasColumns())
                updateKeyspace(databaseDescriptor.getKSMetaDataFactory().fromSchema(new Row(key, newState), Collections.<CFMetaData>emptyList(), new UTMetaData()));
            else
                keyspacesToDrop.add(AsciiType.instance.getString(key.getKey()));
        }

        return keyspacesToDrop;
    }

    private void mergeColumnFamilies(Map<DecoratedKey, ColumnFamily> old, Map<DecoratedKey, ColumnFamily> updated)
    {
        // calculate the difference between old and new states (note that entriesOnlyLeft() will be always empty)
        MapDifference<DecoratedKey, ColumnFamily> diff = Maps.difference(old, updated);

        // check if any new Keyspaces with ColumnFamilies were added.
        for (Map.Entry<DecoratedKey, ColumnFamily> entry : diff.entriesOnlyOnRight().entrySet())
        {
            ColumnFamily cfAttrs = entry.getValue();

            if (cfAttrs.hasColumns())
            {
               Map<String, CFMetaData> cfDefs = databaseDescriptor.getKSMetaDataFactory().deserializeColumnFamilies(new Row(entry.getKey(), cfAttrs));

                for (CFMetaData cfDef : cfDefs.values())
                    addColumnFamily(cfDef);
            }
        }

        // deal with modified ColumnFamilies (remember that all of the keyspace nested ColumnFamilies are put to the single row)
        Map<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> modifiedEntries = diff.entriesDiffering();

        for (DecoratedKey keyspace : modifiedEntries.keySet())
        {
            MapDifference.ValueDifference<ColumnFamily> valueDiff = modifiedEntries.get(keyspace);

            ColumnFamily prevValue = valueDiff.leftValue(); // state before external modification
            ColumnFamily newValue = valueDiff.rightValue(); // updated state

            Row newRow = new Row(keyspace, newValue);

            if (!prevValue.hasColumns()) // whole keyspace was deleted and now it's re-created
            {
                for (CFMetaData cfm : databaseDescriptor.getKSMetaDataFactory().deserializeColumnFamilies(newRow).values())
                    addColumnFamily(cfm);
            }
            else if (!newValue.hasColumns()) // whole keyspace is deleted
            {
                for (CFMetaData cfm : databaseDescriptor.getKSMetaDataFactory().deserializeColumnFamilies(new Row(keyspace, prevValue)).values())
                    dropColumnFamily(cfm.ksName, cfm.cfName);
            }
            else // has modifications in the nested ColumnFamilies, need to perform nested diff to determine what was really changed
            {
                String ksName = AsciiType.instance.getString(keyspace.getKey());

                Map<String, CFMetaData> oldCfDefs = new HashMap<String, CFMetaData>();
                for (CFMetaData cfm : databaseDescriptor.getSchema().getKSMetaData(ksName).cfMetaData().values())
                    oldCfDefs.put(cfm.cfName, cfm);

                Map<String, CFMetaData> newCfDefs = databaseDescriptor.getKSMetaDataFactory().deserializeColumnFamilies(newRow);

                MapDifference<String, CFMetaData> cfDefDiff = Maps.difference(oldCfDefs, newCfDefs);

                for (CFMetaData cfDef : cfDefDiff.entriesOnlyOnRight().values())
                    addColumnFamily(cfDef);

                for (CFMetaData cfDef : cfDefDiff.entriesOnlyOnLeft().values())
                    dropColumnFamily(cfDef.ksName, cfDef.cfName);

                for (MapDifference.ValueDifference<CFMetaData> cfDef : cfDefDiff.entriesDiffering().values())
                    updateColumnFamily(cfDef.rightValue());
            }
        }
    }

    private void mergeTypes(Map<DecoratedKey, ColumnFamily> old, Map<DecoratedKey, ColumnFamily> updated)
    {
        MapDifference<DecoratedKey, ColumnFamily> diff = Maps.difference(old, updated);

        // New keyspace with types
        for (Map.Entry<DecoratedKey, ColumnFamily> entry : diff.entriesOnlyOnRight().entrySet())
        {
            ColumnFamily cfTypes = entry.getValue();
            if (!cfTypes.hasColumns())
                continue;

            for (UserType ut : UTMetaData.fromSchema(new Row(entry.getKey(), cfTypes), databaseDescriptor.getQueryProcessor()).values())
                addType(ut);
        }

        for (Map.Entry<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> modifiedEntry : diff.entriesDiffering().entrySet())
        {
            DecoratedKey keyspace = modifiedEntry.getKey();
            ColumnFamily prevCFTypes = modifiedEntry.getValue().leftValue(); // state before external modification
            ColumnFamily newCFTypes = modifiedEntry.getValue().rightValue(); // updated state

            if (!prevCFTypes.hasColumns()) // whole keyspace was deleted and now it's re-created
            {
                for (UserType ut : UTMetaData.fromSchema(new Row(keyspace, newCFTypes), databaseDescriptor.getQueryProcessor()).values())
                    addType(ut);
            }
            else if (!newCFTypes.hasColumns()) // whole keyspace is deleted
            {
                for (UserType ut : UTMetaData.fromSchema(new Row(keyspace, prevCFTypes), databaseDescriptor.getQueryProcessor()).values())
                    dropType(ut);
            }
            else // has modifications in the types, need to perform nested diff to determine what was really changed
            {
                MapDifference<ByteBuffer, UserType> typesDiff = Maps.difference(UTMetaData.fromSchema(new Row(keyspace, prevCFTypes), databaseDescriptor.getQueryProcessor()),
                                                                                UTMetaData.fromSchema(new Row(keyspace, newCFTypes), databaseDescriptor.getQueryProcessor()));

                for (UserType type : typesDiff.entriesOnlyOnRight().values())
                    addType(type);

                for (UserType type : typesDiff.entriesOnlyOnLeft().values())
                    dropType(type);

                for (MapDifference.ValueDifference<UserType> tdiff : typesDiff.entriesDiffering().values())
                    updateType(tdiff.rightValue()); // use the most recent value
            }
        }
    }

    private void mergeFunctions(Map<DecoratedKey, ColumnFamily> old, Map<DecoratedKey, ColumnFamily> updated)
    {
        MapDifference<DecoratedKey, ColumnFamily> diff = Maps.difference(old, updated);

        // New namespace with functions
        for (Map.Entry<DecoratedKey, ColumnFamily> entry : diff.entriesOnlyOnRight().entrySet())
        {
            ColumnFamily cfFunctions = entry.getValue();
            if (!cfFunctions.hasColumns())
                continue;

            for (UDFunction udf : UDFunction.fromSchema(new Row(entry.getKey(), cfFunctions), databaseDescriptor.getQueryProcessor(), databaseDescriptor.getMutationFactory(), databaseDescriptor.getCFMetaDataFactory()).values())
                addFunction(udf);
        }

        for (Map.Entry<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> modifiedEntry : diff.entriesDiffering().entrySet())
        {
            DecoratedKey namespace = modifiedEntry.getKey();
            ColumnFamily prevCFFunctions = modifiedEntry.getValue().leftValue(); // state before external modification
            ColumnFamily newCFFunctions = modifiedEntry.getValue().rightValue(); // updated state

            if (!prevCFFunctions.hasColumns()) // whole namespace was deleted and now it's re-created
            {
                for (UDFunction udf : UDFunction.fromSchema(new Row(namespace, newCFFunctions), databaseDescriptor.getQueryProcessor(), databaseDescriptor.getMutationFactory(), databaseDescriptor.getCFMetaDataFactory()).values())
                    addFunction(udf);
            }
            else if (!newCFFunctions.hasColumns()) // whole namespace is deleted
            {
                for (UDFunction udf : UDFunction.fromSchema(new Row(namespace, prevCFFunctions), databaseDescriptor.getQueryProcessor(), databaseDescriptor.getMutationFactory(), databaseDescriptor.getCFMetaDataFactory()).values())
                    dropFunction(udf);
            }
            else // has modifications in the functions, need to perform nested diff to determine what was really changed
            {
                MapDifference<ByteBuffer, UDFunction> functionsDiff = Maps.difference(UDFunction.fromSchema(new Row(namespace, prevCFFunctions), databaseDescriptor.getQueryProcessor(), databaseDescriptor.getMutationFactory(), databaseDescriptor.getCFMetaDataFactory()),
                                                                                      UDFunction.fromSchema(new Row(namespace, newCFFunctions), databaseDescriptor.getQueryProcessor(), databaseDescriptor.getMutationFactory(), databaseDescriptor.getCFMetaDataFactory()));

                for (UDFunction udf : functionsDiff.entriesOnlyOnRight().values())
                    addFunction(udf);

                for (UDFunction udf : functionsDiff.entriesOnlyOnLeft().values())
                    dropFunction(udf);

                for (MapDifference.ValueDifference<UDFunction> tdiff : functionsDiff.entriesDiffering().values())
                    updateFunction(tdiff.rightValue()); // use the most recent value
            }
        }
    }

    private void addKeyspace(KSMetaData ksm)
    {
        assert databaseDescriptor.getSchema().getKSMetaData(ksm.name) == null;
        databaseDescriptor.getSchema().load(ksm);

        if (!databaseDescriptor.getStorageService().isClientMode())
        {
            databaseDescriptor.getKeyspaceManager().open(ksm.name);
            databaseDescriptor.getMigrationManager().notifyCreateKeyspace(ksm);
        }
    }

    private void addColumnFamily(CFMetaData cfm)
    {
        assert databaseDescriptor.getSchema().getCFMetaData(cfm.ksName, cfm.cfName) == null;
        KSMetaData ksm = databaseDescriptor.getSchema().getKSMetaData(cfm.ksName);
        ksm = databaseDescriptor.getKSMetaDataFactory().cloneWith(ksm, Iterables.concat(ksm.cfMetaData().values(), Collections.singleton(cfm)));

        logger.info("Loading {}", cfm);

        databaseDescriptor.getSchema().load(cfm);

        // make sure it's init-ed w/ the old definitions first,
        // since we're going to call initCf on the new one manually
        databaseDescriptor.getKeyspaceManager().open(cfm.ksName);

        databaseDescriptor.getSchema().setKeyspaceDefinition(ksm);

        if (!databaseDescriptor.getStorageService().isClientMode())
        {
            databaseDescriptor.getKeyspaceManager().open(ksm.name).initCf(cfm.cfId, cfm.cfName, true);
            databaseDescriptor.getMigrationManager().notifyCreateColumnFamily(cfm);
        }
    }

    private void addType(UserType ut)
    {
        KSMetaData ksm = databaseDescriptor.getSchema().getKSMetaData(ut.keyspace);
        assert ksm != null;

        logger.info("Loading {}", ut);

        ksm.userTypes.addType(ut);

        if (!databaseDescriptor.getStorageService().isClientMode())
            databaseDescriptor.getMigrationManager().notifyCreateUserType(ut);
    }

    private void addFunction(UDFunction udf)
    {
        logger.info("Loading {}", udf);

        Functions.addFunction(udf);

        if (!databaseDescriptor.getStorageService().isClientMode())
            databaseDescriptor.getMigrationManager().notifyCreateFunction(udf);
    }

    private void updateKeyspace(KSMetaData newState)
    {
        KSMetaData oldKsm = databaseDescriptor.getSchema().getKSMetaData(newState.name);
        assert oldKsm != null;
        KSMetaData newKsm = databaseDescriptor.getKSMetaDataFactory().cloneWith(oldKsm.reloadAttributes(), oldKsm.cfMetaData().values());

        databaseDescriptor.getSchema().setKeyspaceDefinition(newKsm);

        if (!databaseDescriptor.getStorageService().isClientMode())
        {
            databaseDescriptor.getKeyspaceManager().open(newState.name).createReplicationStrategy(newKsm);
            databaseDescriptor.getMigrationManager().notifyUpdateKeyspace(newKsm);
        }
    }

    private void updateColumnFamily(CFMetaData newState)
    {
        CFMetaData cfm = databaseDescriptor.getSchema().getCFMetaData(newState.ksName, newState.cfName);
        assert cfm != null;
        cfm.reload();

        if (!databaseDescriptor.getStorageService().isClientMode())
        {
            Keyspace keyspace = databaseDescriptor.getKeyspaceManager().open(cfm.ksName);
            keyspace.getColumnFamilyStore(cfm.cfName).reload();
            databaseDescriptor.getMigrationManager().notifyUpdateColumnFamily(cfm);
        }
    }

    private void updateType(UserType ut)
    {
        KSMetaData ksm = databaseDescriptor.getSchema().getKSMetaData(ut.keyspace);
        assert ksm != null;

        logger.info("Updating {}", ut);

        ksm.userTypes.addType(ut);

        if (!databaseDescriptor.getStorageService().isClientMode())
            databaseDescriptor.getMigrationManager().notifyUpdateUserType(ut);
    }

    private void updateFunction(UDFunction udf)
    {
        logger.info("Updating {}", udf);

        Functions.replaceFunction(udf);

        if (!databaseDescriptor.getStorageService().isClientMode())
            databaseDescriptor.getMigrationManager().notifyUpdateFunction(udf);
    }

    private void dropKeyspace(String ksName)
    {
        KSMetaData ksm = databaseDescriptor.getSchema().getKSMetaData(ksName);
        String snapshotName = Keyspace.getTimestampedSnapshotName(ksName);

        databaseDescriptor.getCompactionManager().interruptCompactionFor(ksm.cfMetaData().values(), true);

        Keyspace keyspace = databaseDescriptor.getKeyspaceManager().open(ksm.name);

        // remove all cfs from the keyspace instance.
        List<UUID> droppedCfs = new ArrayList<>();
        for (CFMetaData cfm : ksm.cfMetaData().values())
        {
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfm.cfName);

            databaseDescriptor.getSchema().purge(cfm);

            if (!databaseDescriptor.getStorageService().isClientMode())
            {
                if (databaseDescriptor.isAutoSnapshot())
                    cfs.snapshot(snapshotName);
                databaseDescriptor.getKeyspaceManager().open(ksm.name).dropCf(cfm.cfId);
            }

            droppedCfs.add(cfm.cfId);
        }

        // remove the keyspace from the static instances.
        Keyspace.clear(ksm.name, databaseDescriptor.getSchema());
        databaseDescriptor.getSchema().clearKeyspaceDefinition(ksm);

        keyspace.writeOrder.awaitNewBarrier();

        // force a new segment in the CL
        databaseDescriptor.getCommitLog().forceRecycleAllSegments(droppedCfs);

        if (!databaseDescriptor.getStorageService().isClientMode())
        {
            databaseDescriptor.getMigrationManager().notifyDropKeyspace(ksm);
        }
    }

    private void dropColumnFamily(String ksName, String cfName)
    {
        KSMetaData ksm = databaseDescriptor.getSchema().getKSMetaData(ksName);
        assert ksm != null;
        ColumnFamilyStore cfs = databaseDescriptor.getKeyspaceManager().open(ksName).getColumnFamilyStore(cfName);
        assert cfs != null;

        // reinitialize the keyspace.
        CFMetaData cfm = ksm.cfMetaData().get(cfName);

        databaseDescriptor.getSchema().purge(cfm);
        databaseDescriptor.getSchema().setKeyspaceDefinition(makeNewKeyspaceDefinition(ksm, cfm));

        databaseDescriptor.getCompactionManager().interruptCompactionFor(Arrays.asList(cfm), true);

        if (!databaseDescriptor.getStorageService().isClientMode())
        {
            if (databaseDescriptor.isAutoSnapshot())
                cfs.snapshot(Keyspace.getTimestampedSnapshotName(cfs.name));
            databaseDescriptor.getKeyspaceManager().open(ksm.name).dropCf(cfm.cfId);
            databaseDescriptor.getMigrationManager().notifyDropColumnFamily(cfm);

            databaseDescriptor.getCommitLog().forceRecycleAllSegments(Collections.singleton(cfm.cfId));
        }
    }

    private void dropType(UserType ut)
    {
        KSMetaData ksm = databaseDescriptor.getSchema().getKSMetaData(ut.keyspace);
        assert ksm != null;

        ksm.userTypes.removeType(ut);

        if (!databaseDescriptor.getStorageService().isClientMode())
            databaseDescriptor.getMigrationManager().notifyDropUserType(ut);
    }

    private void dropFunction(UDFunction udf)
    {
        logger.info("Drop {}", udf);

        // TODO: this is kind of broken as this remove all overloads of the function name
        Functions.removeFunction(udf.name(), udf.argTypes());

        if (!databaseDescriptor.getStorageService().isClientMode())
            databaseDescriptor.getMigrationManager().notifyDropFunction(udf);
    }

    private KSMetaData makeNewKeyspaceDefinition(KSMetaData ksm, CFMetaData toExclude)
    {
        // clone ksm but do not include the new def
        List<CFMetaData> newCfs = new ArrayList<CFMetaData>(ksm.cfMetaData().values());
        newCfs.remove(toExclude);
        assert newCfs.size() == ksm.cfMetaData().size() - 1;
        return databaseDescriptor.getKSMetaDataFactory().cloneWith(ksm, newCfs);
    }

    private void flushSchemaCFs()
    {
        for (String cf : SystemKeyspace.allSchemaCfs)
            databaseDescriptor.getSystemKeyspace().forceBlockingFlush(cf);
    }
}
