package org.apache.cassandra.config;

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

import java.util.*;

public class KSMetaDataFactory
{

    public static final KSMetaDataFactory instance = new KSMetaDataFactory(DatabaseDescriptor.instance);

    private final DatabaseDescriptor databaseDescriptor;

    public KSMetaDataFactory(DatabaseDescriptor databaseDescriptor)
    {
        this.databaseDescriptor = databaseDescriptor;
    }

    public void init()
    {
        // Hardcoded system keyspaces
        List<KSMetaData> systemKeyspaces = Arrays.asList(systemKeyspace());
        assert systemKeyspaces.size() == Schema.systemKeyspaceNames.size();
        for (KSMetaData ksmd : systemKeyspaces)
            databaseDescriptor.getSchema().load(ksmd);
    }

    // For new user created keyspaces (through CQL)
    public KSMetaData newKeyspace(String name, String strategyName, Map<String, String> options, boolean durableWrites) throws ConfigurationException
    {
        Class<? extends AbstractReplicationStrategy> cls = AbstractReplicationStrategy.getClass(strategyName);
        if (cls.equals(LocalStrategy.class))
            throw new ConfigurationException("Unable to use given strategy class: LocalStrategy is reserved for internal use.");

        return newKeyspace(name, cls, options, durableWrites, Collections.<CFMetaData>emptyList());
    }

    public KSMetaData newKeyspace(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> options, boolean durablesWrites, Iterable<CFMetaData> cfDefs)
    {
        return new KSMetaData(name, strategyClass, options, durablesWrites, cfDefs, new UTMetaData(), databaseDescriptor.getQueryProcessor(),
                              databaseDescriptor.getLocatorConfig(), databaseDescriptor.getSystemKeyspace(), databaseDescriptor.getCFMetaDataFactory(), databaseDescriptor.getMutationFactory());
    }

    public KSMetaData cloneWith(KSMetaData ksm, Iterable<CFMetaData> cfDefs)
    {
        return new KSMetaData(ksm.name, ksm.strategyClass, ksm.strategyOptions, ksm.durableWrites, cfDefs, ksm.userTypes,
                              databaseDescriptor.getQueryProcessor(), databaseDescriptor.getLocatorConfig(), databaseDescriptor.getSystemKeyspace(), databaseDescriptor.getCFMetaDataFactory(), databaseDescriptor.getMutationFactory());
    }

    public KSMetaData systemKeyspace()
    {
        List<CFMetaData> cfDefs = Arrays.asList(databaseDescriptor.getCFMetaDataFactory().BatchlogCf,
                                                databaseDescriptor.getCFMetaDataFactory().RangeXfersCf,
                                                databaseDescriptor.getCFMetaDataFactory().LocalCf,
                                                databaseDescriptor.getCFMetaDataFactory().PeersCf,
                                                databaseDescriptor.getCFMetaDataFactory().PeerEventsCf,
                                                databaseDescriptor.getCFMetaDataFactory().HintsCf,
                                                databaseDescriptor.getCFMetaDataFactory().IndexCf,
                                                databaseDescriptor.getCFMetaDataFactory().SchemaKeyspacesCf,
                                                databaseDescriptor.getCFMetaDataFactory().SchemaColumnFamiliesCf,
                                                databaseDescriptor.getCFMetaDataFactory().SchemaColumnsCf,
                                                databaseDescriptor.getCFMetaDataFactory().SchemaTriggersCf,
                                                databaseDescriptor.getCFMetaDataFactory().SchemaUserTypesCf,
                                                databaseDescriptor.getCFMetaDataFactory().SchemaFunctionsCf,
                                                databaseDescriptor.getCFMetaDataFactory().CompactionLogCf,
                                                databaseDescriptor.getCFMetaDataFactory().CompactionHistoryCf,
                                                databaseDescriptor.getCFMetaDataFactory().PaxosCf,
                                                databaseDescriptor.getCFMetaDataFactory().SSTableActivityCF);
        return new KSMetaData(Keyspace.SYSTEM_KS, LocalStrategy.class, Collections.<String, String>emptyMap(), true, cfDefs,
                              databaseDescriptor.getQueryProcessor(), databaseDescriptor.getLocatorConfig(), databaseDescriptor.getSystemKeyspace(), databaseDescriptor.getCFMetaDataFactory(), databaseDescriptor.getMutationFactory());
    }

    public KSMetaData traceKeyspace()
    {
        List<CFMetaData> cfDefs = Arrays.asList(databaseDescriptor.getCFMetaDataFactory().TraceSessionsCf, databaseDescriptor.getCFMetaDataFactory().TraceEventsCf);
        return new KSMetaData(Tracing.TRACE_KS, SimpleStrategy.class, ImmutableMap.of("replication_factor", "2"), true, cfDefs,
                              databaseDescriptor.getQueryProcessor(), databaseDescriptor.getLocatorConfig(), databaseDescriptor.getSystemKeyspace(), databaseDescriptor.getCFMetaDataFactory(), databaseDescriptor.getMutationFactory());
    }

    public KSMetaData testMetadata(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> strategyOptions, CFMetaData... cfDefs)
    {
        return new KSMetaData(name, strategyClass, strategyOptions, true, Arrays.asList(cfDefs), databaseDescriptor.getQueryProcessor(),
                              databaseDescriptor.getLocatorConfig(), databaseDescriptor.getSystemKeyspace(), databaseDescriptor.getCFMetaDataFactory(), databaseDescriptor.getMutationFactory());
    }

    public KSMetaData testMetadataNotDurable(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> strategyOptions, CFMetaData... cfDefs)
    {
        return new KSMetaData(name, strategyClass, strategyOptions, false, Arrays.asList(cfDefs), databaseDescriptor.getQueryProcessor(),
                              databaseDescriptor.getLocatorConfig(), databaseDescriptor.getSystemKeyspace(), databaseDescriptor.getCFMetaDataFactory(), databaseDescriptor.getMutationFactory());
    }

    /**
     * Deserialize only Keyspace attributes without nested ColumnFamilies
     *
     * @param row Keyspace attributes in serialized form
     *
     * @return deserialized keyspace without cf_defs
     */
    public KSMetaData fromSchema(Row row, Iterable<CFMetaData> cfms, UTMetaData userTypes)
    {
        UntypedResultSet.Row result = databaseDescriptor.getQueryProcessor().resultify("SELECT * FROM system.schema_keyspaces", row).one();
        try
        {
            return new KSMetaData(result.getString("keyspace_name"),
                                  AbstractReplicationStrategy.getClass(result.getString("strategy_class")),
                                  FBUtilities.fromJsonMap(result.getString("strategy_options")),
                                  result.getBoolean("durable_writes"),
                                  cfms,
                                  userTypes,
                                  databaseDescriptor.getQueryProcessor(),
                                  databaseDescriptor.getLocatorConfig(),
                                  databaseDescriptor.getSystemKeyspace(),
                                  databaseDescriptor.getCFMetaDataFactory(),
                                  databaseDescriptor.getMutationFactory());
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deserialize Keyspace with nested ColumnFamilies
     *
     * @param serializedKs Keyspace in serialized form
     * @param serializedCFs Collection of the serialized ColumnFamilies
     *
     * @return deserialized keyspace with cf_defs
     */
    public KSMetaData fromSchema(Row serializedKs, Row serializedCFs, Row serializedUserTypes)
    {
        Map<String, CFMetaData> cfs = deserializeColumnFamilies(serializedCFs);
        UTMetaData userTypes = new UTMetaData(UTMetaData.fromSchema(serializedUserTypes, databaseDescriptor.getQueryProcessor()));
        return fromSchema(serializedKs, cfs.values(), userTypes);
    }

    public KSMetaData fromThrift(KsDef ksd, CFMetaData... cfDefs) throws ConfigurationException
    {
        Class<? extends AbstractReplicationStrategy> cls = AbstractReplicationStrategy.getClass(ksd.strategy_class);
        if (cls.equals(LocalStrategy.class))
            throw new ConfigurationException("Unable to use given strategy class: LocalStrategy is reserved for internal use.");

        return new KSMetaData(ksd.name,
                              cls,
                              ksd.strategy_options == null ? Collections.<String, String>emptyMap() : ksd.strategy_options,
                              ksd.durable_writes,
                              Arrays.asList(cfDefs),
                              databaseDescriptor.getQueryProcessor(),
                              databaseDescriptor.getLocatorConfig(),
                              databaseDescriptor.getSystemKeyspace(),
                              databaseDescriptor.getCFMetaDataFactory(),
                              databaseDescriptor.getMutationFactory());
    }

    /**
     * Deserialize ColumnFamilies from low-level schema representation, all of them belong to the same keyspace
     *
     * @return map containing name of the ColumnFamily and it's metadata for faster lookup
     */
    public Map<String, CFMetaData> deserializeColumnFamilies(Row row)
    {
        if (row.cf == null)
            return Collections.emptyMap();

        Map<String, CFMetaData> cfms = new HashMap<>();
        UntypedResultSet results = databaseDescriptor.getQueryProcessor().resultify("SELECT * FROM system.schema_columnfamilies", row);
        for (UntypedResultSet.Row result : results)
        {
            CFMetaData cfm = databaseDescriptor.getCFMetaDataFactory().fromSchema(result);
            cfms.put(cfm.cfName, cfm);
        }
        return cfms;
    }

}
