package org.apache.cassandra.config;

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.MutationFactory;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.LocatorConfig;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

import java.util.*;

public class KSMetaDataFactory
{

    public static final KSMetaDataFactory instance = new KSMetaDataFactory();

    public KSMetaDataFactory()
    {
        // Hardcoded system keyspaces
        List<KSMetaData> systemKeyspaces = Arrays.asList(systemKeyspace());
        assert systemKeyspaces.size() == Schema.systemKeyspaceNames.size();
        for (KSMetaData ksmd : systemKeyspaces)
            Schema.instance.load(ksmd);
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
        return new KSMetaData(name, strategyClass, options, durablesWrites, cfDefs, new UTMetaData(), QueryProcessor.instance,
                              LocatorConfig.instance, SystemKeyspace.instance, CFMetaDataFactory.instance, MutationFactory.instance);
    }

    public KSMetaData cloneWith(KSMetaData ksm, Iterable<CFMetaData> cfDefs)
    {
        return new KSMetaData(ksm.name, ksm.strategyClass, ksm.strategyOptions, ksm.durableWrites, cfDefs, ksm.userTypes,
                              QueryProcessor.instance, LocatorConfig.instance, SystemKeyspace.instance, CFMetaDataFactory.instance, MutationFactory.instance);
    }

    public KSMetaData systemKeyspace()
    {
        List<CFMetaData> cfDefs = Arrays.asList(CFMetaDataFactory.instance.BatchlogCf,
                                                CFMetaDataFactory.instance.RangeXfersCf,
                                                CFMetaDataFactory.instance.LocalCf,
                                                CFMetaDataFactory.instance.PeersCf,
                                                CFMetaDataFactory.instance.PeerEventsCf,
                                                CFMetaDataFactory.instance.HintsCf,
                                                CFMetaDataFactory.instance.IndexCf,
                                                CFMetaDataFactory.instance.SchemaKeyspacesCf,
                                                CFMetaDataFactory.instance.SchemaColumnFamiliesCf,
                                                CFMetaDataFactory.instance.SchemaColumnsCf,
                                                CFMetaDataFactory.instance.SchemaTriggersCf,
                                                CFMetaDataFactory.instance.SchemaUserTypesCf,
                                                CFMetaDataFactory.instance.SchemaFunctionsCf,
                                                CFMetaDataFactory.instance.CompactionLogCf,
                                                CFMetaDataFactory.instance.CompactionHistoryCf,
                                                CFMetaDataFactory.instance.PaxosCf,
                                                CFMetaDataFactory.instance.SSTableActivityCF);
        return new KSMetaData(Keyspace.SYSTEM_KS, LocalStrategy.class, Collections.<String, String>emptyMap(), true, cfDefs,
                              QueryProcessor.instance, LocatorConfig.instance, SystemKeyspace.instance, CFMetaDataFactory.instance, MutationFactory.instance);
    }

    public KSMetaData traceKeyspace()
    {
        List<CFMetaData> cfDefs = Arrays.asList(CFMetaDataFactory.instance.TraceSessionsCf, CFMetaDataFactory.instance.TraceEventsCf);
        return new KSMetaData(Tracing.TRACE_KS, SimpleStrategy.class, ImmutableMap.of("replication_factor", "2"), true, cfDefs,
                              QueryProcessor.instance, LocatorConfig.instance, SystemKeyspace.instance, CFMetaDataFactory.instance, MutationFactory.instance);
    }

    public KSMetaData testMetadata(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> strategyOptions, CFMetaData... cfDefs)
    {
        return new KSMetaData(name, strategyClass, strategyOptions, true, Arrays.asList(cfDefs), QueryProcessor.instance,
                              LocatorConfig.instance, SystemKeyspace.instance, CFMetaDataFactory.instance, MutationFactory.instance);
    }

    public KSMetaData testMetadataNotDurable(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> strategyOptions, CFMetaData... cfDefs)
    {
        return new KSMetaData(name, strategyClass, strategyOptions, false, Arrays.asList(cfDefs), QueryProcessor.instance,
                              LocatorConfig.instance, SystemKeyspace.instance, CFMetaDataFactory.instance, MutationFactory.instance);
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
        UntypedResultSet.Row result = QueryProcessor.instance.resultify("SELECT * FROM system.schema_keyspaces", row).one();
        try
        {
            return new KSMetaData(result.getString("keyspace_name"),
                                  AbstractReplicationStrategy.getClass(result.getString("strategy_class")),
                                  FBUtilities.fromJsonMap(result.getString("strategy_options")),
                                  result.getBoolean("durable_writes"),
                                  cfms,
                                  userTypes,
                                  QueryProcessor.instance,
                                  LocatorConfig.instance,
                                  SystemKeyspace.instance,
                                  CFMetaDataFactory.instance,
                                  MutationFactory.instance);
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
        UTMetaData userTypes = new UTMetaData(UTMetaData.fromSchema(serializedUserTypes, QueryProcessor.instance));
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
                              QueryProcessor.instance,
                              LocatorConfig.instance,
                              SystemKeyspace.instance,
                              CFMetaDataFactory.instance,
                              MutationFactory.instance);
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
        UntypedResultSet results = QueryProcessor.instance.resultify("SELECT * FROM system.schema_columnfamilies", row);
        for (UntypedResultSet.Row result : results)
        {
            CFMetaData cfm = CFMetaDataFactory.instance.fromSchema(result);
            cfms.put(cfm.cfName, cfm);
        }
        return cfms;
    }

}
