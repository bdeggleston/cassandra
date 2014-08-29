package org.apache.cassandra.config;

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KSMetaDataFactory
{

    public static final KSMetaDataFactory instance = new KSMetaDataFactory();

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
        return new KSMetaData(name, strategyClass, options, durablesWrites, cfDefs, new UTMetaData());
    }

    public KSMetaData cloneWith(KSMetaData ksm, Iterable<CFMetaData> cfDefs)
    {
        return new KSMetaData(ksm.name, ksm.strategyClass, ksm.strategyOptions, ksm.durableWrites, cfDefs, ksm.userTypes);
    }

    public KSMetaData systemKeyspace()
    {
        List<CFMetaData> cfDefs = Arrays.asList(CFMetaData.BatchlogCf,
                                                CFMetaData.RangeXfersCf,
                                                CFMetaData.LocalCf,
                                                CFMetaData.PeersCf,
                                                CFMetaData.PeerEventsCf,
                                                CFMetaData.HintsCf,
                                                CFMetaData.IndexCf,
                                                CFMetaData.SchemaKeyspacesCf,
                                                CFMetaData.SchemaColumnFamiliesCf,
                                                CFMetaData.SchemaColumnsCf,
                                                CFMetaData.SchemaTriggersCf,
                                                CFMetaData.SchemaUserTypesCf,
                                                CFMetaData.SchemaFunctionsCf,
                                                CFMetaData.CompactionLogCf,
                                                CFMetaData.CompactionHistoryCf,
                                                CFMetaData.PaxosCf,
                                                CFMetaData.SSTableActivityCF);
        return new KSMetaData(Keyspace.SYSTEM_KS, LocalStrategy.class, Collections.<String, String>emptyMap(), true, cfDefs);
    }

    public KSMetaData traceKeyspace()
    {
        List<CFMetaData> cfDefs = Arrays.asList(CFMetaData.TraceSessionsCf, CFMetaData.TraceEventsCf);
        return new KSMetaData(Tracing.TRACE_KS, SimpleStrategy.class, ImmutableMap.of("replication_factor", "2"), true, cfDefs);
    }

    public KSMetaData testMetadata(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> strategyOptions, CFMetaData... cfDefs)
    {
        return new KSMetaData(name, strategyClass, strategyOptions, true, Arrays.asList(cfDefs));
    }

    public KSMetaData testMetadataNotDurable(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> strategyOptions, CFMetaData... cfDefs)
    {
        return new KSMetaData(name, strategyClass, strategyOptions, false, Arrays.asList(cfDefs));
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
                                  userTypes);
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
        Map<String, CFMetaData> cfs = KSMetaData.deserializeColumnFamilies(serializedCFs);
        UTMetaData userTypes = new UTMetaData(UTMetaData.fromSchema(serializedUserTypes, QueryProcessor.instance));
        return fromSchema(serializedKs, cfs.values(), userTypes);
    }

}
