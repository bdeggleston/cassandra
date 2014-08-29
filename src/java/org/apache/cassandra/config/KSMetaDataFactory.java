package org.apache.cassandra.config;

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.tracing.Tracing;

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

}
