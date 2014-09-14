package org.apache.cassandra.db;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaDataFactory;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.locator.LocatorConfig;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;

import java.io.File;

public class KeyspaceManager
{
    public static final KeyspaceManager instance;
    static
    {
        instance = new KeyspaceManager();
        assert KSMetaDataFactory.instance != null;
    }

    public final Function<String,Keyspace> keyspaceTransformer;

    public KeyspaceManager()
    {
        keyspaceTransformer = new Function<String, Keyspace>()
        {
            public Keyspace apply(String keyspaceName)
            {
                return open(keyspaceName);
            }

        };
    }

    private volatile boolean initialized = false;

    public void setInitialized()
    {
        initialized = true;
    }

    public Keyspace open(String keyspaceName)
    {
        assert initialized || keyspaceName.equals(Keyspace.SYSTEM_KS);
        return open(keyspaceName, true);
    }

    // to only be used by org.apache.cassandra.tools.Standalone* classes
    public Keyspace openWithoutSSTables(String keyspaceName)
    {
        return open(keyspaceName, false);
    }

    private Keyspace open(String keyspaceName, boolean loadSSTables)
    {
        Keyspace keyspaceInstance = Schema.instance.getKeyspaceInstance(keyspaceName);

        if (keyspaceInstance == null)
        {
            // instantiate the Keyspace.  we could use putIfAbsent but it's important to making sure it is only done once
            // per keyspace, so we synchronize and re-check before doing it.
            synchronized (Keyspace.class)
            {
                keyspaceInstance = Schema.instance.getKeyspaceInstance(keyspaceName);
                if (keyspaceInstance == null)
                {
                    // open and store the keyspace
                    keyspaceInstance = new Keyspace(keyspaceName,
                                                    loadSSTables,
                                                    Tracing.instance,
                                                    Schema.instance,
                                                    ColumnFamilyStoreManager.instance,
                                                    LocatorConfig.instance,
                                                    CommitLog.instance);
                    Schema.instance.storeKeyspaceInstance(keyspaceInstance);

                    // keyspace has to be constructed and in the cache before cacheRow can be called
                    for (ColumnFamilyStore cfs : keyspaceInstance.getColumnFamilyStores())
                        cfs.initRowCache();
                }
            }
        }
        return keyspaceInstance;
    }

    public Iterable<Keyspace> all()
    {
        return Iterables.transform(Schema.instance.getKeyspaces(), keyspaceTransformer);
    }

    public Iterable<Keyspace> nonSystem()
    {
        return Iterables.transform(Schema.instance.getNonSystemKeyspaces(), keyspaceTransformer);
    }

    public Iterable<Keyspace> system()
    {
        return Iterables.transform(Schema.systemKeyspaceNames, keyspaceTransformer);
    }

    /**
     * Removes every SSTable in the directory from the appropriate DataTracker's view.
     * @param directory the unreadable directory, possibly with SSTables in it, but not necessarily.
     */
    public void removeUnreadableSSTables(File directory)
    {
        for (Keyspace keyspace : all())
        {
            for (ColumnFamilyStore baseCfs : keyspace.getColumnFamilyStores())
            {
                for (ColumnFamilyStore cfs : baseCfs.concatWithIndexes())
                    cfs.maybeRemoveUnreadableSSTables(directory);
            }
        }
    }

}
