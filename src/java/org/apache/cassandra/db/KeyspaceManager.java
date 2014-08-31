package org.apache.cassandra.db;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.cassandra.config.KSMetaDataFactory;
import org.apache.cassandra.config.Schema;

import java.io.File;

public class KeyspaceManager
{
    public static final KeyspaceManager instance;
    static
    {
        KSMetaDataFactory ksmdf = KSMetaDataFactory.instance;
        instance = new KeyspaceManager();
    }

    public final Function<String,Keyspace> keyspaceTransformer = new Function<String, Keyspace>()
    {
        public Keyspace apply(String keyspaceName)
        {
            return open(keyspaceName);
        }

    };

    private volatile boolean initialized = false;
    public void setInitialized()
    {
        initialized = true;
    }

    public Keyspace open(String keyspaceName)
    {
        assert initialized || keyspaceName.equals(Keyspace.SYSTEM_KS);
        return open(keyspaceName, Schema.instance, true);
    }

    // to only be used by org.apache.cassandra.tools.Standalone* classes
    public Keyspace openWithoutSSTables(String keyspaceName)
    {
        return open(keyspaceName, Schema.instance, false);
    }

    private Keyspace open(String keyspaceName, Schema schema, boolean loadSSTables)
    {
        Keyspace keyspaceInstance = schema.getKeyspaceInstance(keyspaceName);

        if (keyspaceInstance == null)
        {
            // instantiate the Keyspace.  we could use putIfAbsent but it's important to making sure it is only done once
            // per keyspace, so we synchronize and re-check before doing it.
            synchronized (Keyspace.class)
            {
                keyspaceInstance = schema.getKeyspaceInstance(keyspaceName);
                if (keyspaceInstance == null)
                {
                    // open and store the keyspace
                    keyspaceInstance = new Keyspace(keyspaceName, loadSSTables);
                    schema.storeKeyspaceInstance(keyspaceInstance);

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
