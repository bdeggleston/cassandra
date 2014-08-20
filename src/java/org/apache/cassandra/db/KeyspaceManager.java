package org.apache.cassandra.db;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class KeyspaceManager
{
    private static final Logger logger = LoggerFactory.getLogger(Keyspace.class);

    public static final String SYSTEM_KS = "system";
    public static final KeyspaceManager instance = new KeyspaceManager(
            Schema.instance, StorageService.instance, CommitLog.instance, ColumnFamilyStoreManager.instance
    );

    private final Schema schema;
    private final StorageService storageService;
    private final CommitLog commitLog;
    private final ColumnFamilyStoreManager columnFamilyStoreManager;

    private volatile boolean initialized = false;

    public KeyspaceManager(Schema schema, StorageService storageService, CommitLog commitLog, ColumnFamilyStoreManager columnFamilyStoreManager)
    {
        assert schema != null;
        assert storageService != null;
        assert commitLog != null;
        assert columnFamilyStoreManager != null;

        this.schema = schema;
        this.storageService = storageService;
        this.commitLog = commitLog;
        this.columnFamilyStoreManager = columnFamilyStoreManager;
    }

    public final Function<String,Keyspace> keyspaceTransformer = new Function<String, Keyspace>()
    {
        public Keyspace apply(String keyspaceName)
        {
            return open(keyspaceName);
        }
    };

    @Deprecated
    public Keyspace open(String keyspaceName)
    {
        assert initialized || keyspaceName.equals(SYSTEM_KS);
        return open(keyspaceName, schema, true);
    }

    public void setInitialized()
    {
        initialized = true;
    }

    // to only be used by org.apache.cassandra.tools.Standalone* classes
    public Keyspace openWithoutSSTables(String keyspaceName)
    {
        return open(keyspaceName, schema, false);
    }

    public Keyspace open(String keyspaceName, Schema schema)
    {
        return open(keyspaceName, schema, true);
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

    public Keyspace clear(String keyspaceName)
    {
        return clear(keyspaceName, schema);
    }

    public Keyspace clear(String keyspaceName, Schema schema)
    {
        synchronized (Keyspace.class)
        {
            Keyspace t = schema.removeKeyspaceInstance(keyspaceName);
            if (t != null)
            {
                t.clear();
            }
            return t;
        }
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

    public Iterable<Keyspace> all()
    {
        return Iterables.transform(schema.getKeyspaces(), keyspaceTransformer);
    }

    public Iterable<Keyspace> nonSystem()
    {
        return Iterables.transform(schema.getNonSystemKeyspaces(), keyspaceTransformer);
    }

    public Iterable<Keyspace> system()
    {
        return Iterables.transform(Schema.systemKeyspaceNames, keyspaceTransformer);
    }

}
