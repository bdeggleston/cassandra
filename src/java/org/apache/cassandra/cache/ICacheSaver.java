package org.apache.cassandra.cache;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.utils.Pair;

import java.util.Iterator;
import java.util.concurrent.Future;

/**
 * Saves and loads caches from disk
 */
public interface ICacheSaver<K extends CacheKey, V>
{
    public Reader<K, V> getReader(ColumnFamilyStore cfs);
    public Writer<K> getWriter();

    public interface Reader<K extends CacheKey, V>
    {
        public Iterator<Future<Pair<K, V>>> read();
    }

    public interface Writer<K extends CacheKey>
    {
        public int write(Iterator<K> keys);
    }
}
