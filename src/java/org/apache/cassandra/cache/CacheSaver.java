package org.apache.cassandra.cache;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public abstract class CacheSaver<K extends CacheKey, V> implements ICacheSaver<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(CacheSaver.class);

    private final ICacheSerializer<K, V> serializer;
    protected final CacheService.CacheType cacheType;

    public CacheSaver(ICacheSerializer<K, V> serializer, CacheService.CacheType cacheType)
    {
        this.serializer = serializer;
        this.cacheType = cacheType;
    }

    public File getCachePath(String ksName, String cfName, UUID cfId, String version)
    {
        return DatabaseDescriptor.getSerializedCachePath(ksName, cfName, cfId, cacheType, version);
    }

    protected String getVersion()
    {
        return "b";
    }

    @Override
    public ICacheSaver.Reader<K, V> getReader(ColumnFamilyStore cfs)
    {
        return new Reader(cfs);
    }

    @Override
    public ICacheSaver.Writer<K> getWriter()
    {
        return new Writer();
    }

    public class Reader implements ICacheSaver.Reader<K, V>
    {

        private final ColumnFamilyStore cfs;

        public Reader(ColumnFamilyStore cfs)
        {
            this.cfs = cfs;
        }

        protected class ReadIterator implements Iterator<Future<Pair<K, V>>>
        {
            private final FileDataInput in;
            private boolean open = true;

            public ReadIterator(FileDataInput in)
            {
                this.in = in;
            }

            private void maybeClose()
            {
                if (open)
                {
                    FileUtils.closeQuietly(in);
                    open = false;
                }
            }

            @Override
            public boolean hasNext()
            {
                try
                {
                    if (!in.isEOF())
                        return true;
                }
                catch (IOException e)
                {
                    // just close and return false
                }
                maybeClose();
                return false;
            }

            @Override
            public Future<Pair<K, V>> next()
            {
                try
                {
                    return serializer.deserialize(in, cfs);
                }
                catch (IOException e)
                {
                    throw new AssertionError(e);
                }
            }

            @Override
            public void remove() {}
        }

        protected Iterator<Future<Pair<K, V>>> emptyIterator()
        {
            return new Iterator<Future<Pair<K, V>>>()
            {
                @Override public boolean hasNext() { return false; }
                @Override public Future<Pair<K, V>> next() { return null; }
                @Override public void remove() {}
            };
        }

        protected ReadIterator getReadIterator(FileDataInput in)
        {
            return new ReadIterator(in);
        }


        @Override
        public Iterator<Future<Pair<K, V>>> read()
        {
            // modern format, allows both key and value (so key cache load can be purely sequential)
            File path = getCachePath(cfs.keyspace.getName(), cfs.name, cfs.metadata.cfId, getVersion());
            // if path does not exist, try without cfId (assuming saved cache is created with current CF)
            if (!path.exists())
                path = getCachePath(cfs.keyspace.getName(), cfs.name, null, getVersion());

            if (! path.exists())
                return emptyIterator();

            logger.info(String.format("reading saved cache %s", path));
            RandomAccessReader reader = RandomAccessReader.open(path);

            return getReadIterator(reader);
        }
    }

    public class Writer implements ICacheSaver.Writer<K>
    {

        protected HashMap<CacheKey.PathInfo, SequentialWriter> writers = new HashMap<>();

        protected void writeKey(K key, SequentialWriter writer)
        {
            try
            {
                serializer.serialize(key, writer.stream);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, writer.getPath());
            }
        }

        protected void close()
        {
            for (Map.Entry<CacheKey.PathInfo, SequentialWriter> info : writers.entrySet())
            {
                CacheKey.PathInfo path = info.getKey();
                SequentialWriter writer = info.getValue();

                File tmpFile = new File(writer.getPath());
                File cacheFile = getCachePath(path.keyspace, path.columnFamily, path.cfId, getVersion());

                cacheFile.delete(); // ignore error if it didn't exist
                if (!tmpFile.renameTo(cacheFile))
                    logger.error("Unable to rename {} to {}", tmpFile, cacheFile);
            }

        }

        @Override
        public int write(Iterator<K> keys)
        {
            logger.debug("Deleting old {} files.", cacheType);
            deleteOldCacheFiles();

            if (!keys.hasNext())
            {
                logger.debug("Skipping {} save, cache is empty.", cacheType);
                return 0;
            }

            long start = System.nanoTime();
            int keysWritten = 0;


            try
            {
                while (keys.hasNext())
                {
                    K key = keys.next();

                    CacheKey.PathInfo path = key.getPathInfo();
                    SequentialWriter writer = writers.get(path);
                    if (writer == null)
                    {
                        writer = tempCacheFile(path);
                        writers.put(path, writer);
                    }

                    writeKey(key, writer);
                    keysWritten++;
                }
            }
            finally
            {
                for (SequentialWriter writer : writers.values())
                    FileUtils.closeQuietly(writer);
            }

            close();
            logger.info("Saved {} ({} items) in {} ms", cacheType, keysWritten, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            return keysWritten;
        }

        protected SequentialWriter newSequentialWriter(File path)
        {
            return SequentialWriter.open(path);
        }

        protected SequentialWriter tempCacheFile(CacheKey.PathInfo pathInfo)
        {
            File path = getCachePath(pathInfo.keyspace, pathInfo.columnFamily, pathInfo.cfId, getVersion());
            File tmpFile = FileUtils.createTempFile(path.getName(), null, path.getParentFile());
            return newSequentialWriter(tmpFile);
        }

        protected void deleteOldCacheFiles()
        {
            File savedCachesDir = new File(DatabaseDescriptor.getSavedCachesLocation());
            assert savedCachesDir.exists() && savedCachesDir.isDirectory();
            File[] files = savedCachesDir.listFiles();
            if (files != null)
            {
                for (File file : files)
                {
                    if (!file.isFile())
                        continue; // someone's been messing with our directory.  naughty!

                    if (file.getName().endsWith(cacheType.toString())
                            || file.getName().endsWith(String.format("%s-%s.db", cacheType.toString(), getVersion())))
                    {
                        if (!file.delete())
                            logger.warn("Failed to delete {}", file.getAbsolutePath());
                    }
                }
            } else
                logger.warn("Could not list files in {}", savedCachesDir);
        }
    }
}
