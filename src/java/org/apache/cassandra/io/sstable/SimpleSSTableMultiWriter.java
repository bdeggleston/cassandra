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

package org.apache.cassandra.io.sstable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

public class SimpleSSTableMultiWriter implements SSTableMultiWriter
{
    private final SSTableWriter writer;

    public SimpleSSTableMultiWriter(Descriptor descriptor,
                                    long keyCount,
                                    long repairedAt,
                                    CFMetaData metadata,
                                    MetadataCollector metadataCollector,
                                    SerializationHeader header,
                                    LifecycleTransaction txn)
    {
        this(SSTableWriter.create(descriptor, keyCount, repairedAt, metadata, metadataCollector, header, txn));
    }

    private SimpleSSTableMultiWriter(SSTableWriter writer)
    {
        this.writer = writer;
    }

    public boolean append(UnfilteredRowIterator partition)
    {
        RowIndexEntry indexEntry = writer.append(partition);
        return indexEntry != null;
    }

    public Collection<SSTableReader> finish(long repairedAt, long maxDataAge, boolean openResult)
    {
        return Collections.singleton(writer.finish(repairedAt, maxDataAge, openResult));
    }

    public Collection<SSTableReader> finish(boolean openResult)
    {
        return Collections.singleton(writer.finish(openResult));
    }

    public Collection<SSTableReader> finished()
    {
        return Collections.singleton(writer.finished());
    }

    public String getFilename()
    {
        return writer.getFilename();
    }

    // TODO: rename to bytes written?
    public long getFilePointer()
    {
        return writer.getFilePointer();
    }

    public Throwable commit(Throwable accumulate)
    {
        return writer.commit(accumulate);
    }

    public Throwable abort(Throwable accumulate)
    {
        return writer.abort(accumulate);
    }

    public void prepareToCommit()
    {
        writer.prepareToCommit();
    }

    public void close() throws Exception
    {
        writer.close();
    }

    public SSTableMultiWriter setOpenResult(boolean openResult)
    {
        writer.setOpenResult(openResult);
        return this;
    }

    public static SSTableMultiWriter create(Descriptor descriptor, long keyCount, long repairedAt, int sstableLevel, SerializationHeader header, LifecycleTransaction txn)
    {
        SSTableWriter writer = SSTableWriter.create(descriptor, keyCount, repairedAt, sstableLevel, header, txn);
        return new SimpleSSTableMultiWriter(writer);
    }
}
