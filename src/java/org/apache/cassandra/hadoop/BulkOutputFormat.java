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
package org.apache.cassandra.hadoop;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.config.CFMetaDataFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DBConfig;
import org.apache.cassandra.io.sstable.SSTableReaderFactory;
import org.apache.cassandra.locator.LocatorConfig;
import org.apache.cassandra.thrift.Mutation;
import org.apache.hadoop.mapreduce.*;

public class BulkOutputFormat extends AbstractBulkOutputFormat<ByteBuffer,List<Mutation>>
{
    private final DatabaseDescriptor databaseDescriptor;
    private final CFMetaDataFactory cfMetaDataFactory;
    private final SSTableReaderFactory ssTableReaderFactory;
    private final DBConfig dbConfig;

    public BulkOutputFormat(DatabaseDescriptor databaseDescriptor, CFMetaDataFactory cfMetaDataFactory, SSTableReaderFactory ssTableReaderFactory, DBConfig dbConfig)
    {
        this.databaseDescriptor = databaseDescriptor;
        this.cfMetaDataFactory = cfMetaDataFactory;
        this.ssTableReaderFactory = ssTableReaderFactory;
        this.dbConfig = dbConfig;
    }

    /** Fills the deprecated OutputFormat interface for streaming. */
    @Deprecated
    public BulkRecordWriter getRecordWriter(org.apache.hadoop.fs.FileSystem filesystem, org.apache.hadoop.mapred.JobConf job, String name, org.apache.hadoop.util.Progressable progress) throws IOException
    {
        return new BulkRecordWriter(job, progress, databaseDescriptor, cfMetaDataFactory, ssTableReaderFactory, dbConfig);
    }

    @Override
    public BulkRecordWriter getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException
    {
        return new BulkRecordWriter(context, databaseDescriptor, cfMetaDataFactory, ssTableReaderFactory, dbConfig);
    }
}
