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
package org.apache.cassandra.hadoop.cql3;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.auth.Auth;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.DBConfig;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.hadoop.AbstractBulkRecordWriter;
import org.apache.cassandra.hadoop.BulkRecordWriter;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.sstable.SSTableReaderFactory;
import org.apache.cassandra.locator.LocatorConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

/**
 * The <code>CqlBulkRecordWriter</code> maps the output &lt;key, value&gt;
 * pairs to a Cassandra column family. In particular, it applies the binded variables
 * in the value to the prepared statement, which it associates with the key, and in 
 * turn the responsible endpoint.
 *
 * <p>
 * Furthermore, this writer groups the cql queries by the endpoint responsible for
 * the rows being affected. This allows the cql queries to be executed in parallel,
 * directly to a responsible endpoint.
 * </p>
 *
 * @see CqlBulkOutputFormat
 */
public class CqlBulkRecordWriter extends AbstractBulkRecordWriter<Object, List<ByteBuffer>>
{
    private String keyspace;
    private String columnFamily;
    private String schemaDesc;
    private String insertStatement;
    private File outputDir;

    private final DatabaseDescriptor databaseDescriptor;
    private final Schema schema;
    private final QueryProcessor queryProcessor;
    private final CFMetaDataFactory cfMetaDataFactory;
    private final KSMetaDataFactory ksMetaDataFactory;
    private final SSTableReaderFactory ssTableReaderFactory;
    private final Auth auth;
    private final LocatorConfig locatorConfig;
    private final DBConfig dbConfig;

    CqlBulkRecordWriter(TaskAttemptContext context,
                        DatabaseDescriptor databaseDescriptor,
                        Schema schema,
                        QueryProcessor queryProcessor,
                        CFMetaDataFactory cfMetaDataFactory,
                        KSMetaDataFactory ksMetaDataFactory,
                        SSTableReaderFactory ssTableReaderFactory,
                        Auth auth,
                        LocatorConfig locatorConfig,
                        DBConfig dbConfig) throws IOException
    {
        super(context, databaseDescriptor);
        this.databaseDescriptor = databaseDescriptor;
        this.schema = schema;
        this.queryProcessor = queryProcessor;
        this.cfMetaDataFactory = cfMetaDataFactory;
        this.ksMetaDataFactory = ksMetaDataFactory;
        this.ssTableReaderFactory = ssTableReaderFactory;
        this.auth = auth;
        this.locatorConfig = locatorConfig;
        this.dbConfig = dbConfig;
        setConfigs();
    }

    CqlBulkRecordWriter(Configuration conf,
                        Progressable progress,
                        DatabaseDescriptor databaseDescriptor,
                        Schema schema,
                        QueryProcessor queryProcessor,
                        CFMetaDataFactory cfMetaDataFactory,
                        KSMetaDataFactory ksMetaDataFactory,
                        SSTableReaderFactory ssTableReaderFactory,
                        Auth auth,
                        LocatorConfig locatorConfig,
                        DBConfig dbConfig) throws IOException
    {
        super(conf, progress, databaseDescriptor);
        this.databaseDescriptor = databaseDescriptor;
        this.schema = schema;
        this.queryProcessor = queryProcessor;
        this.cfMetaDataFactory = cfMetaDataFactory;
        this.ksMetaDataFactory = ksMetaDataFactory;
        this.ssTableReaderFactory = ssTableReaderFactory;
        this.auth = auth;
        this.locatorConfig = locatorConfig;
        this.dbConfig = dbConfig;
        setConfigs();
    }

    CqlBulkRecordWriter(Configuration conf,
                        DatabaseDescriptor databaseDescriptor,
                        Schema schema,
                        QueryProcessor queryProcessor,
                        CFMetaDataFactory cfMetaDataFactory,
                        KSMetaDataFactory ksMetaDataFactory,
                        SSTableReaderFactory ssTableReaderFactory,
                        Auth auth,
                        LocatorConfig locatorConfig,
                        DBConfig dbConfig) throws IOException
    {
        super(conf, databaseDescriptor);
        this.databaseDescriptor = databaseDescriptor;
        this.schema = schema;
        this.queryProcessor = queryProcessor;
        this.cfMetaDataFactory = cfMetaDataFactory;
        this.ksMetaDataFactory = ksMetaDataFactory;
        this.ssTableReaderFactory = ssTableReaderFactory;
        this.auth = auth;
        this.locatorConfig = locatorConfig;
        this.dbConfig = dbConfig;
        setConfigs();
    }
    
    private void setConfigs() throws IOException
    {
        // if anything is missing, exceptions will be thrown here, instead of on write()
        keyspace = ConfigHelper.getOutputKeyspace(conf);
        columnFamily = ConfigHelper.getOutputColumnFamily(conf);
        schemaDesc = CqlBulkOutputFormat.getColumnFamilySchema(conf, columnFamily);
        insertStatement = CqlBulkOutputFormat.getColumnFamilyInsertStatement(conf, columnFamily);
        outputDir = getColumnFamilyDirectory();
    }

    
    private void prepareWriter() throws IOException
    {
        try
        {
            if (writer == null)
            {
                writer = CQLSSTableWriter.builder(schema, queryProcessor, auth, ksMetaDataFactory, dbConfig)
                    .forTable(schemaDesc)
                    .using(insertStatement)
                    .withPartitioner(ConfigHelper.getOutputPartitioner(conf))
                    .inDirectory(outputDir)
                    .withBufferSizeInMB(Integer.parseInt(conf.get(BUFFER_SIZE_IN_MB, "64")))
                    .build();
            }
            if (loader == null)
            {
                ExternalClient externalClient = new ExternalClient(conf, queryProcessor, cfMetaDataFactory, locatorConfig, databaseDescriptor, dbConfig);
                
                externalClient.addKnownCfs(keyspace, schemaDesc);

                this.loader = new SSTableLoader(outputDir, externalClient, new BulkRecordWriter.NullOutputHandler(), databaseDescriptor, ssTableReaderFactory);
            }
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }      
    }
    
    /**
     * The column values must correspond to the order in which
     * they appear in the insert stored procedure. 
     * 
     * Key is not used, so it can be null or any object.
     * </p>
     *
     * @param key
     *            any object or null.
     * @param values
     *            the values to write.
     * @throws IOException
     */
    @Override
    public void write(Object key, List<ByteBuffer> values) throws IOException
    {
        prepareWriter();
        try
        {
            ((CQLSSTableWriter) writer).rawAddRow(values);
            
            if (null != progress)
                progress.progress();
            if (null != context)
                HadoopCompat.progress(context);
        } 
        catch (InvalidRequestException e)
        {
            throw new IOException("Error adding row with key: " + key, e);
        }
    }
    
    private File getColumnFamilyDirectory() throws IOException
    {
        File dir = new File(String.format("%s%s%s%s%s", getOutputLocation(), File.separator, keyspace, File.separator, columnFamily));
        
        if (!dir.exists() && !dir.mkdirs())
        {
            throw new IOException("Failed to created output directory: " + dir);
        }
        
        return dir;
    }
    
    public static class ExternalClient extends AbstractBulkRecordWriter.ExternalClient
    {
        private Map<String, Map<String, CFMetaData>> knownCqlCfs = new HashMap<>();
        private final QueryProcessor queryProcessor;
        
        public ExternalClient(Configuration conf, QueryProcessor queryProcessor, CFMetaDataFactory cfMetaDataFactory, LocatorConfig locatorConfig, DatabaseDescriptor databaseDescriptor, DBConfig dbConfig)
        {
            super(conf, cfMetaDataFactory, locatorConfig, databaseDescriptor, dbConfig);
            this.queryProcessor = queryProcessor;
        }

        public void addKnownCfs(String keyspace, String cql)
        {
            Map<String, CFMetaData> cfs = knownCqlCfs.get(keyspace);
            
            if (cfs == null)
            {
                cfs = new HashMap<>();
                knownCqlCfs.put(keyspace, cfs);
            }
            
            CFMetaData metadata = cfMetaDataFactory.compile(cql, keyspace, queryProcessor);
            cfs.put(metadata.cfName, metadata);
        }
        
        @Override
        public CFMetaData getCFMetaData(String keyspace, String cfName)
        {
            CFMetaData metadata = super.getCFMetaData(keyspace, cfName);
            if (metadata != null)
            {
                return metadata;
            }
            
            Map<String, CFMetaData> cfs = knownCqlCfs.get(keyspace);
            return cfs != null ? cfs.get(cfName) : null;
        }
    }
}
