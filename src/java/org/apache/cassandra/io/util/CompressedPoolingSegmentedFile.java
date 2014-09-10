/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.cassandra.io.util;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.io.compress.CompressedRandomAccessReader;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.service.FileCacheService;

public class CompressedPoolingSegmentedFile extends PoolingSegmentedFile implements ICompressedFile
{
    public final CompressionMetadata metadata;

    public CompressedPoolingSegmentedFile(String path, CompressionMetadata metadata, FileCacheService fileCacheService)
    {
        super(path, metadata.dataLength, metadata.compressedFileLength, fileCacheService);
        this.metadata = metadata;
    }

    public static class Builder extends CompressedSegmentedFile.Builder
    {
        private final FileCacheService fileCacheService;
        public Builder(CompressedSequentialWriter writer, FileCacheService fileCacheService, Config.DiskAccessMode diskAccessMode, IAllocator allocator)
        {
            super(writer, diskAccessMode, allocator);
            this.fileCacheService = fileCacheService;
        }

        public void addPotentialBoundary(long boundary)
        {
            // only one segment in a standard-io file
        }

        public SegmentedFile complete(String path)
        {
            return new CompressedPoolingSegmentedFile(path, metadata(path, false), fileCacheService);
        }

        public SegmentedFile openEarly(String path)
        {
            return new CompressedPoolingSegmentedFile(path, metadata(path, true), fileCacheService);
        }
    }

    protected RandomAccessReader createReader(String path)
    {
        return CompressedRandomAccessReader.open(path, metadata, this);
    }

    public CompressionMetadata getMetadata()
    {
        return metadata;
    }

    @Override
    public void cleanup()
    {
        super.cleanup();
        metadata.close();
    }
}
