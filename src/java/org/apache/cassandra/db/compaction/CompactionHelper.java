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

package org.apache.cassandra.db.compaction;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.SSTableRewriter;

// TODO: come up with a non-crappy name
public class CompactionHelper
{
    private final ColumnFamilyStore cfs;

    public CompactionHelper(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
    }

    public ColumnFamilyStore getCfs()
    {
        return cfs;
    }

    public Directories getDirectories()
    {
        return cfs.directories;
    }

    public SSTableRewriter createSSTableRewriter(LifecycleTransaction transaction, long maxAge, boolean isOffline, boolean shouldOpenEarly)
    {
        return new SSTableRewriter(cfs, transaction, maxAge, isOffline, shouldOpenEarly);
    }

    public final SSTableRewriter createSSTableRewriter(LifecycleTransaction transaction, long maxAge, boolean isOffline)
    {
        return createSSTableRewriter(transaction, maxAge, isOffline, true);
    }
}
