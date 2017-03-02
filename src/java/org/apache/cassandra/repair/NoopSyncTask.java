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

package org.apache.cassandra.repair;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.MerkleTree;

/**
 * Doesn't perform any syncing, only collects data about ranges that need to be repaired
 */
public class NoopSyncTask extends SyncTask
{
    private static final Logger logger = LoggerFactory.getLogger(NoopSyncTask.class);

    public NoopSyncTask(RepairJobDesc desc, TreeResponse r1, TreeResponse r2)
    {
        super(desc, r1, r2, true);
    }

    @Override
    protected void startSync(List<MerkleTree.TreeDifference> differences)
    {
        // noop, sync stat will be retrieved later
        set(stat);
    }
}
