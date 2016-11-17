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

import org.apache.cassandra.utils.MerkleTree;

/**
 * Statistics about synchronizing two replica
 */
public class SyncStat
{
    public final NodePair nodes;
    public final List<MerkleTree.TreeDifference> differences;

    public SyncStat(NodePair nodes, List<MerkleTree.TreeDifference> differences)
    {
        this.nodes = nodes;
        this.differences = differences;
    }

    long bytes()
    {
        return differences.stream().mapToLong(MerkleTree.TreeDifference::totalSize).sum();
    }

    long rows()
    {
        return differences.stream().mapToLong(MerkleTree.TreeDifference::totalRows).sum();
    }
}
