/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.cassandra.service.paxos;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DBConfig;
import org.apache.cassandra.db.MutationFactory;

public class PaxosState
{
    final Commit promised;
    final Commit accepted;
    final Commit mostRecentCommit;

    public PaxosState(ByteBuffer key, CFMetaData metadata, MutationFactory mutationFactory, DBConfig dbConfig)
    {
        this(Commit.emptyCommit(key, metadata, mutationFactory, dbConfig), Commit.emptyCommit(key, metadata, mutationFactory, dbConfig), Commit.emptyCommit(key, metadata, mutationFactory, dbConfig));
    }

    public PaxosState(Commit promised, Commit accepted, Commit mostRecentCommit)
    {
        assert promised.key == accepted.key && accepted.key == mostRecentCommit.key;
        assert promised.update.metadata() == accepted.update.metadata() && accepted.update.metadata() == mostRecentCommit.update.metadata();

        this.promised = promised;
        this.accepted = accepted;
        this.mostRecentCommit = mostRecentCommit;
    }

}
