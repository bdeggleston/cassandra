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

package org.apache.cassandra.config;

import java.util.List;
import java.util.UUID;

import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.FBUtilities;

public class CFMetaDataFactory
{
    public static final CFMetaDataFactory instance;
    static
    {
        String className = System.getProperty("cassandra.cfmetadatafactory", CFMetaDataFactory.class.getName());
        CFMetaDataFactory factory;
        try
        {
            factory = (CFMetaDataFactory) FBUtilities.classForName(className, "CFMetaDataFactory").newInstance();
        }
        catch (InstantiationException | IllegalAccessException e)
        {
            throw new IllegalArgumentException(e);
        }
        instance = factory;
    }


    public CFMetaData newCFMetaData(String keyspace,
                                    String name, UUID cfId,
                                    boolean isSuper,
                                    boolean isCounter,
                                    boolean isDense,
                                    boolean isCompound,
                                    boolean isMaterializedView,
                                    List<ColumnDefinition> partitionKeyColumns,
                                    List<ColumnDefinition> clusteringColumns,
                                    PartitionColumns partitionColumns,
                                    IPartitioner partitioner)
    {
        return new CFMetaData(keyspace,
                              name,
                              cfId,
                              isSuper,
                              isCounter,
                              isDense,
                              isCompound,
                              isMaterializedView,
                              partitionKeyColumns,
                              clusteringColumns,
                              partitionColumns,
                              partitioner);
    }
}
