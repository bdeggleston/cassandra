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

public class NachoCFMetaData //extends CFMetaData
{
//    private volatile boolean nachoMode = false;
//    private volatile int nachoQuesoFactor = 0;
//    private volatile boolean tortillaSombrero = false;
//
//    public NachoCFMetaData(String keyspace,
//                           String name,
//                           UUID cfId,
//                           boolean isSuper,
//                           boolean isCounter,
//                           boolean isDense,
//                           boolean isCompound,
//                           boolean isMaterializedView,
//                           List<ColumnDefinition> partitionKeyColumns,
//                           List<ColumnDefinition> clusteringColumns,
//                           PartitionColumns partitionColumns)
//    {
//        super(keyspace,
//              name,
//              cfId,
//              isSuper,
//              isCounter,
//              isDense,
//              isCompound,
//              isMaterializedView,
//              partitionKeyColumns,
//              clusteringColumns,
//              partitionColumns);
//    }
//
//    public boolean isNachoMode()
//    {
//        return nachoMode;
//    }
//
//    public int getNachoQuesoFactor()
//    {
//        return nachoQuesoFactor;
//    }
//
//    public boolean isTortillaSombrero()
//    {
//        return tortillaSombrero;
//    }
//
//    public CFMetaData setNachoMode(boolean nachoMode)
//    {
//        this.nachoMode = nachoMode;
//        return this;
//    }
//
//    public CFMetaData setNachoQuesoFactor(int nachoQuesoFactor)
//    {
//        this.nachoQuesoFactor = nachoQuesoFactor;
//        return this;
//    }
//
//    public CFMetaData setTortillaSombrero(boolean nachoTortillaSombrero)
//    {
//        this.tortillaSombrero = nachoTortillaSombrero;
//        return this;
//    }
//
//    @Override
//    public CFMetaData copyOpts(CFMetaData that)
//    {
//        NachoCFMetaData cfm = (NachoCFMetaData) super.copyOpts(that);
//        cfm.setNachoMode(nachoMode);
//        cfm.setNachoQuesoFactor(nachoQuesoFactor);
//        cfm.setTortillaSombrero(tortillaSombrero);
//        return cfm;
//    }
//
//    @Override
//    public boolean equals(Object o)
//    {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        if (!super.equals(o)) return false;
//
//        NachoCFMetaData that = (NachoCFMetaData) o;
//
//        if (nachoMode != that.nachoMode) return false;
//        if (nachoQuesoFactor != that.nachoQuesoFactor) return false;
//        return tortillaSombrero == that.tortillaSombrero;
//    }
//
//    @Override
//    public int hashCode()
//    {
//        int result = super.hashCode();
//        result = 31 * result + (nachoMode ? 1 : 0);
//        result = 31 * result + nachoQuesoFactor;
//        result = 31 * result + (tortillaSombrero ? 1 : 0);
//        return result;
//    }
}
