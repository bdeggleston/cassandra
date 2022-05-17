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

package org.apache.cassandra.service.accord.txn;

import java.util.HashMap;
import java.util.Map;

import accord.api.Data;
import org.apache.cassandra.db.partitions.FilteredPartition;

public class TxnData implements Data
{
    private final Map<String, FilteredPartition> data = new HashMap<>();

    public void put(String name, FilteredPartition partition)
    {
        data.put(name, partition);
    }

    public FilteredPartition get(String name)
    {
        return data.get(name);
    }

    @Override
    public Data merge(Data data)
    {
        TxnData that = (TxnData) data;
        TxnData merged = new TxnData();
        this.data.forEach(merged::put);
        that.data.forEach(merged::put);
        return merged;
    }
}
