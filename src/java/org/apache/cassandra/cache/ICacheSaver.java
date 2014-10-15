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
package org.apache.cassandra.cache;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.utils.Pair;

import java.util.Iterator;
import java.util.concurrent.Future;

/**
 * Saves and loads caches from disk
 */
public interface ICacheSaver<K extends CacheKey, V>
{
    public Reader<K, V> getReader(ColumnFamilyStore cfs);
    public Writer<K> getWriter();

    public interface Reader<K extends CacheKey, V>
    {
        public Iterator<Future<Pair<K, V>>> read();
    }

    public interface Writer<K extends CacheKey>
    {
        public int write(Iterator<K> keys);
    }
}
