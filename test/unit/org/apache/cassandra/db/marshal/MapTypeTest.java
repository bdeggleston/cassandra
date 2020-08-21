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

package org.apache.cassandra.db.marshal;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

public class MapTypeTest
{
    private static Map<Object, Object> randomMap(ValueGenerator keyGen, ValueGenerator valGen, int size, Random random)
    {
        Map<Object, Object> map = new HashMap<>();
        for (int k=0; k<size; k++)
            map.put(keyGen.nextValue(random), valGen.nextValue(random));
        return map;
    }

    @Test
    public void testSerializationDeserialization()
    {
        CollectionTypeTests.testSerializationDeserialization((k, v) -> MapType.getInstance(k, v, false),
                                                             MapTypeTest::randomMap);
    }
}
