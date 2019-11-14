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
package org.apache.cassandra.db.composites;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.values.Value;
import org.apache.cassandra.utils.values.Values;

import org.junit.Test;

import java.nio.ByteBuffer;

public class CTypeTest
{
    @Test
    public void testCompoundType()
    {
        CompositeType baseType = CompositeType.getInstance(AsciiType.instance, UUIDType.instance, LongType.instance);

        Value a1 = CompositeType.build(
                Values.valueOf("a"),
                UUIDType.instance.valueFromString("00000000-0000-0000-0000-000000000000"),
                Values.valueOf(1));
        Value a2 = CompositeType.build(
                Values.valueOf("a"),
                UUIDType.instance.valueFromString("00000000-0000-0000-0000-000000000000"),
                Values.valueOf(100));
        Value b1 = CompositeType.build(
                Values.valueOf("a"),
                UUIDType.instance.valueFromString("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                Values.valueOf(1));
        Value b2 = CompositeType.build(
                Values.valueOf("a"),
                UUIDType.instance.valueFromString("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                Values.valueOf(100));
        Value c1 = CompositeType.build(
                Values.valueOf("z"),
                UUIDType.instance.valueFromString("00000000-0000-0000-0000-000000000000"),
                Values.valueOf(1));
        Value c2 = CompositeType.build(
                Values.valueOf("z"),
                UUIDType.instance.valueFromString("00000000-0000-0000-0000-000000000000"),
                Values.valueOf(100));
        Value d1 = CompositeType.build(
                Values.valueOf("z"),
                UUIDType.instance.valueFromString("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                Values.valueOf(1));
        Value d2 = CompositeType.build(
                Values.valueOf("z"),
                UUIDType.instance.valueFromString("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                Values.valueOf(100));
        Value z1 = CompositeType.build(
                Values.EMPTY,
                UUIDType.instance.valueFromString("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                Values.valueOf(100));

        assert baseType.compare(a1,a2) < 0;
        assert baseType.compare(a2,b1) < 0;
        assert baseType.compare(b1,b2) < 0;
        assert baseType.compare(b2,c1) < 0;
        assert baseType.compare(c1,c2) < 0;
        assert baseType.compare(c2,d1) < 0;
        assert baseType.compare(d1,d2) < 0;

        assert baseType.compare(a2,a1) > 0;
        assert baseType.compare(b1,a2) > 0;
        assert baseType.compare(b2,b1) > 0;
        assert baseType.compare(c1,b2) > 0;
        assert baseType.compare(c2,c1) > 0;
        assert baseType.compare(d1,c2) > 0;
        assert baseType.compare(d2,d1) > 0;

        assert baseType.compare(z1,a1) < 0;
        assert baseType.compare(z1,a2) < 0;
        assert baseType.compare(z1,b1) < 0;
        assert baseType.compare(z1,b2) < 0;
        assert baseType.compare(z1,c1) < 0;
        assert baseType.compare(z1,c2) < 0;
        assert baseType.compare(z1,d1) < 0;
        assert baseType.compare(z1,d2) < 0;

        assert baseType.compare(a1,a1) == 0;
        assert baseType.compare(a2,a2) == 0;
        assert baseType.compare(b1,b1) == 0;
        assert baseType.compare(b2,b2) == 0;
        assert baseType.compare(c1,c1) == 0;
        assert baseType.compare(c2,c2) == 0;
        assert baseType.compare(z1,z1) == 0;
    }

    @Test
    public void testSimpleType2()
    {
        CompositeType baseType = CompositeType.getInstance(UUIDType.instance);
        Value a = CompositeType.build(UUIDType.instance.valueFromString("00000000-0000-0000-0000-000000000000"));
        Value z = CompositeType.build(UUIDType.instance.valueFromString("ffffffff-ffff-ffff-ffff-ffffffffffff"));

        assert baseType.compare(a,z) < 0;
        assert baseType.compare(z,a) > 0;
        assert baseType.compare(a,a) == 0;
        assert baseType.compare(z,z) == 0;
    }

    @Test
    public void testSimpleType1()
    {
        CompositeType baseType = CompositeType.getInstance(BytesType.instance);
        Value a = CompositeType.build(Values.valueOf("a"));
        Value z = CompositeType.build(Values.valueOf("z"));

        assert baseType.compare(a,z) < 0;
        assert baseType.compare(z,a) > 0;
        assert baseType.compare(a,a) == 0;
        assert baseType.compare(z,z) == 0;
    }
}
