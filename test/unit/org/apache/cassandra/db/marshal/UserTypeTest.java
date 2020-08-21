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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.db.marshal.ValueAccessors.ACCESSORS;

public class UserTypeTest
{

    private static Pair<List<FieldIdentifier>, List<AbstractType<?>>> getTypes(ValueGenerator[] generators, Random random)
    {
        List<AbstractType<?>> types = new ArrayList<>(generators.length);
        List<FieldIdentifier> names = new ArrayList<>(generators.length);

        for (int i=0; i<generators.length; i++)
        {
            types.add(generators[i].getType());
            names.add(FieldIdentifier.forUnquoted(ValueGenerator.randomString(random)));
        }

        return Pair.create(names, types);
    }

    /**
     * Serialize random user types in one format and deserialize it from another. Check they both produce the same
     * results in terms of composed object equality, toString, and comparisons
     */
    @Test
    public void testSerializationDeserialization()
    {
        for (int i=0; i<100; i++)
        {
            Random random = new Random(i);
            ValueGenerator[] generators = ValueGenerator.randomGenerators(random, 100);
            Pair<List<FieldIdentifier>, List<AbstractType<?>>> p = getTypes(generators, random);
            UserType type = new UserType("ks", ByteBufferUtil.bytes(ValueGenerator.randomString(random)),
                                         p.left, p.right, random.nextBoolean());

            for (int j=0; j<100; j++)
            {
                Object[] expected = ValueGenerator.createValues(generators, random);
                for (ValueAccessor<Object> srcAccessor : ACCESSORS)
                {
                    Object[] srcValues = ValueGenerator.decompose(generators, expected, srcAccessor);
                    Object srcJoined = TupleType.buildValue(srcValues, srcAccessor);
                    String srcString  = type.getString(srcJoined, srcAccessor);

                    for (ValueAccessor<Object> dstAccessor : ACCESSORS)
                    {
                        // convert data types and deserialize with
                        Object[] dstValues = ValueGenerator.convert(srcValues, dstAccessor);
                        Object dstJoined = TupleType.buildValue(dstValues, dstAccessor);
                        String dstString = type.getString(dstJoined, dstAccessor);

                        Object[] composed = ValueGenerator.compose(generators, dstValues, dstAccessor);
                        Assert.assertArrayEquals(expected, composed);
                        Assert.assertEquals(0, type.compare(srcJoined, srcAccessor, dstJoined, dstAccessor));
                        Assert.assertEquals(srcString, dstString);
                    }
                }
            }
        }
    }
}
