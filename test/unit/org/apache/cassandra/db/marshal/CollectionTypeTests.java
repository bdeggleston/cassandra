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

import java.util.Random;

import org.junit.Assert;

import static org.apache.cassandra.db.marshal.ValueAccessors.ACCESSORS;

public class CollectionTypeTests
{
    interface TypeFactory<T extends CollectionType> { T createType(AbstractType<?> keyType, AbstractType<?> valType); }
    interface ValueFactory<T> { T createValue(ValueGenerator keyGen, ValueGenerator valGen, int size, Random random); }

    static <CT extends CollectionType, T> void testSerializationDeserialization(TypeFactory<CT> typeFactory, ValueFactory<T> valueFactory)
    {
        for (int i=0; i<75; i++)
        {
            Random random = new Random(i);
            ValueGenerator keyType = ValueGenerator.randomGenerator(random);
            ValueGenerator valueType = ValueGenerator.randomGenerator(random);
            CT type = typeFactory.createType(keyType.getType(), valueType.getType());

            for (int j=0; j<75; j++)
            {
                int size = random.nextInt(1000);
                T expected = valueFactory.createValue(keyType, valueType, size, random);

                for (ValueAccessor<Object> srcAccessor : ACCESSORS)
                {
                    Object srcBytes = type.decompose(expected, srcAccessor);
                    String srcString = type.getString(srcBytes, srcAccessor);

                    for (ValueAccessor<Object> dstAccessor : ACCESSORS)
                    {
                        Object dstBytes = dstAccessor.convert(srcBytes, srcAccessor);
                        String dstString = type.getString(dstBytes, dstAccessor);
                        T composed = (T) type.compose(dstBytes, dstAccessor);
                        Assert.assertEquals(expected, composed);
                        ValueAccessors.assertDataEquals(srcBytes, srcAccessor, dstBytes, dstAccessor);
                        Assert.assertEquals(srcString, dstString);
                    }
                }
            }
        }
    }
}
