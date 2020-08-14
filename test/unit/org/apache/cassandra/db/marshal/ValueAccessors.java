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

import java.nio.ByteBuffer;

public class ValueAccessors
{
    private static <V> void assertEqual(V expected, ValueAccessor<V> accessor, Object actualValue, boolean equal)
    {
        V actual = accessor.convert(actualValue);
        if ((accessor.compareUnsigned(expected, actual) == 0) != equal)
        {
            String msg = String.format("Expected %s %s actual %s, but was %s",
                                       accessor.toHex(expected), equal ? "=" : "!=",
                                       accessor.toHex(actual), equal ? "!=" : "=");
            throw new AssertionError(msg);
        }
    }

    public static void assertDataEquals(Object expected, Object actual)
    {
        if (expected instanceof ByteBuffer)
            assertEqual((ByteBuffer) expected, ByteBufferAccessor.instance, actual, true);
        else if (expected instanceof byte[])
            assertEqual((byte[]) expected, ByteArrayAccessor.instance, actual, true);
        else
            throw new AssertionError("Unhandled type " + expected.getClass().getName());
    }

    public static void assertDataNotEquals(Object expected, Object actual)
    {
        if (expected instanceof ByteBuffer)
            assertEqual((ByteBuffer) expected, ByteBufferAccessor.instance, actual, false);
        else if (expected instanceof byte[])
            assertEqual((byte[]) expected, ByteArrayAccessor.instance, actual, false);
        else
            throw new AssertionError("Unhandled type " + expected.getClass().getName());
    }
}
