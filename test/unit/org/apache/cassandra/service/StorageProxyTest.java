/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.LocatorConfig;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import static org.apache.cassandra.Util.token;
import static org.apache.cassandra.Util.rp;

import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class StorageProxyTest
{
    public static final DatabaseDescriptor databaseDescriptor = DatabaseDescriptor.createMain(false);

    private static Range<RowPosition> range(RowPosition left, RowPosition right)
    {
        return new Range<RowPosition>(left, right, databaseDescriptor.getLocatorConfig().getPartitioner());
    }

    private static Bounds<RowPosition> bounds(RowPosition left, RowPosition right)
    {
        return new Bounds<RowPosition>(left, right, databaseDescriptor.getLocatorConfig().getPartitioner());
    }

    private static ExcludingBounds<RowPosition> exBounds(RowPosition left, RowPosition right)
    {
        return new ExcludingBounds<RowPosition>(left, right, databaseDescriptor.getLocatorConfig().getPartitioner());
    }

    private static IncludingExcludingBounds<RowPosition> incExBounds(RowPosition left, RowPosition right)
    {
        return new IncludingExcludingBounds<RowPosition>(left, right, databaseDescriptor.getLocatorConfig().getPartitioner());
    }

    private static RowPosition startOf(String key)
    {
        return databaseDescriptor.getLocatorConfig().getPartitioner().getToken(ByteBufferUtil.bytes(key)).minKeyBound();
    }

    private static RowPosition endOf(String key)
    {
        return databaseDescriptor.getLocatorConfig().getPartitioner().getToken(ByteBufferUtil.bytes(key)).maxKeyBound();
    }

    private static Range<Token> tokenRange(String left, String right)
    {
        return new Range<Token>(token(left, databaseDescriptor), token(right, databaseDescriptor), databaseDescriptor.getLocatorConfig().getPartitioner());
    }

    private static Bounds<Token> tokenBounds(String left, String right)
    {
        return new Bounds<Token>(token(left, databaseDescriptor), token(right, databaseDescriptor), databaseDescriptor.getLocatorConfig().getPartitioner());
    }

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        TokenMetadata tmd = databaseDescriptor.getLocatorConfig().getTokenMetadata();
        tmd.updateNormalToken(token("1", databaseDescriptor), InetAddress.getByName("127.0.0.1"));
        tmd.updateNormalToken(token("6", databaseDescriptor), InetAddress.getByName("127.0.0.6"));
    }

    // test getRestrictedRanges for token
    private void testGRR(AbstractBounds<Token> queryRange, AbstractBounds<Token>... expected)
    {
        // Testing for tokens
        List<AbstractBounds<Token>> restricted = databaseDescriptor.getStorageProxy().getRestrictedRanges(queryRange);
        assertEquals(restricted.toString(), expected.length, restricted.size());
        for (int i = 0; i < expected.length; i++)
            assertEquals("Mismatch for index " + i + ": " + restricted, expected[i], restricted.get(i));
    }

    // test getRestrictedRanges for keys
    private void testGRRKeys(AbstractBounds<RowPosition> queryRange, AbstractBounds<RowPosition>... expected)
    {
        // Testing for keys
        List<AbstractBounds<RowPosition>> restrictedKeys = databaseDescriptor.getStorageProxy().getRestrictedRanges(queryRange);
        assertEquals(restrictedKeys.toString(), expected.length, restrictedKeys.size());
        for (int i = 0; i < expected.length; i++)
            assertEquals("Mismatch for index " + i + ": " + restrictedKeys, expected[i], restrictedKeys.get(i));

    }

    @Test
    public void testGRR() throws Throwable
    {
        // no splits
        testGRR(tokenRange("2", "5"), tokenRange("2", "5"));
        testGRR(tokenBounds("2", "5"), tokenBounds("2", "5"));
        // single split
        testGRR(tokenRange("2", "7"), tokenRange("2", "6"), tokenRange("6", "7"));
        testGRR(tokenBounds("2", "7"), tokenBounds("2", "6"), tokenRange("6", "7"));
        // single split starting from min
        testGRR(tokenRange("", "2"), tokenRange("", "1"), tokenRange("1", "2"));
        testGRR(tokenBounds("", "2"), tokenBounds("", "1"), tokenRange("1", "2"));
        // single split ending with max
        testGRR(tokenRange("5", ""), tokenRange("5", "6"), tokenRange("6", ""));
        testGRR(tokenBounds("5", ""), tokenBounds("5", "6"), tokenRange("6", ""));
        // two splits
        testGRR(tokenRange("0", "7"), tokenRange("0", "1"), tokenRange("1", "6"), tokenRange("6", "7"));
        testGRR(tokenBounds("0", "7"), tokenBounds("0", "1"), tokenRange("1", "6"), tokenRange("6", "7"));


        // Keys
        // no splits
        testGRRKeys(range(rp("2", databaseDescriptor), rp("5", databaseDescriptor)), range(rp("2", databaseDescriptor), rp("5", databaseDescriptor)));
        testGRRKeys(bounds(rp("2", databaseDescriptor), rp("5", databaseDescriptor)), bounds(rp("2", databaseDescriptor), rp("5", databaseDescriptor)));
        testGRRKeys(exBounds(rp("2", databaseDescriptor), rp("5", databaseDescriptor)), exBounds(rp("2", databaseDescriptor), rp("5", databaseDescriptor)));
        // single split testGRRKeys(range("2", "7"), range(rp("2"), endOf("6")), range(endOf("6"), rp("7")));
        testGRRKeys(bounds(rp("2", databaseDescriptor), rp("7", databaseDescriptor)), bounds(rp("2", databaseDescriptor), endOf("6")), range(endOf("6"), rp("7", databaseDescriptor)));
        testGRRKeys(exBounds(rp("2", databaseDescriptor), rp("7", databaseDescriptor)), range(rp("2", databaseDescriptor), endOf("6")), exBounds(endOf("6"), rp("7", databaseDescriptor)));
        testGRRKeys(incExBounds(rp("2", databaseDescriptor), rp("7", databaseDescriptor)), bounds(rp("2", databaseDescriptor), endOf("6")), exBounds(endOf("6"), rp("7", databaseDescriptor)));
        // single split starting from min
        testGRRKeys(range(rp("", databaseDescriptor), rp("2", databaseDescriptor)), range(rp("", databaseDescriptor), endOf("1")), range(endOf("1"), rp("2", databaseDescriptor)));
        testGRRKeys(bounds(rp("", databaseDescriptor), rp("2", databaseDescriptor)), bounds(rp("", databaseDescriptor), endOf("1")), range(endOf("1"), rp("2", databaseDescriptor)));
        testGRRKeys(exBounds(rp("", databaseDescriptor), rp("2", databaseDescriptor)), range(rp("", databaseDescriptor), endOf("1")), exBounds(endOf("1"), rp("2", databaseDescriptor)));
        testGRRKeys(incExBounds(rp("", databaseDescriptor), rp("2", databaseDescriptor)), bounds(rp("", databaseDescriptor), endOf("1")), exBounds(endOf("1"), rp("2", databaseDescriptor)));
        // single split ending with max
        testGRRKeys(range(rp("5", databaseDescriptor), rp("", databaseDescriptor)), range(rp("5", databaseDescriptor), endOf("6")), range(endOf("6"), rp("", databaseDescriptor)));
        testGRRKeys(bounds(rp("5", databaseDescriptor), rp("", databaseDescriptor)), bounds(rp("5", databaseDescriptor), endOf("6")), range(endOf("6"), rp("", databaseDescriptor)));
        testGRRKeys(exBounds(rp("5", databaseDescriptor), rp("", databaseDescriptor)), range(rp("5", databaseDescriptor), endOf("6")), exBounds(endOf("6"), rp("", databaseDescriptor)));
        testGRRKeys(incExBounds(rp("5", databaseDescriptor), rp("", databaseDescriptor)), bounds(rp("5", databaseDescriptor), endOf("6")), exBounds(endOf("6"), rp("", databaseDescriptor)));
        // two splits
        testGRRKeys(range(rp("0", databaseDescriptor), rp("7", databaseDescriptor)), range(rp("0", databaseDescriptor), endOf("1")), range(endOf("1"), endOf("6")), range(endOf("6"), rp("7", databaseDescriptor)));
        testGRRKeys(bounds(rp("0", databaseDescriptor), rp("7", databaseDescriptor)), bounds(rp("0", databaseDescriptor), endOf("1")), range(endOf("1"), endOf("6")), range(endOf("6"), rp("7", databaseDescriptor)));
        testGRRKeys(exBounds(rp("0", databaseDescriptor), rp("7", databaseDescriptor)), range(rp("0", databaseDescriptor), endOf("1")), range(endOf("1"), endOf("6")), exBounds(endOf("6"), rp("7", databaseDescriptor)));
        testGRRKeys(incExBounds(rp("0", databaseDescriptor), rp("7", databaseDescriptor)), bounds(rp("0", databaseDescriptor), endOf("1")), range(endOf("1"), endOf("6")), exBounds(endOf("6"), rp("7", databaseDescriptor)));
    }

    @Test
    public void testGRRExact() throws Throwable
    {
        // min
        testGRR(tokenRange("1", "5"), tokenRange("1", "5"));
        testGRR(tokenBounds("1", "5"), tokenBounds("1", "1"), tokenRange("1", "5"));
        // max
        testGRR(tokenRange("2", "6"), tokenRange("2", "6"));
        testGRR(tokenBounds("2", "6"), tokenBounds("2", "6"));
        // both
        testGRR(tokenRange("1", "6"), tokenRange("1", "6"));
        testGRR(tokenBounds("1", "6"), tokenBounds("1", "1"), tokenRange("1", "6"));


        // Keys
        // min
        testGRRKeys(range(endOf("1"), endOf("5")), range(endOf("1"), endOf("5")));
        testGRRKeys(range(rp("1", databaseDescriptor), endOf("5")), range(rp("1", databaseDescriptor), endOf("1")), range(endOf("1"), endOf("5")));
        testGRRKeys(bounds(startOf("1"), endOf("5")), bounds(startOf("1"), endOf("1")), range(endOf("1"), endOf("5")));
        testGRRKeys(exBounds(endOf("1"), rp("5", databaseDescriptor)), exBounds(endOf("1"), rp("5", databaseDescriptor)));
        testGRRKeys(exBounds(rp("1", databaseDescriptor), rp("5", databaseDescriptor)), range(rp("1", databaseDescriptor), endOf("1")), exBounds(endOf("1"), rp("5", databaseDescriptor)));
        testGRRKeys(exBounds(startOf("1"), endOf("5")), range(startOf("1"), endOf("1")), exBounds(endOf("1"), endOf("5")));
        testGRRKeys(incExBounds(rp("1", databaseDescriptor), rp("5", databaseDescriptor)), bounds(rp("1", databaseDescriptor), endOf("1")), exBounds(endOf("1"), rp("5", databaseDescriptor)));
        // max
        testGRRKeys(range(endOf("2"), endOf("6")), range(endOf("2"), endOf("6")));
        testGRRKeys(bounds(startOf("2"), endOf("6")), bounds(startOf("2"), endOf("6")));
        testGRRKeys(exBounds(rp("2", databaseDescriptor), rp("6", databaseDescriptor)), exBounds(rp("2", databaseDescriptor), rp("6", databaseDescriptor)));
        testGRRKeys(incExBounds(rp("2", databaseDescriptor), rp("6", databaseDescriptor)), incExBounds(rp("2", databaseDescriptor), rp("6", databaseDescriptor)));
        // bothKeys
        testGRRKeys(range(rp("1", databaseDescriptor), rp("6", databaseDescriptor)), range(rp("1", databaseDescriptor), endOf("1")), range(endOf("1"), rp("6", databaseDescriptor)));
        testGRRKeys(bounds(rp("1", databaseDescriptor), rp("6", databaseDescriptor)), bounds(rp("1", databaseDescriptor), endOf("1")), range(endOf("1"), rp("6", databaseDescriptor)));
        testGRRKeys(exBounds(rp("1", databaseDescriptor), rp("6", databaseDescriptor)), range(rp("1", databaseDescriptor), endOf("1")), exBounds(endOf("1"), rp("6", databaseDescriptor)));
        testGRRKeys(incExBounds(rp("1", databaseDescriptor), rp("6", databaseDescriptor)), bounds(rp("1", databaseDescriptor), endOf("1")), exBounds(endOf("1"), rp("6", databaseDescriptor)));
    }

    @Test
    public void testGRRWrapped() throws Throwable
    {
        // one token in wrapped range
        testGRR(tokenRange("7", "0"), tokenRange("7", ""), tokenRange("", "0"));
        // two tokens in wrapped range
        testGRR(tokenRange("5", "0"), tokenRange("5", "6"), tokenRange("6", ""), tokenRange("", "0"));
        testGRR(tokenRange("7", "2"), tokenRange("7", ""), tokenRange("", "1"), tokenRange("1", "2"));
        // full wraps
        testGRR(tokenRange("0", "0"), tokenRange("0", "1"), tokenRange("1", "6"), tokenRange("6", ""), tokenRange("", "0"));
        testGRR(tokenRange("", ""), tokenRange("", "1"), tokenRange("1", "6"), tokenRange("6", ""));
        // wrap on member tokens
        testGRR(tokenRange("6", "6"), tokenRange("6", ""), tokenRange("", "1"), tokenRange("1", "6"));
        testGRR(tokenRange("6", "1"), tokenRange("6", ""), tokenRange("", "1"));
        // end wrapped
        testGRR(tokenRange("5", ""), tokenRange("5", "6"), tokenRange("6", ""));

        // Keys
        // one token in wrapped range
        testGRRKeys(range(rp("7", databaseDescriptor), rp("0", databaseDescriptor)), range(rp("7", databaseDescriptor), rp("", databaseDescriptor)), range(rp("", databaseDescriptor), rp("0", databaseDescriptor)));
        // two tokens in wrapped range
        testGRRKeys(range(rp("5", databaseDescriptor), rp("0", databaseDescriptor)), range(rp("5", databaseDescriptor), endOf("6")), range(endOf("6"), rp("", databaseDescriptor)), range(rp("", databaseDescriptor), rp("0", databaseDescriptor)));
        testGRRKeys(range(rp("7", databaseDescriptor), rp("2", databaseDescriptor)), range(rp("7", databaseDescriptor), rp("", databaseDescriptor)), range(rp("", databaseDescriptor), endOf("1")), range(endOf("1"), rp("2", databaseDescriptor)));
        // full wraps
        testGRRKeys(range(rp("0", databaseDescriptor), rp("0", databaseDescriptor)), range(rp("0", databaseDescriptor), endOf("1")), range(endOf("1"), endOf("6")), range(endOf("6"), rp("", databaseDescriptor)), range(rp("", databaseDescriptor), rp("0", databaseDescriptor)));
        testGRRKeys(range(rp("", databaseDescriptor), rp("", databaseDescriptor)), range(rp("", databaseDescriptor), endOf("1")), range(endOf("1"), endOf("6")), range(endOf("6"), rp("", databaseDescriptor)));
        // wrap on member tokens
        testGRRKeys(range(rp("6", databaseDescriptor), rp("6", databaseDescriptor)), range(rp("6", databaseDescriptor), endOf("6")), range(endOf("6"), rp("", databaseDescriptor)), range(rp("", databaseDescriptor), endOf("1")), range(endOf("1"), rp("6", databaseDescriptor)));
        testGRRKeys(range(rp("6", databaseDescriptor), rp("1", databaseDescriptor)), range(rp("6", databaseDescriptor), endOf("6")), range(endOf("6"), rp("", databaseDescriptor)), range(rp("", databaseDescriptor), rp("1", databaseDescriptor)));
        // end wrapped
        testGRRKeys(range(rp("5", databaseDescriptor), rp("", databaseDescriptor)), range(rp("5", databaseDescriptor), endOf("6")), range(endOf("6"), rp("", databaseDescriptor)));
    }

    @Test
    public void testGRRExactBounds() throws Throwable
    {
        // equal tokens are special cased as non-wrapping for bounds
        testGRR(tokenBounds("0", "0"), tokenBounds("0", "0"));
        // completely empty bounds match everything
        testGRR(tokenBounds("", ""), tokenBounds("", "1"), tokenRange("1", "6"), tokenRange("6", ""));

        // Keys
        // equal tokens are special cased as non-wrapping for bounds
        testGRRKeys(bounds(rp("0", databaseDescriptor), rp("0", databaseDescriptor)), bounds(rp("0", databaseDescriptor), rp("0", databaseDescriptor)));
        // completely empty bounds match everything
        testGRRKeys(bounds(rp("", databaseDescriptor), rp("", databaseDescriptor)), bounds(rp("", databaseDescriptor), endOf("1")), range(endOf("1"), endOf("6")), range(endOf("6"), rp("", databaseDescriptor)));
        testGRRKeys(exBounds(rp("", databaseDescriptor), rp("", databaseDescriptor)), range(rp("", databaseDescriptor), endOf("1")), range(endOf("1"), endOf("6")), exBounds(endOf("6"), rp("", databaseDescriptor)));
        testGRRKeys(incExBounds(rp("", databaseDescriptor), rp("", databaseDescriptor)), bounds(rp("", databaseDescriptor), endOf("1")), range(endOf("1"), endOf("6")), exBounds(endOf("6"), rp("", databaseDescriptor)));
    }
}
