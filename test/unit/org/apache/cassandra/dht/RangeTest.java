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
package org.apache.cassandra.dht;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import static java.util.Arrays.asList;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.LocatorConfig;
import org.apache.cassandra.service.StorageService;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.RowPosition;
import static org.apache.cassandra.Util.range;


public class RangeTest
{
    public static final DatabaseDescriptor databaseDescriptor = DatabaseDescriptor.createMain(false);

    @Test
    public void testContains()
    {
        Range left = new Range(new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        assert !left.contains(new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()));
        assert left.contains(new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()));
        assert left.contains(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()));
        assert !left.contains(new BigIntegerToken("101", databaseDescriptor.getLocatorConfig().getPartitioner()));
    }

    @Test
    public void testContainsWrapping()
    {
        Range range = new Range(new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        assert range.contains(new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()));
        assert range.contains(new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()));
        assert range.contains(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()));
        assert range.contains(new BigIntegerToken("101", databaseDescriptor.getLocatorConfig().getPartitioner()));

        range = new Range(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        assert range.contains(new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()));
        assert !range.contains(new BigIntegerToken("1", databaseDescriptor.getLocatorConfig().getPartitioner()));
        assert !range.contains(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()));
        assert range.contains(new BigIntegerToken("200", databaseDescriptor.getLocatorConfig().getPartitioner()));
    }

    @Test
    public void testContainsRange()
    {
        Range one = new Range(new BigIntegerToken("2", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range two = new Range(new BigIntegerToken("2", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("5", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range thr = new Range(new BigIntegerToken("5", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range fou = new Range(new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("12", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());

        assert one.contains(two);
        assert one.contains(thr);
        assert !one.contains(fou);

        assert !two.contains(one);
        assert !two.contains(thr);
        assert !two.contains(fou);

        assert !thr.contains(one);
        assert !thr.contains(two);
        assert !thr.contains(fou);

        assert !fou.contains(one);
        assert !fou.contains(two);
        assert !fou.contains(thr);
    }

    @Test
    public void testContainsRangeWrapping()
    {
        Range one = new Range(new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("2", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range two = new Range(new BigIntegerToken("5", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("3", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range thr = new Range(new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("12", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range fou = new Range(new BigIntegerToken("2", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("6", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range fiv = new Range(new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());

        assert !one.contains(two);
        assert one.contains(thr);
        assert !one.contains(fou);

        assert two.contains(one);
        assert two.contains(thr);
        assert !two.contains(fou);

        assert !thr.contains(one);
        assert !thr.contains(two);
        assert !thr.contains(fou);

        assert !fou.contains(one);
        assert !fou.contains(two);
        assert !fou.contains(thr);

        assert fiv.contains(one);
        assert fiv.contains(two);
        assert fiv.contains(thr);
        assert fiv.contains(fou);
    }

    @Test
    public void testContainsRangeOneWrapping()
    {
        Range wrap1 = new Range(new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range wrap2 = new Range(new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("2", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());

        Range nowrap1 = new Range(new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("2", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range nowrap2 = new Range(new BigIntegerToken("2", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range nowrap3 = new Range(new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());

        assert wrap1.contains(nowrap1);
        assert wrap1.contains(nowrap2);
        assert wrap1.contains(nowrap3);

        assert wrap2.contains(nowrap1);
        assert !wrap2.contains(nowrap2);
        assert wrap2.contains(nowrap3);
    }

    @Test
    public void testIntersects()
    {
        Range all = new Range(new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()); // technically, this is a wrapping range
        Range one = new Range(new BigIntegerToken("2", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range two = new Range(new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("8", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range not = new Range(new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("12", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());

        assert all.intersects(one);
        assert all.intersects(two);

        assert one.intersects(two);
        assert two.intersects(one);

        assert !one.intersects(not);
        assert !not.intersects(one);

        assert !two.intersects(not);
        assert !not.intersects(two);
    }

    @Test
    public void testIntersectsWrapping()
    {
        Range onewrap = new Range(new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("2", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range onecomplement = new Range(onewrap.right, onewrap.left, databaseDescriptor.getLocatorConfig().getPartitioner());
        Range onestartswith = new Range(onewrap.left, new BigIntegerToken("12", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range oneendswith = new Range(new BigIntegerToken("1", databaseDescriptor.getLocatorConfig().getPartitioner()), onewrap.right, databaseDescriptor.getLocatorConfig().getPartitioner());
        Range twowrap = new Range(new BigIntegerToken("5", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("3", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range not = new Range(new BigIntegerToken("2", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("6", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());

        assert !onewrap.intersects(onecomplement);
        assert onewrap.intersects(onestartswith);
        assert onewrap.intersects(oneendswith);

        assert onewrap.intersects(twowrap);
        assert twowrap.intersects(onewrap);

        assert !onewrap.intersects(not);
        assert !not.intersects(onewrap);

        assert twowrap.intersects(not);
        assert not.intersects(twowrap);
    }

    static <T extends RingPosition> void assertIntersection(Range one, Range two, Range<T> ... ranges)
    {
        Set<Range<T>> correct = Range.rangeSet(ranges);
        Set<Range> result1 = one.intersectionWith(two);
        assert result1.equals(correct) : String.format("%s != %s",
                                                       StringUtils.join(result1, ","),
                                                       StringUtils.join(correct, ","));
        Set<Range> result2 = two.intersectionWith(one);
        assert result2.equals(correct) : String.format("%s != %s",
                                                       StringUtils.join(result2, ","),
                                                       StringUtils.join(correct, ","));
    }

    private void assertNoIntersection(Range wraps1, Range nowrap3)
    {
        assertIntersection(wraps1, nowrap3);
    }

    @Test
    public void testIntersectionWithAll()
    {
        Range all0 = new Range(new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range all10 = new Range(new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range all100 = new Range(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range all1000 = new Range(new BigIntegerToken("1000", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("1000", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range wraps = new Range(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());

        assertIntersection(all0, wraps, wraps);
        assertIntersection(all10, wraps, wraps);
        assertIntersection(all100, wraps, wraps);
        assertIntersection(all1000, wraps, wraps);
    }

    @Test
    public void testIntersectionContains()
    {
        Range wraps1 = new Range(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range wraps2 = new Range(new BigIntegerToken("90", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("20", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range wraps3 = new Range(new BigIntegerToken("90", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range nowrap1 = new Range(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("110", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range nowrap2 = new Range(new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range nowrap3 = new Range(new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("9", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());

        assertIntersection(wraps1, wraps2, wraps1);
        assertIntersection(wraps3, wraps2, wraps3);

        assertIntersection(wraps1, nowrap1, nowrap1);
        assertIntersection(wraps1, nowrap2, nowrap2);
        assertIntersection(nowrap2, nowrap3, nowrap3);

        assertIntersection(wraps1, wraps1, wraps1);
        assertIntersection(nowrap1, nowrap1, nowrap1);
        assertIntersection(nowrap2, nowrap2, nowrap2);
        assertIntersection(wraps3, wraps3, wraps3);
    }

    @Test
    public void testNoIntersection()
    {
        Range wraps1 = new Range(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range wraps2 = new Range(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range nowrap1 = new Range(new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range nowrap2 = new Range(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("200", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range nowrap3 = new Range(new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());

        assertNoIntersection(wraps1, nowrap3);
        assertNoIntersection(wraps2, nowrap1);
        assertNoIntersection(nowrap1, nowrap2);
    }

    @Test
    public void testIntersectionOneWraps()
    {
        Range wraps1 = new Range(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range wraps2 = new Range(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range nowrap1 = new Range(new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("200", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range nowrap2 = new Range(new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());

        assertIntersection(wraps1,
                           nowrap1,
                           new Range(new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()),
                           new Range(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("200", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
        assertIntersection(wraps2,
                           nowrap1,
                           new Range(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("200", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
        assertIntersection(wraps1,
                           nowrap2,
                           new Range(new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
    }

    @Test
    public void testIntersectionTwoWraps()
    {
        Range wraps1 = new Range(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("20", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range wraps2 = new Range(new BigIntegerToken("120", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("90", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range wraps3 = new Range(new BigIntegerToken("120", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("110", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range wraps4 = new Range(new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range wraps5 = new Range(new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("1", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
        Range wraps6 = new Range(new BigIntegerToken("30", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());

        assertIntersection(wraps1,
                           wraps2,
                           new Range(new BigIntegerToken("120", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("20", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
        assertIntersection(wraps1,
                           wraps3,
                           new Range(new BigIntegerToken("120", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("20", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()),
                           new Range(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("110", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
        assertIntersection(wraps1,
                           wraps4,
                           new Range(new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("20", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()),
                           new Range(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("0", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
        assertIntersection(wraps1,
                           wraps5,
                           new Range(new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("20", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()),
                           new Range(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("1", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
        assertIntersection(wraps1,
                           wraps6,
                           new Range(new BigIntegerToken("100", databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken("10", databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner()));
    }

    @Test
    public void testByteTokensCompare()
    {
        Token t1 = new BytesToken(ByteBuffer.wrap(new byte[] { 1,2,3 }), databaseDescriptor.getLocatorConfig().getPartitioner());
        Token t2 = new BytesToken(ByteBuffer.wrap(new byte[] { 1,2,3 }), databaseDescriptor.getLocatorConfig().getPartitioner());
        Token t3 = new BytesToken(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}), databaseDescriptor.getLocatorConfig().getPartitioner());

        assert t1.compareTo(t2) == 0;
        assert t1.compareTo(t3) < 0;
        assert t3.compareTo(t1) > 0;
        assert t1.compareTo(t1) == 0;

        Token t4 = new BytesToken(new byte[] { 1,2,3 }, databaseDescriptor.getLocatorConfig().getPartitioner());
        Token t5 = new BytesToken(new byte[] { 4,5,6,7 }, databaseDescriptor.getLocatorConfig().getPartitioner());

        assert t4.compareTo(t5) < 0;
        assert t5.compareTo(t4) > 0;
        assert t1.compareTo(t4) == 0;
    }

    private Range makeRange(String token1, String token2)
    {
        return new Range(new BigIntegerToken(token1, databaseDescriptor.getLocatorConfig().getPartitioner()), new BigIntegerToken(token2, databaseDescriptor.getLocatorConfig().getPartitioner()), databaseDescriptor.getLocatorConfig().getPartitioner());
    }

    private Set<Range> makeRanges(String[][] tokenPairs)
    {
        Set<Range> ranges = new HashSet<Range>();
        for (int i = 0; i < tokenPairs.length; ++i)
            ranges.add(makeRange(tokenPairs[i][0], tokenPairs[i][1]));
        return ranges;
    }

    private void checkDifference(Range oldRange, String[][] newTokens, String[][] expected)
    {
        Set<Range> ranges = makeRanges(newTokens);
        for (Range newRange : ranges)
        {
            Set<Range> diff = oldRange.differenceToFetch(newRange);
            assert diff.equals(makeRanges(expected)) : "\n" +
                                                       "Old range: " + oldRange.toString() + "\n" +
                                                       "New range: " + newRange.toString() + "\n" +
                                                       "Difference: (result) " + diff.toString() + " != " + makeRanges(expected) + " (expected)";
        }
    }

    @Test
    public void testDifferenceToFetchNoWrap()
    {
        Range oldRange = makeRange("10", "40");

        // New range is entirely contained
        String[][] newTokens1 = { { "20", "30" }, { "10", "20" }, { "10", "40" }, { "20", "40" } };
        String[][] expected1 = { };
        checkDifference(oldRange, newTokens1, expected1);

        // Right half of the new range is needed
        String[][] newTokens2 = { { "10", "50" }, { "20", "50" }, { "40", "50" } };
        String[][] expected2 = { { "40", "50" } };
        checkDifference(oldRange, newTokens2, expected2);

        // Left half of the new range is needed
        String[][] newTokens3 = { { "0", "10" }, { "0", "20" }, { "0", "40" } };
        String[][] expected3 = { { "0", "10" } };
        checkDifference(oldRange, newTokens3, expected3);

        // Parts on both ends of the new range are needed
        String[][] newTokens4 = { { "0", "50" } };
        String[][] expected4 = { { "0", "10" }, { "40", "50" } };
        checkDifference(oldRange, newTokens4, expected4);
    }

    @Test
    public void testDifferenceToFetchBothWrap()
    {
        Range oldRange = makeRange("1010", "40");

        // New range is entirely contained
        String[][] newTokens1 = { { "1020", "30" }, { "1010", "20" }, { "1010", "40" }, { "1020", "40" } };
        String[][] expected1 = { };
        checkDifference(oldRange, newTokens1, expected1);

        // Right half of the new range is needed
        String[][] newTokens2 = { { "1010", "50" }, { "1020", "50" }, { "1040", "50" } };
        String[][] expected2 = { { "40", "50" } };
        checkDifference(oldRange, newTokens2, expected2);

        // Left half of the new range is needed
        String[][] newTokens3 = { { "1000", "10" }, { "1000", "20" }, { "1000", "40" } };
        String[][] expected3 = { { "1000", "1010" } };
        checkDifference(oldRange, newTokens3, expected3);

        // Parts on both ends of the new range are needed
        String[][] newTokens4 = { { "1000", "50" } };
        String[][] expected4 = { { "1000", "1010" }, { "40", "50" } };
        checkDifference(oldRange, newTokens4, expected4);
    }

    @Test
    public void testDifferenceToFetchOldWraps()
    {
        Range oldRange = makeRange("1010", "40");

        // New range is entirely contained
        String[][] newTokens1 = { { "0", "30" }, { "0", "40" }, { "10", "40" } };
        String[][] expected1 = { };
        checkDifference(oldRange, newTokens1, expected1);

        // Right half of the new range is needed
        String[][] newTokens2 = { { "0", "50" }, { "10", "50" }, { "40", "50" } };
        String[][] expected2 = { { "40", "50" } };
        checkDifference(oldRange, newTokens2, expected2);

        // Whole range is needed
        String[][] newTokens3 = { { "50", "90" } };
        String[][] expected3 = { { "50", "90" } };
        checkDifference(oldRange, newTokens3, expected3);

        // Both ends of the new range overlaps the old range
        String[][] newTokens4 = { { "10", "1010" }, { "40", "1010" }, { "10", "1030" }, { "40", "1030" } };
        String[][] expected4 = { { "40", "1010" } };
        checkDifference(oldRange, newTokens4, expected4);

        // Only RHS of the new range overlaps the old range
        String[][] newTokens5 = { { "60", "1010" }, { "60", "1030" } };
        String[][] expected5 = { { "60", "1010" } };
        checkDifference(oldRange, newTokens5, expected5);
    }

    @Test
    public void testDifferenceToFetchNewWraps()
    {
        Range oldRange = makeRange("0", "40");

        // Only the LHS of the new range is needed
        String[][] newTokens1 = { { "1010", "0" }, { "1010", "10" }, { "1010", "40" } };
        String[][] expected1 = { { "1010", "0" } };
        checkDifference(oldRange, newTokens1, expected1);

        // Both ends of the new range are needed
        String[][] newTokens2 = { { "1010", "50" } };
        String[][] expected2 = { { "1010", "0" }, { "40", "50" } };
        checkDifference(oldRange, newTokens2, expected2);

        oldRange = makeRange("20", "40");

        // Whole new range is needed
        String[][] newTokens3 = { { "1010", "0" } };
        String[][] expected3 = { { "1010", "0" } };
        checkDifference(oldRange, newTokens3, expected3);

        // Whole new range is needed (matching endpoints)
        String[][] newTokens4 = { { "1010", "20" } };
        String[][] expected4 = { { "1010", "20" } };
        checkDifference(oldRange, newTokens4, expected4);

        // Only RHS of new range is needed
        String[][] newTokens5 = { { "30", "0" }, { "40", "0" } };
        String[][] expected5 = { { "40", "0" } };
        checkDifference(oldRange, newTokens5, expected5);

        // Only RHS of new range is needed (matching endpoints)
        String[][] newTokens6 = { { "30", "20" }, { "40", "20" } };
        String[][] expected6 = { { "40", "20" } };
        checkDifference(oldRange, newTokens6, expected6);
    }

    private <T extends RingPosition> void assertNormalize(List<Range<T>> input, List<Range<T>> expected)
    {
        List<Range<T>> result = Range.normalize(input);
        assert result.equals(expected) : "Expecting " + expected + " but got " + result;
    }

    @Test
    public void testNormalizeNoop()
    {
        List<Range<RowPosition>> l;

        l = asList(range("1", "3", databaseDescriptor), range("4", "5", databaseDescriptor));
        assertNormalize(l, l);
    }

    @Test
    public void testNormalizeSimpleOverlap()
    {
        List<Range<RowPosition>> input, expected;

        input = asList(range("1", "4", databaseDescriptor), range("3", "5", databaseDescriptor));
        expected = asList(range("1", "5", databaseDescriptor));
        assertNormalize(input, expected);

        input = asList(range("1", "4", databaseDescriptor), range("1", "4", databaseDescriptor));
        expected = asList(range("1", "4", databaseDescriptor));
        assertNormalize(input, expected);
    }

    @Test
    public void testNormalizeSort()
    {
        List<Range<RowPosition>> input, expected;

        input = asList(range("4", "5", databaseDescriptor), range("1", "3", databaseDescriptor));
        expected = asList(range("1", "3", databaseDescriptor), range("4", "5", databaseDescriptor));
        assertNormalize(input, expected);
    }

    @Test
    public void testNormalizeUnwrap()
    {
        List<Range<RowPosition>> input, expected;

        input = asList(range("9", "2", databaseDescriptor));
        expected = asList(range("", "2", databaseDescriptor), range("9", "", databaseDescriptor));
        assertNormalize(input, expected);
    }

    @Test
    public void testNormalizeComplex()
    {
        List<Range<RowPosition>> input, expected;

        input = asList(range("8", "2", databaseDescriptor), range("7", "9", databaseDescriptor), range("4", "5", databaseDescriptor));
        expected = asList(range("", "2", databaseDescriptor), range("4", "5", databaseDescriptor), range("7", "", databaseDescriptor));
        assertNormalize(input, expected);

        input = asList(range("5", "9", databaseDescriptor), range("2", "5", databaseDescriptor));
        expected = asList(range("2", "9", databaseDescriptor));
        assertNormalize(input, expected);

        input = asList(range ("", "1", databaseDescriptor), range("9", "2", databaseDescriptor), range("4", "5", databaseDescriptor), range("", "", databaseDescriptor));
        expected = asList(range("", "", databaseDescriptor));
        assertNormalize(input, expected);

        input = asList(range ("", "1", databaseDescriptor), range("1", "4", databaseDescriptor), range("4", "5", databaseDescriptor), range("5", "", databaseDescriptor));
        expected = asList(range("", "", databaseDescriptor));
        assertNormalize(input, expected);
    }
}
