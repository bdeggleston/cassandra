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

package org.apache.cassandra.dht;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.utils.Pair;

/**
 * Splits a range at the boundaries of a set of {@link NormalizedRanges}.
 *
 * After splitting, each sub-range is either entirely within or entirely outside the boundary set.
 * Used by both MigrationRouter (tracked/untracked routing) and CoordinationPlanIterator
 * (failover state boundary splitting).
 */
public class RangeSplitter
{
    /**
     * Split a range at the boundaries of the given {@link NormalizedRanges} set.
     *
     * Returns a list of contiguous sub-ranges covering the original range. Each sub-range is either
     * entirely within a boundary range (intersection) or entirely outside all boundary ranges (gap).
     * Callers can determine which is which via {@link NormalizedRanges#intersects(RingPosition)}.
     *
     * If the range does not cross any boundary, returns a single-element list containing the original range.
     *
     * @param range the range to split
     * @param boundaries the set of ranges to split at
     * @return ordered list of sub-ranges covering the original range
     */
    public static List<AbstractBounds<PartitionPosition>> splitAtBoundaries(AbstractBounds<PartitionPosition> range,
                                                                            List<Range<Token>> boundaries)
    {
        List<AbstractBounds<PartitionPosition>> result = new ArrayList<>();
        AbstractBounds<PartitionPosition> remainder = range;

        for (Range<Token> boundary : boundaries)
        {
            if (addGapBefore(result, remainder, boundary))
            {
                remainder = null;
                break;
            }

            Pair<AbstractBounds<PartitionPosition>, AbstractBounds<PartitionPosition>> split =
                Range.intersectionAndRemainder(remainder, boundary);

            if (split.left != null)
                result.add(split.left);

            remainder = split.right;
            if (remainder == null)
                break;
        }

        if (remainder != null)
            result.add(remainder);

        return result;
    }

    /**
     * If the remainder starts before the boundary range, add the gap (the portion before the boundary)
     * to the result.
     *
     * @return true if the remainder ends before the boundary (no intersection possible, remainder fully consumed)
     */
    private static boolean addGapBefore(List<AbstractBounds<PartitionPosition>> result,
                                        AbstractBounds<PartitionPosition> remainder,
                                        Range<Token> boundary)
    {
        Token boundaryStart = boundary.left;
        Token remainderStart = remainder.left.getToken();
        Token remainderEnd = remainder.right.getToken();

        if (remainderStart.compareTo(boundaryStart) >= 0)
            return false; // No gap -- remainder starts at or after boundary

        // Check if remainder ends before boundary starts
        if (!remainderEnd.isMinimum() && remainderEnd.compareTo(boundaryStart) <= 0)
        {
            result.add(remainder);
            return true;
        }

        // Add the gap before boundary
        AbstractBounds<PartitionPosition> gap = remainder.withNewRight(boundaryStart.maxKeyBound());
        if (!gap.left.equals(gap.right))
            result.add(gap);

        return false;
    }
}
