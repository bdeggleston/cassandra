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

package org.apache.cassandra.locator;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public class ReplicaListTest extends ReplicaCollectionTest
{

    @Test
    public void removeEndpoint()
    {
        ReplicaList rlist = new ReplicaList();
        rlist.add(ReplicaUtils.full(EP1, range(0, 100)));
        rlist.add(ReplicaUtils.full(EP2, range(0, 100)));
        rlist.add(ReplicaUtils.full(EP3, range(0, 100)));

        Assert.assertTrue(rlist.containsEndpoint(EP1));
        Assert.assertEquals(3, rlist.size());
        rlist.removeEndpoint(EP1);
        Assert.assertFalse(rlist.containsEndpoint(EP1));
        Assert.assertEquals(2, rlist.size());
    }

    @Test
    public void readPrioritizationFindFull()
    {
        Range<Token> rrange = range(0, 100);
        Replica r1 = ReplicaUtils.full(EP1, rrange);
        Replica r2 = ReplicaUtils.full(EP2, rrange);
        Replica r3 = ReplicaUtils.trans(EP3,rrange);

        ReplicaList actual = new ReplicaList(Lists.newArrayList(r1, r2, r3));
        actual.prioritizeForRead();
        ReplicaList expected = new ReplicaList(Lists.newArrayList(r1, r3, r2));
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void readPrioritizationFindTrans()
    {
        Range<Token> rrange = range(0, 100);
        Replica r1 = ReplicaUtils.full(EP1, rrange);
        Replica r2 = ReplicaUtils.trans(EP2, rrange);
        Replica r3 = ReplicaUtils.trans(EP3,rrange);

        ReplicaList actual = new ReplicaList(Lists.newArrayList(r2, r3, r1));
        actual.prioritizeForRead();
        ReplicaList expected = new ReplicaList(Lists.newArrayList(r1, r2, r3));
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void readPrioritizationNoop()
    {
        Range<Token> rrange = range(0, 100);
        Replica r1 = ReplicaUtils.full(EP1, rrange);
        Replica r2 = ReplicaUtils.trans(EP2, rrange);
        Replica r3 = ReplicaUtils.trans(EP3,rrange);

        ReplicaList actual = new ReplicaList(Lists.newArrayList(r1, r2, r3));
        actual.prioritizeForRead();
        ReplicaList expected = new ReplicaList(Lists.newArrayList(r1, r2, r3));
        Assert.assertEquals(expected, actual);
    }

    @Test (expected = IllegalStateException.class)
    public void readPrioritizationNoFullFailure()
    {
        Range<Token> rrange = range(0, 100);
        Replica r1 = ReplicaUtils.trans(EP1, rrange);
        Replica r2 = ReplicaUtils.trans(EP2, rrange);
        Replica r3 = ReplicaUtils.trans(EP3,rrange);

        ReplicaList actual = new ReplicaList(Lists.newArrayList(r1, r2, r3));
        actual.prioritizeForRead();
    }
}
