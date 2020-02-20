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

package org.apache.cassandra.test.microbench;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;

import org.apache.cassandra.utils.MergeIterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 4, time = 20)
@Fork(value = 1,jvmArgsAppend = { "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(32) // make sure this matches the number of _physical_cores_
@State(Scope.Benchmark)
public class MergeIteratorGetBench
{
    private static final List<String> STRINGS = Lists.newArrayList("do", "re", "mi");
    private static final List<Iterator<String>> ONE = Lists.<Iterator<String>>newArrayList(STRINGS.iterator());
    private static final List<Iterator<String>> MANY = Lists.newArrayList(STRINGS.iterator(), STRINGS.iterator());

    private static final MergeIterator.Reducer<String, String> REDUCER = new MergeIterator.Reducer<String, String>()
    {
        private String next = null;
        public void reduce(int idx, String current)
        {
            next = current;

        }

        protected String getReduced()
        {
            return next;
        }
    };

    @Benchmark
    public void manyToOne()
    {
        MergeIterator<String, String> mi = MergeIterator.get(MANY, String::compareTo, REDUCER);
        mi.close();
    }

    @Benchmark
    public void one()
    {
        MergeIterator<String, String> mi = MergeIterator.get(ONE, String::compareTo, REDUCER);
        mi.close();
    }
}
