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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import joptsimple.OptionParser;
import joptsimple.OptionSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 250, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 1, time = 250, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 1)
//@Fork(value = 1, jvmArgs = {"-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"})
//@Threads(32)
@State(Scope.Benchmark)
public class TestBench
{
    private static final Logger logger = LoggerFactory.getLogger(TestBench.class);

    private static final Curve PARTITON_CURVE = new Curve(256, 128, 64, 32);
    private static final Curve ROW_CURVE =      new Curve(32, 128, 256, 128);
    private static final Curve VAL_SIZE_CURVE = new Curve(8, 32, 128, 256);
    private static final Map<String, Integer> DEFAULT_STEPS = ImmutableMap.<String, Integer>builder()
                                                                          .put("point", 16).build();

    @Param({"0.0"})
    double point;

    @State(Scope.Benchmark)
    public static class BenchState
    {
        int partitionCount;
        int rowCount;
        int valueSize;

        @Setup(Level.Trial)
        public void setup(TestBench bench)
        {
            partitionCount = PARTITON_CURVE.valueInt(bench.point);
            rowCount = ROW_CURVE.valueInt(bench.point);
            valueSize = VAL_SIZE_CURVE.valueInt(bench.point);
        }
    }

    @Benchmark
    public void doSomething(BenchState state)
    {

    }

    static void expand(String name, double value, Map<String, String> params, Map<String, String> dst)
    {
        Preconditions.checkArgument(name.equals("point"));

        dst.put("partitionCount", Integer.toString(PARTITON_CURVE.valueInt(value)));
        dst.put("rowCount", Integer.toString(ROW_CURVE.valueInt(value)));
        dst.put("valueSize", Integer.toString(VAL_SIZE_CURVE.valueInt(value)));
    }

    public static void main(String... args) throws Exception
    {
        Curve.mainHelper(args, TestBench.class, TestBench::expand, DEFAULT_STEPS);
    }
}
