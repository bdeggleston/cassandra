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

package org.apache.cassandra.test.microbench.profile;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.NativeLibrary;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.runner.IterationType;

public class BPFTraceProfiler implements InternalProfiler
{
    private final File output;
    private long pid;
    private int iterNum = 0;
    private int hz = 997;

    private Process process;
    public BPFTraceProfiler()
    {
        output = new File(System.getProperty("bpftrace_profiler.out", "bcc_out"));
        FileUtils.createDirectory(output);
    }

    public synchronized void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams)
    {
        if (iterationParams.getType() == IterationType.WARMUP)
            return;

        if (iterNum++ > 0)
            return;

        pid = NativeLibrary.getProcessID();
        System.out.println(String.format("Setting up %s for %s", getClass().getSimpleName(), pid));

        try
        {
            System.out.println(String.format("Dumping symbols s for %s", pid));
            new ProcessBuilder("create-java-perf-map.sh", Long.toString(pid)).start().waitFor();
        }
        catch (InterruptedException | IOException e)
        {
            System.out.println(e);
            throw new RuntimeException(e);
        }

        String bpft = String.format("profile:hz:%s /pid == %s/ {@[kstack, ustack] = count();}", hz, pid);

        System.out.println("bpftrace string: " + bpft);
        ProcessBuilder builder = new ProcessBuilder("sudo",
                                                    "BCC_KERNEL_SOURCE=/usr/src/kernels/3.10.0-1062.9.1.el7.x86_64",
                                                    "bpftrace",
                                                    "--unsafe",
                                                    "-e",
                                                    bpft);
        builder.environment().put("BCC_KERNEL_SOURCE", "/usr/src/kernels/3.10.0-1062.7.1.el7.x86_64");
        builder.redirectOutput(new File(output, "out-" + output.list().length + ".txt"));
        try
        {
            process = builder.start();
        }
        catch (IOException e)
        {
            System.out.println(e);
            throw new RuntimeException(e);
        }
    }

    public synchronized Collection<? extends Result> afterIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams, IterationResult result)
    {
        if (iterationParams.getType() == IterationType.WARMUP)
            return Collections.emptyList();

        if (iterNum == iterationParams.getCount())
        {
            try
            {
                System.out.println(String.format("Updating symbols s for %s", pid));
                new ProcessBuilder("create-java-perf-map.sh", Long.toString(pid)).start().waitFor();
            }
            catch (InterruptedException | IOException e)
            {
                System.out.println(e);
                throw new RuntimeException(e);
            }
            System.out.println(String.format("Cleaning up %s for %s", getClass().getSimpleName(), pid));
            process.destroy();
        }

        return Collections.emptyList();
    }

    public String getDescription()
    {
        return "bpftrace stack profiler";
    }
}
