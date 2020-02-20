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
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.NativeLibrary;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.runner.IterationType;


public class PerfEventProfiler implements InternalProfiler
{
    private final File output;
    private final String event;

    private long pid = -1;
    private int iterNum = 0;
    private Process process;

    public PerfEventProfiler(String optString)
    {
        String[] parts = optString.split(",");
        Preconditions.checkArgument(parts.length == 1 || parts.length == 2);

        output = new File(parts[0] + '.' + NativeLibrary.getProcessID());
        if (output.exists())
            Preconditions.checkArgument(!output.isDirectory());
        else
            FileUtils.createDirectory(output.getParentFile());

        event = parts.length == 2 ? parts[1] : "branch-misses";
    }

    public void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams)
    {
        if (iterationParams.getType() == IterationType.WARMUP)
            return;

        if (iterNum++ > 0)
            return;

        pid = NativeLibrary.getProcessID();
        System.out.println(String.format("Setting up %s for %s", getClass().getSimpleName(), pid));
        System.out.println("recording " + event);
        System.out.println(String.format("Saving perf data to %s", output.getParentFile().getAbsolutePath()));
        ProcessBuilder builder = new ProcessBuilder("sudo",
                                                    "perf",
                                                    "record",
                                                    "-g",
                                                    "-e", event,
                                                    "-p", Long.toString(pid),
                                                    "-o", output.getAbsolutePath() + ".perf",
                                                    "sleep", "30");
        builder.inheritIO();
        System.out.println("Starting profiler with command: " + Joiner.on(" ").join(builder.command()));
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

    private static void waitOnProcess(Process process)
    {
        try
        {
            int retcode = process.waitFor();
            if (retcode != 0)
            {
                StringWriter writer = new StringWriter();
                IOUtils.copy(process.getErrorStream(), writer);
                System.out.println("profiler failed: " + writer.toString());
                throw new RuntimeException();
            }
        }
        catch (InterruptedException | IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void waitOnProcess(ProcessBuilder process)
    {
        try
        {
            waitOnProcess(process.start());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public Collection<? extends Result> afterIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams, IterationResult result)
    {
        if (iterationParams.getType() == IterationType.WARMUP)
            return Collections.emptyList();

        if (iterNum == iterationParams.getCount())
        {
            waitOnProcess(process);
            System.out.println(String.format("Getting symbols for %s", pid));
            waitOnProcess(new ProcessBuilder("create-java-perf-map.sh", Long.toString(pid)));
            System.out.println("Sampling and symbol data dumped");
            System.out.println(String.format("Run `perf report -n -i %s -g` to explore", output.getAbsolutePath() + ".perf"));
            System.out.println(String.format("Or `perf script -i %s | ~/FlameGraph/stackcollapse-perf.pl > %s` to export data",
                                             output.getAbsolutePath() + ".perf", output.getAbsolutePath()));
        }

        return Collections.emptyList();
    }

    public String getDescription()
    {
        return "Perf " + event;
    }
}
