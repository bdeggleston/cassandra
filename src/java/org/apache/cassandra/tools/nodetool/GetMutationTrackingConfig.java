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
package org.apache.cassandra.tools.nodetool;

import java.io.PrintStream;

import org.apache.cassandra.tools.NodeProbe;

import picocli.CommandLine.Command;

@Command(name = "getmutationtrackingconfig", description = "Print mutation tracking configurations")
public class GetMutationTrackingConfig extends AbstractCommand
{
    private static PrintStream out = System.out;

    @Override
    public void execute(NodeProbe probe)
    {
        if (probe.isMutationTrackingDisabled())
        {
            out.println("Mutation tracking is not enabled");
            return;
        }

        out.println("background_reconciliation_enabled: " + probe.getMutationTrackingBackgroundReconciliationEnabled());
        out.println("background_reconciliation_interval_ms: " + probe.getMutationTrackingBackgroundReconciliationIntervalMilliseconds());
    }
}
