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
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.tools.NodeProbe;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "setmutationtrackingconfig", description = "Sets the mutation tracking configuration")
public class SetMutationTrackingConfig extends AbstractCommand
{
    @VisibleForTesting
    protected List<String> args = new ArrayList<>();

    @Parameters(index = "0", arity = "0..1", description = { "Mutation tracking param type.",
                                                          "Possible parameters: " +
                                                          "[background_reconciliation_enabled|background_reconciliation_interval_ms]" })
    public String paramType;

    @Parameters(index = "1", description = "Mutation tracking param value", arity = "0..1")
    public String paramValue;

    @VisibleForTesting
    protected PrintStream out = System.out;

    @Override
    public void execute(NodeProbe probe)
    {
        args = args.isEmpty() ? CommandUtils.concatArgs(paramType, paramValue) : args;
        checkArgument(args.size() == 2, "setmutationtrackingconfig requires param-type and value args.");
        String type = args.get(0);
        String value = args.get(1);

        if (probe.isMutationTrackingDisabled())
        {
            out.println("Mutation tracking is not enabled");
            return;
        }

        switch (type)
        {
            case "background_reconciliation_enabled":
                probe.setMutationTrackingBackgroundReconciliationEnabled(Boolean.parseBoolean(value));
                break;
            case "background_reconciliation_interval_ms":
                probe.setMutationTrackingBackgroundReconciliationIntervalMilliseconds(Long.parseLong(value));
                break;
            default:
                throw new IllegalArgumentException("Unknown parameter: " + type);
        }
    }
}
