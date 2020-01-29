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

import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class Curve
{
    private final double[] points;

    public Curve(double... points)
    {
        Preconditions.checkArgument(points.length > 1);
        this.points = points;
    }

    public double value(double pos)
    {
        Preconditions.checkArgument(pos >= 0.0);
        Preconditions.checkArgument(pos <= 1.0);

        if (pos == 1.0)
            return points[points.length - 1];

        double interval = 1.0 / (points.length - 1);
        int idx = (int) (pos / interval);
        double frac = (pos - (interval * idx)) / interval;
        double start = points[idx];
        double end = points[idx+1];
        double range = end - start;
        double delta = (range * frac);
        return points[idx] + delta;
    }

    public int valueInt(double pos)
    {
        return (int) (value(pos) + 0.5);
    }

    private static Set<String> parseSteps(String[] args, ChainedOptionsBuilder builder, Map<String, Integer> defaultSteps)
    {
        Map<String, Integer> steppedParams = new HashMap<>(defaultSteps);
        for (int i=0; i<args.length; i++)
        {
            if (args[i].equals("-steps"))
            {
                Preconditions.checkArgument(i + 3 <= args.length);
                String param = args[++i];
                int steps = Integer.parseInt(args[++i]);
                Preconditions.checkArgument(steps > 1);
                steppedParams.put(param, steps);
            }
        }
        Options tempOpts = builder.build();

        for (Map.Entry<String, Integer> entry : steppedParams.entrySet())
        {
            String param = entry.getKey();
            if (tempOpts.getParameter(param).hasValue())
            {
                System.out.println(param + " explicitly set, not setting sequence values");
                continue;
            }
            int steps = entry.getValue();
            double interval = 1.0 / (steps - 1);
            String[] values = new String[steps];
            for (int s=0; s<steps; s++)
                values[s] = Double.toString(interval * s);
            builder.param(param, values);
        }

        return steppedParams.keySet();
    }

    public interface CurveExpander
    {
        void expand(String name, double value, Map<String, String> params, Map<String, String> dst);
    }

    private static void expandCurves(Set<String> steppedParams, Options options, CurveExpander expander) throws Exception
    {
        if (steppedParams.isEmpty())
            return;

        if (!options.getResult().hasValue())
            return;

        if (options.getResultFormat().orElse(null) != ResultFormatType.JSON)
            return;

        String name = options.getResult().get();
        String tmpName = name + ".tmp";
        FileUtils.renameWithConfirm(name, tmpName);

        JSONParser parser = new JSONParser();
        Reader reader = new FileReader(tmpName);
        JSONArray root = (JSONArray) parser.parse(reader);
        for (Object obj: root)
        {
            JSONObject jobj = (JSONObject) obj;
            JSONObject expandedParams = new JSONObject();

            JSONObject params = (JSONObject) jobj.get("params");
            for (String steppedParam: steppedParams)
            {
                double pos = Double.parseDouble((String) params.get(steppedParam));
                JSONObject expanded = new JSONObject();
                expander.expand(steppedParam, pos, (Map<String, String>) jobj.get("params"), expanded);
                if (!expanded.isEmpty())
                    expandedParams.put(steppedParam, expanded);
            }
            if (!expandedParams.isEmpty())
                jobj.put("expandedParams", expandedParams);
        }

        try (FileWriter writer = new FileWriter(name))
        {
            root.writeJSONString(writer);
        }

        FileUtils.delete(tmpName);
    }

    public static int numSteps(Curve curve, int resolution)
    {
        return (curve.points.length * resolution) + 1;
    }

    public static void mainHelper(String[] args, Class<?> clazz, CurveExpander expander, Map<String, Integer> defaultSteps) throws Exception
    {
        ChainedOptionsBuilder builder = new OptionsBuilder();

        builder.include(clazz.getSimpleName());
        builder.parent(new CommandLineOptions(args));
        Set<String> steppedParams = parseSteps(args, builder, defaultSteps);
        Options options = builder.build();

        new Runner(options).run();

        expandCurves(steppedParams, options, expander);
    }
}
