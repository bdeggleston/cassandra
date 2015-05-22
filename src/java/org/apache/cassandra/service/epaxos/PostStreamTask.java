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

package org.apache.cassandra.service.epaxos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

/**
 * Since execution of instances is suspended during some stream tasks,
 * this works it's way through a range and executes any instances that
 * would have been skipped
 */
public class PostStreamTask implements Runnable
{
    private final EpaxosState state;
    private final Range<Token> range;
    private final UUID cfId;
    private final Scope scope;

    public PostStreamTask(EpaxosState state, Range<Token> range, UUID cfId, Scope scope)
    {
        this.state = state;
        this.range = range;
        this.cfId = cfId;
        this.scope = scope;
    }

    Comparator<UUID> comparator = new Comparator<UUID>()
    {
        @Override
        public int compare(UUID o1, UUID o2)
        {
            return DependencyGraph.comparator.compare(o2, o1);
        }
    };

    @Override
    public void run()
    {
        Iterator<KeyState> iter = state.getKeyStateManager(scope).getStaleKeyStateRange(cfId, range);
        while (iter.hasNext())
        {
            KeyState ks = iter.next();
            List<UUID> activeIds = new ArrayList<>(ks.getActiveInstanceIds());

            // sort ids so most recently created ones will be at the head of the list
            // even though the first committed instance may not be the first to execute,
            // it will be in the first strongly connected component
            Collections.sort(activeIds, comparator);
            for (UUID id: activeIds)
            {
                Instance instance = state.loadInstance(id);
                if (instance != null)
                {
                    if (instance.getState().atLeast(Instance.State.EXECUTED))
                    {
                        // nothing to do here
                        break;
                    }
                    else if (instance.getState().atLeast(Instance.State.COMMITTED))
                    {
                        state.execute(id);
                        break;
                    }
                }
            }
        }
    }
}
