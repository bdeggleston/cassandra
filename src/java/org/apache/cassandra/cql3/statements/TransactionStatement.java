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

package org.apache.cassandra.cql3.statements;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnReference;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

public class TransactionStatement implements CQLStatement
{
    static class NamedSelect
    {
        final String name;
        final SelectStatement select;

        public NamedSelect(String name, SelectStatement select)
        {
            this.name = name;
            this.select = select;
        }
    }

    private final List<NamedSelect> selects;
    private final List<ModificationStatement> updates;
    private final List<ColumnReference> columnReferences;

    public TransactionStatement(List<NamedSelect> selects, List<ModificationStatement> updates, List<ColumnReference> columnReferences)
    {
        this.selects = selects;
        this.updates = updates;
        this.columnReferences = columnReferences;
    }


    @Override
    public void authorize(ClientState state)
    {
        // TODO: this
    }

    @Override
    public void validate(ClientState state)
    {
        // TODO: this
    }

    @Override
    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime)
    {
        // TODO: this
        return null;
    }

    @Override
    public ResultMessage executeLocally(QueryState state, QueryOptions options)
    {
        // TODO: this
        return null;
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        // TODO: this
        return null;
    }

    public static class Parsed extends QualifiedStatement
    {
        private final List<SelectStatement.RawStatement> selects;
        private final List<ModificationStatement.Parsed> updates;
        private final List<Pair<ColumnIdentifier, ColumnCondition.Raw>> conditions;
        private final List<ColumnReference.Raw> columnReferences;

        public Parsed(List<SelectStatement.RawStatement> selects, List<ModificationStatement.Parsed> updates, List<Pair<ColumnIdentifier, ColumnCondition.Raw>> conditions, List<ColumnReference.Raw> columnReferences)
        {
            super(null);
            this.selects = selects;
            this.updates = updates;
            this.conditions = conditions != null ? conditions : Collections.emptyList();
            this.columnReferences = columnReferences;
        }

        @Override
        public void setKeyspace(ClientState state)
        {
            selects.forEach(select -> select.setKeyspace(state));
            updates.forEach(update -> update.setKeyspace(state));
        }

        @Override
        public CQLStatement prepare(ClientState state)
        {
            checkTrue(bindVariables.isEmpty(), "TODO: support bound variables");
            checkFalse(selects.isEmpty() && updates.isEmpty(), "Transaction is empty");

            List<NamedSelect> preparedSelects = new ArrayList<>(selects.size());
            Map<String, SelectStatement> selectMap = new HashMap<>();
            Set<String> selectNames = new HashSet<>();

            // TODO: confirm no custom timestamps
            for (int i=0, mi= selects.size(); i<mi; i++)
            {
                SelectStatement.RawStatement select = selects.get(i);
                String name = select.parameters.refName;
                if (name == null)
                    name = "select" + i;

                checkTrue(selectNames.add(name), "The name '%s' has been used by another select", name);

                SelectStatement preparedSelect = select.prepare(bindVariables);
                NamedSelect namedSelect = new NamedSelect(name, preparedSelect);
                preparedSelects.add(namedSelect);
                selectMap.put(name, preparedSelect);
            }

            for (ColumnReference.Raw reference : columnReferences)
                reference.resolveReference(selectMap);

            List<ModificationStatement> preparedUpdates = new ArrayList<>(updates.size());
            for (ModificationStatement.Parsed parsed : updates)
            {
                ModificationStatement prepared = parsed.prepare(bindVariables);
                // TODO: visit where clause terms and confirm they're not column references
                preparedUpdates.add(prepared);
            }
            // TODO: instead of materializing partition updates after select statement execution, maybe we could materialize
            //   them without column references, and have the select statements materialize those parts. Tricky part would
            //   be preserving functions... but maybe those could be serialized without literally everything else??

            List<ColumnReference> preparedReferences = new ArrayList<>(columnReferences.size());
            for (ColumnReference.Raw reference : columnReferences)
                preparedReferences.add(reference.prepared());


            return new TransactionStatement(preparedSelects, preparedUpdates, preparedReferences);
        }
    }
}
