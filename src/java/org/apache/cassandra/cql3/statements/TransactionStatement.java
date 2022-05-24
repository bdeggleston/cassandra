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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import accord.api.Key;
import accord.txn.Keys;
import accord.txn.Txn;
import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnReference;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.transactions.UpdateCondition;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.accord.txn.TxnCondition;
import org.apache.cassandra.service.accord.txn.TxnNamedRead;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnUpdate;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.transport.messages.ResultMessage;

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
    private final List<UpdateCondition> conditions;

    public TransactionStatement(List<NamedSelect> selects, List<ModificationStatement> updates, List<ColumnReference> columnReferences, List<UpdateCondition> conditions)
    {
        this.selects = selects;
        this.updates = updates;
        this.columnReferences = columnReferences;
        this.conditions = conditions;
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

    TxnNamedRead createNamedRead(NamedSelect namedSelect, QueryOptions options)
    {
        SelectStatement select = namedSelect.select;
        ReadQuery readQuery = select.getQuery(options, 0);
        SinglePartitionReadQuery.Group<SinglePartitionReadCommand> selectQuery = (SinglePartitionReadQuery.Group<SinglePartitionReadCommand>) readQuery;
        return new TxnNamedRead(namedSelect.name, Iterables.getOnlyElement(selectQuery.queries));
    }

    TxnRead createRead(QueryOptions options, Consumer<Key> keyConsumer)
    {
        List<TxnNamedRead> reads = new ArrayList<>(selects.size());
        for (NamedSelect select : selects)
        {
            TxnNamedRead read = createNamedRead(select, options);
            keyConsumer.accept(read.key());
            reads.add(read);
        }
        return new TxnRead(reads);
    }

    TxnCondition createCondition(QueryOptions options)
    {
        if (conditions.isEmpty())
            return TxnCondition.NONE;
        if (conditions.size() == 1)
            return conditions.get(0).createCondition(options);

        List<TxnCondition> result = new ArrayList<>(conditions.size());
        for (UpdateCondition condition : conditions)
            result.add(condition.createCondition(options));

        return new TxnCondition.BooleanGroup(TxnCondition.Kind.AND, result);
    }

    List<TxnWrite.Fragment> createWriteFragments(QueryOptions options, Consumer<Key> keyConsumer)
    {
        List<TxnWrite.Fragment> fragments = new ArrayList<>(updates.size());
        int idx = 0;
        for (ModificationStatement modification : updates)
        {
            TxnWrite.Fragment fragment = modification.getTxnWriteFragment(idx++, options);
            keyConsumer.accept(fragment.key);
            fragments.add(fragment);
        }
        return fragments;
    }

    TxnUpdate createUpdate(QueryOptions options, Consumer<Key> keyConsumer)
    {
        return new TxnUpdate(createWriteFragments(options, keyConsumer), createCondition(options));
    }

    Keys toKeys(Set<Key> keySet)
    {
        Key[] keyArray = new Key[keySet.size()];
        keySet.toArray(keyArray);
        Arrays.sort(keyArray);
        return new Keys(keyArray);
    }

    @VisibleForTesting
    public Txn createTxn(QueryOptions options)
    {
        Set<Key> keySet = new HashSet<>();
        TxnRead read = createRead(options, keySet::add);
        if (updates.isEmpty())
        {
            Preconditions.checkState(conditions.isEmpty());
            return new Txn.InMemory(toKeys(keySet), read, TxnQuery.ALL);
        }
        else
        {
            TxnUpdate update = createUpdate(options, keySet::add);
            return new Txn.InMemory(toKeys(keySet), read, TxnQuery.ALL, update);
        }
    }

    @Override
    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime)
    {
        return StorageProxy.instance.txn(createTxn(options));
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

    // TODO: move to ColumnReference
    public interface ReferenceSource
    {

        boolean isPointSelect();
        ColumnMetadata getColumn(String name);

    }

    private static class SelectReferenceSource implements ReferenceSource
    {
        private final SelectStatement statement;
        private final Set<ColumnMetadata> selectedColumns;
        private final TableMetadata metadata;

        public SelectReferenceSource(SelectStatement statement)
        {
            this.statement = statement;
            this.metadata = statement.table;
            Selection selection = statement.getSelection();
            selectedColumns = new HashSet<>(selection.getColumns());
        }

        @Override
        public boolean isPointSelect()
        {
            return Iterables.all(metadata.primaryKeyColumns(), selectedColumns::contains);
        }

        @Override
        public ColumnMetadata getColumn(String name)
        {
            ColumnMetadata column = metadata.getColumn(new ColumnIdentifier(name, true));
            if (column != null)
                checkTrue(selectedColumns.contains(column), "%s refererences a column not included in the select", this);
            return column;
        }
    }

    private static class UpdateReferenceSource implements ReferenceSource
    {
        private final ModificationStatement.Parsed parsed;
        private final TableMetadata metadata;

        public UpdateReferenceSource(ModificationStatement.Parsed parsed)
        {
            this.parsed = parsed;
            this.metadata = Schema.instance.validateTable(parsed.keyspace(), parsed.name());
        }

        @Override
        public boolean isPointSelect()
        {
            // TODO: I believe updates/inserts must always specfiy all primary keys, except maybe for static column updates only
            //   in which case we don't want to allow references to non-static columns
            return true;
        }

        @Override
        public ColumnMetadata getColumn(String name)
        {
            return metadata.getColumn(new ColumnIdentifier(name, true));
        }
    }

    public static class Parsed extends QualifiedStatement
    {
        private final List<SelectStatement.RawStatement> selects;
        private final List<ModificationStatement.Parsed> updates;
        private final List<UpdateCondition.Raw> conditions;
        private final List<ColumnReference.Raw> columnReferences;

        public Parsed(List<SelectStatement.RawStatement> selects, List<ModificationStatement.Parsed> updates, List<UpdateCondition.Raw> conditions, List<ColumnReference.Raw> columnReferences)
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
            Map<String, ReferenceSource> refSources = new HashMap<>();
            Set<String> selectNames = new HashSet<>();

            // TODO: confirm no custom timestamps
            for (int i=0, mi= selects.size(); i<mi; i++)
            {
                // TODO: no multi partition reads
                SelectStatement.RawStatement select = selects.get(i);
                String name = select.parameters.refName;
                if (name == null)
                    name = "select" + i;

                checkTrue(selectNames.add(name), "The name '%s' has been used by another select", name);

                SelectStatement preparedSelect = select.prepare(bindVariables);
                NamedSelect namedSelect = new NamedSelect(name, preparedSelect);
                preparedSelects.add(namedSelect);
                refSources.put(name, new SelectReferenceSource(preparedSelect));
            }

            // check for any read-before-write updates
            for (int i=0, mi=updates.size(); i<mi; i++)
            {
                ModificationStatement.Parsed parsed = updates.get(i);
                String name = parsed.txnReadName;
                if (name != null)
                    refSources.put(name, new UpdateReferenceSource(parsed));
            }

            for (ColumnReference.Raw reference : columnReferences)
                reference.resolveReference(refSources);

            List<ModificationStatement> preparedUpdates = new ArrayList<>(updates.size());
            for (ModificationStatement.Parsed parsed : updates)
            {
                ModificationStatement prepared = parsed.prepare(bindVariables);
                preparedUpdates.add(prepared);

                if (parsed.txnReadName == null)
                    continue;
                // TODO: create select counterparts for named updates
                // TODO: can we borrow placeholder terms for the selection pk?? Test
                preparedSelects.add(new NamedSelect(parsed.txnReadName, ((UpdateStatement) prepared).createSelectForTxn()));
            }

            List<UpdateCondition> preparedConditions = new ArrayList<>(conditions.size());
            for (UpdateCondition.Raw condition : conditions)
                preparedConditions.add(condition.prepare("[txn]", bindVariables));

            List<ColumnReference> preparedReferences = new ArrayList<>(columnReferences.size());
            for (ColumnReference.Raw reference : columnReferences)
                preparedReferences.add(reference.prepared());


            return new TransactionStatement(preparedSelects, preparedUpdates, preparedReferences, preparedConditions);
        }
    }
}
