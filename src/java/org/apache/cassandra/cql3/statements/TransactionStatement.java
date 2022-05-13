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

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import accord.api.Key;
import accord.primitives.Keys;
import accord.txn.Txn;
import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.selection.ResultSetBuilder;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.transactions.ConditionStatement;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.accord.txn.*;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.statements.RequestValidations.*;

public class TransactionStatement implements CQLStatement
{
    public static final String DUPLICATE_TUPLE_NAME_MESSAGE = "The name '%s' has already been used by a LET assignment.";
    public static final String INCOMPLETE_PRIMARY_KEY_MESSAGE = "SELECTs in LET assignments without a LIMIT must specify all primary key elements.";
    public static final String INVALID_LIMIT_MESSAGE = "LIMIT must be 1 for SELECTs in LET assignments.";
    public static final String NO_CONDITIONS_IN_UPDATES_MESSAGE = "Updates within transactions may not specify their own conditions.";
    public static final String NO_TIMESTAMPS_IN_UPDATES_MESSAGE = "Updates within transactions may not specify custom timestamps.";
    public static final String EMPTY_TRANSACTION_MESSAGE = "Transaction contains no reads or writes";

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

    private final List<NamedSelect> assignments;
    private final NamedSelect returningSelect;
    private final List<ColumnReference> returningReferences;
    private final List<ModificationStatement> updates;
    private final List<ConditionStatement> conditions;

    public TransactionStatement(List<NamedSelect> assignments,
                                NamedSelect returningSelect,
                                List<ColumnReference> returningReferences,
                                List<ModificationStatement> updates,
                                List<ConditionStatement> conditions)
    {
        this.assignments = assignments;
        this.returningSelect = returningSelect;
        this.returningReferences = returningReferences;
        this.updates = updates;
        this.conditions = conditions;
    }

    @Override
    public void authorize(ClientState state)
    {
        // TODO: Likely just authorize all the constituent reads and modifications
    }

    @Override
    public void validate(ClientState state)
    {
        for (ModificationStatement statement : updates)
            statement.validate(state);
    }

    @VisibleForTesting
    public List<ColumnReference> getReturningReferences()
    {
        return returningReferences;
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
        List<TxnNamedRead> reads = new ArrayList<>(assignments.size() + 1);

        for (NamedSelect select : assignments)
        {
            TxnNamedRead read = createNamedRead(select, options);
            keyConsumer.accept(read.key());
            reads.add(read);
        }

        if (returningSelect != null)
        {
            TxnNamedRead read = createNamedRead(returningSelect, options);
            keyConsumer.accept(read.key());
            reads.add(read);
        }
        
        return new TxnRead(reads);
    }

    TxnCondition createCondition(QueryOptions options)
    {
        if (conditions.isEmpty())
            return TxnCondition.none();
        if (conditions.size() == 1)
            return conditions.get(0).createCondition(options);

        List<TxnCondition> result = new ArrayList<>(conditions.size());
        for (ConditionStatement condition : conditions)
            result.add(condition.createCondition(options));

        return new TxnCondition.BooleanGroup(TxnCondition.Kind.AND, result);
    }

    List<TxnWrite.Fragment> createWriteFragments(ClientState state, QueryOptions options, Consumer<Key> keyConsumer)
    {
        List<TxnWrite.Fragment> fragments = new ArrayList<>(updates.size());
        int idx = 0;
        for (ModificationStatement modification : updates)
        {
            TxnWrite.Fragment fragment = modification.getTxnWriteFragment(idx++, state, options);
            keyConsumer.accept(fragment.key);
            fragments.add(fragment);
        }
        return fragments;
    }

    TxnUpdate createUpdate(ClientState state, QueryOptions options, Consumer<Key> keyConsumer)
    {
        return new TxnUpdate(createWriteFragments(state, options, keyConsumer), createCondition(options));
    }

    Keys toKeys(SortedSet<Key> keySet)
    {
        return new Keys(keySet);
    }

    @VisibleForTesting
    public Txn createTxn(ClientState state, QueryOptions options)
    {
        SortedSet<Key> keySet = new TreeSet<>();
        TxnRead read = createRead(options, keySet::add);
        if (updates.isEmpty())
        {
            Preconditions.checkState(conditions.isEmpty());
            return new Txn.InMemory(toKeys(keySet), read, TxnQuery.ALL);
        }
        else
        {
            TxnUpdate update = createUpdate(state, options, keySet::add);
            return new Txn.InMemory(toKeys(keySet), read, TxnQuery.ALL, update);
        }
    }

    @Override
    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime)
    {
        TxnData data = StorageProxy.instance.txn(createTxn(state.getClientState(), options));
        
        if (returningSelect != null)
        {
            FilteredPartition partition = data.get("returning");
            Selection.Selectors selectors = returningSelect.select.getSelection().newSelectors(options);
            ResultSetBuilder result = new ResultSetBuilder(returningSelect.select.getResultMetadata(), selectors, null);
            returningSelect.select.processPartition(partition.rowIterator(), options, result, FBUtilities.nowInSeconds());
            return new ResultMessage.Rows(result.build());
        }
        
        if (returningReferences != null)
        {
            // TODO: If the ResultMetadata ctor didn't require ColumnSpecification, we wouldn't need both of these.
            List<ColumnSpecification> names = new ArrayList<>(returningReferences.size());
            List<ColumnMetadata> columns = new ArrayList<>(returningReferences.size());
            
            for (ColumnReference reference : returningReferences)
            {
                names.add(reference.column);
                columns.add(reference.column);
            }

            // TODO: Is the column metadata correct here?
            ResultSetBuilder result = new ResultSetBuilder(new ResultSet.ResultMetadata(names), Selection.noopSelector(), null);
            result.newRow(options.getProtocolVersion(), null, null, columns);
            
            for (ColumnReference reference : returningReferences)
            {
                Cell<?> cell = reference.toValueReference(options).getCell(data);
                result.add(cell, FBUtilities.nowInSeconds());
            }

            return new ResultMessage.Rows(result.build());
        }

        // This is a write-only transaction
        return new ResultMessage.Void();
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
            return statement.getRestrictions().hasAllPKColumnsRestrictedByEqualities()
                   || statement.getLimit(QueryOptions.DEFAULT) == 1;
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

    public static class Parsed extends QualifiedStatement
    {
        private final List<SelectStatement.RawStatement> assignments;
        private final SelectStatement.RawStatement select;
        private final List<ColumnReference.Raw> returning;
        private final List<ModificationStatement.Parsed> updates;
        private final List<ConditionStatement.Raw> conditions;
        private final List<ColumnReference.Raw> columnReferences;

        public Parsed(List<SelectStatement.RawStatement> assignments,
                      SelectStatement.RawStatement select,
                      List<ColumnReference.Raw> returning,
                      List<ModificationStatement.Parsed> updates,
                      List<ConditionStatement.Raw> conditions,
                      List<ColumnReference.Raw> columnReferences)
        {
            super(null);
            this.assignments = assignments;
            this.select = select;
            this.returning = returning;
            this.updates = updates;
            this.conditions = conditions != null ? conditions : Collections.emptyList();
            this.columnReferences = columnReferences;
        }

        @Override
        public void setKeyspace(ClientState state)
        {
            assignments.forEach(select -> select.setKeyspace(state));
            updates.forEach(update -> update.setKeyspace(state));
        }

        @Override
        public CQLStatement prepare(ClientState state)
        {
            checkTrue(bindVariables.isEmpty(), "TODO: support bound variables");
            checkFalse(updates.isEmpty() && returning == null && select == null, EMPTY_TRANSACTION_MESSAGE);

            if (select != null || returning != null)
                checkTrue(select != null ^ returning != null, "Cannot specify both a full SELECT and a SELECT w/ LET references.");

            List<NamedSelect> preparedAssignments = new ArrayList<>(assignments.size());
            Map<String, ReferenceSource> refSources = new HashMap<>();
            Set<String> selectNames = new HashSet<>();

            for (SelectStatement.RawStatement select : assignments)
            {
                String name = select.parameters.refName;
                checkNotNull(name, "Assignments must be named");
                checkTrue(selectNames.add(name), DUPLICATE_TUPLE_NAME_MESSAGE, name);
                checkFalse(name.equals("returning"), "Assignments may not use the name \"returning\"");

                SelectStatement preparedSelect = select.prepare(bindVariables);

                int limit = preparedSelect.getLimit(QueryOptions.DEFAULT);

                if (limit == DataLimits.NO_LIMIT)
                    checkTrue(preparedSelect.getRestrictions().hasAllPKColumnsRestrictedByEqualities(), INCOMPLETE_PRIMARY_KEY_MESSAGE);
                else
                    checkTrue(limit == 1, INVALID_LIMIT_MESSAGE);

                NamedSelect namedSelect = new NamedSelect(name, preparedSelect);
                preparedAssignments.add(namedSelect);
                refSources.put(name, new SelectReferenceSource(preparedSelect));
            }

            if (columnReferences != null)
            {
                for (ColumnReference.Raw reference : columnReferences)
                    reference.resolveReference(refSources);
            }

            NamedSelect returningSelect = null;

            if (select != null)
                returningSelect = new NamedSelect("returning", select.prepare(bindVariables));

            List<ColumnReference> returningReferences = null;
            if (returning != null)
                returningReferences = returning.stream().map(ColumnReference.Raw::prepareAsReceiver).collect(Collectors.toList());

            List<ModificationStatement> preparedUpdates = new ArrayList<>(updates.size());
            
            // check for any read-before-write updates
            for (int i = 0; i < updates.size(); i++)
            {
                ModificationStatement.Parsed parsed = updates.get(i);

                if (parsed.hasSelfReference())
                {
                    Preconditions.checkState(parsed.txnReadName == null, "Explicit naming of updates is not allowed.");
                    parsed.txnReadName = "update" + (i + 1);
                    parsed.setSelfSourceName(parsed.txnReadName);
                }

                ModificationStatement prepared = parsed.prepare(bindVariables);
                checkFalse(prepared.hasConditions(), NO_CONDITIONS_IN_UPDATES_MESSAGE);
                checkFalse(prepared.isTimestampSet(), NO_TIMESTAMPS_IN_UPDATES_MESSAGE);

                preparedUpdates.add(prepared);

                if (parsed.txnReadName != null)
                {
                    // TODO: can we borrow placeholder terms for the selection pk?? Test
                    checkTrue(selectNames.add(parsed.txnReadName), DUPLICATE_TUPLE_NAME_MESSAGE, parsed.txnReadName);
                    preparedAssignments.add(new NamedSelect(parsed.txnReadName, ((UpdateStatement) prepared).createSelectForTxn()));
                }
            }

            List<ConditionStatement> preparedConditions = new ArrayList<>(conditions.size());
            for (ConditionStatement.Raw condition : conditions)
                preparedConditions.add(condition.prepare("[txn]", bindVariables));

            return new TransactionStatement(preparedAssignments, returningSelect, returningReferences, preparedUpdates, preparedConditions);
        }
    }
}
