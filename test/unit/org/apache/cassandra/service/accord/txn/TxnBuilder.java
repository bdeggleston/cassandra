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

package org.apache.cassandra.service.accord.txn;

import java.nio.ByteBuffer;
import java.util.*;

import accord.primitives.Keys;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import accord.api.Key;
import accord.txn.Txn;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.accord.api.AccordKey;

public class TxnBuilder
{
    private final List<TxnNamedRead> reads = new ArrayList<>();
    private final List<TxnWrite.Fragment> writes = new ArrayList<>();
    private final List<TxnCondition> conditions = new ArrayList<>();

    public static TxnBuilder builder()
    {
        return new TxnBuilder();
    }

    public TxnBuilder withRead(String name, String query)
    {
        SelectStatement.RawStatement parsed = (SelectStatement.RawStatement) QueryProcessor.parseStatement(query);
        // the parser will only let us define a ref name if we're parsing a transaction, which we're not
        // so we need to manually add it in the call, and confirm nothing got parsed
        Preconditions.checkState(parsed.parameters.refName == null);

        VariableSpecifications bindVariables = VariableSpecifications.empty();
        SelectStatement statement = parsed.prepare(bindVariables);

        ReadQuery readQuery = statement.getQuery(QueryOptions.DEFAULT, 0);
        SinglePartitionReadQuery.Group<SinglePartitionReadCommand> selectQuery = (SinglePartitionReadQuery.Group<SinglePartitionReadCommand>) readQuery;
        reads.add(new TxnNamedRead(name, Iterables.getOnlyElement(selectQuery.queries)));
        return this;
    }

    public TxnBuilder withWrite(PartitionUpdate update, TxnReferenceOperations referenceOps)
    {
        int index = writes.size();
        writes.add(new TxnWrite.Fragment(AccordKey.of(update), index, update, referenceOps));
        return this;
    }

    public TxnBuilder withWrite(String query, TxnReferenceOperations referenceOps)
    {
        ModificationStatement.Parsed parsed = (ModificationStatement.Parsed) QueryProcessor.parseStatement(query);
        VariableSpecifications bindVariables = VariableSpecifications.empty();
        ModificationStatement prepared = parsed.prepare(bindVariables);

        // TODO: If forInternalCalls() correct here?
        return withWrite(prepared.getTxnUpdate(ClientState.forInternalCalls(), QueryOptions.DEFAULT), referenceOps);
    }

    public TxnBuilder withWrite(String query)
    {
        return withWrite(query, TxnReferenceOperations.empty());
    }

    static ValueReference reference(String name, int index, String column)
    {
        ColumnMetadata metadata = null;
        if (column != null)
        {
            String[] parts = column.split("\\.");
            Preconditions.checkArgument(parts.length == 3);
            TableMetadata table = Schema.instance.getTableMetadata(parts[0], parts[1]);
            Preconditions.checkArgument(table != null);
            metadata = table.getColumn(new ColumnIdentifier(parts[2], true));
            Preconditions.checkArgument(metadata != null);
        }
        return new ValueReference(name, index, metadata);
    }

    private TxnBuilder withCondition(TxnCondition condition)
    {
        conditions.add(condition);
        return this;
    }

    public TxnBuilder withValueCondition(String name, int index, String column, TxnCondition.Kind kind, ByteBuffer value)
    {
        return withCondition(new TxnCondition.Value(reference(name, index, column), kind, value));
    }

    public TxnBuilder withEqualsCondition(String name, int index, String column, ByteBuffer value)
    {
        return withValueCondition(name, index, column, TxnCondition.Kind.EQUAL, value);
    }

    private TxnBuilder withExistsCondition(String name, int index, String column, TxnCondition.Kind kind)
    {
        return withCondition(new TxnCondition.Exists(reference(name, index, column), kind));
    }

    public TxnBuilder withExistsCondition(String name, int index, String column)
    {
        return withExistsCondition(name, index, column, TxnCondition.Kind.IS_NOT_NULL);
    }

    public TxnBuilder withNotExistsCondition(String name, int index, String column)
    {
        return withExistsCondition(name, index, column, TxnCondition.Kind.IS_NULL);
    }

    Keys toKeys(SortedSet<Key> keySet)
    {
        return new Keys(keySet);
    }

    public Txn build()
    {
        SortedSet<Key> keySet = new TreeSet<>();

        List<TxnNamedRead> namedReads = new ArrayList<>(reads.size());
        for (TxnNamedRead read : reads)
        {
            keySet.add(read.key());
            namedReads.add(read);
        }
        TxnRead read = new TxnRead(namedReads);

        if (writes.isEmpty())
        {
            Preconditions.checkState(conditions.isEmpty());
            return new Txn.InMemory(toKeys(keySet), read, TxnQuery.ALL);
        }
        else
        {
            TxnCondition condition;
            if (conditions.isEmpty())
                condition = TxnCondition.none();
            else if (conditions.size() == 1)
                condition = conditions.get(0);
            else
                condition = new TxnCondition.BooleanGroup(TxnCondition.Kind.AND, conditions);

            writes.forEach(write -> keySet.add(write.key));
            TxnUpdate update = new TxnUpdate(writes, condition);
            return new Txn.InMemory(toKeys(keySet), read, TxnQuery.ALL, update);
        }
    }
}
