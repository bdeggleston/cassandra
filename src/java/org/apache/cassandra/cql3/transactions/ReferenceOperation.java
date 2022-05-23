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

package org.apache.cassandra.cql3.transactions;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnReference;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.txn.TxnReferenceOperation;
import org.apache.cassandra.service.accord.txn.TxnReferenceValue;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

public abstract class ReferenceOperation
{
    public enum Kind { SET, ADD, SUB, MULT, DIV }

    private final ColumnMetadata receiver;

    public ReferenceOperation(ColumnMetadata receiver)
    {
        this.receiver = receiver;
    }

    public ColumnMetadata receiver()
    {
        return receiver;
    }

    public abstract TxnReferenceOperation bindAndGet(QueryOptions options);

    public abstract static class Raw
    {
        public final ColumnIdentifier column;

        public Raw(ColumnIdentifier column)
        {
            this.column = column;
        }

        public abstract ReferenceOperation prepare(TableMetadata metadata, VariableSpecifications bindVariables);
    }

    public static class Substitution extends ReferenceOperation
    {
        private final ColumnReference reference;

        public Substitution(ColumnMetadata receiver, ColumnReference reference)
        {
            super(receiver);
            this.reference = reference;
        }

        @Override
        public TxnReferenceOperation bindAndGet(QueryOptions options)
        {
            TxnReferenceValue value = new TxnReferenceValue.Substitution(reference.toValueReference(options));
            return new TxnReferenceOperation(receiver(), value);
        }

        public static class Raw extends ReferenceOperation.Raw
        {
            private final ColumnReference.Raw reference;

            public Raw(ColumnIdentifier column, ColumnReference.Raw reference)
            {
                super(column);
                this.reference = reference;
            }

            @Override
            public ReferenceOperation prepare(TableMetadata metadata, VariableSpecifications bindVariables)
            {
                reference.checkResolved();
                ColumnMetadata receiver = metadata.getColumn(column);
                checkTrue(!receiver.isPrimaryKeyColumn(), "Cannot use value references for primary key columns: %s", column);
                checkTrue(receiver != null, "Unknown column %s for %s.%s", column, metadata.keyspace, metadata.name);
                checkTrue(reference.column() != null, "substitution references must reference a column (%s)", reference);
                return new Substitution(receiver, (ColumnReference) reference.prepare("", receiver));
            }
        }
    }
}
