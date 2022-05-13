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
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.txn.TxnReferenceOperation;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.schema.TableMetadata.UNDEFINED_COLUMN_NAME_MESSAGE;

public abstract class ReferenceOperation
{
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

        public abstract boolean hasSelfReference();
        public abstract void setSelfSourceName(String name);

        public abstract void setTableMetadata(TableMetadata metadata);
        public abstract ReferenceOperation prepare(TableMetadata metadata, VariableSpecifications bindVariables);
    }

    public static class Assignment extends ReferenceOperation
    {
        private final ReferenceValue value;

        public Assignment(ColumnMetadata receiver, ReferenceValue value)
        {
            super(receiver);
            this.value = value;
        }

        @Override
        public TxnReferenceOperation bindAndGet(QueryOptions options)
        {
            return new TxnReferenceOperation(receiver(), value.bindAndGet(options));
        }

        public static class Raw extends ReferenceOperation.Raw
        {
            private final ReferenceValue.Raw value;

            public Raw(ColumnIdentifier column, ReferenceValue.Raw value)
            {
                super(column);
                this.value = value;
            }

            @Override
            public ReferenceOperation prepare(TableMetadata metadata, VariableSpecifications bindVariables)
            {
                ColumnMetadata receiver = metadata.getColumn(column);
                checkTrue(receiver != null, UNDEFINED_COLUMN_NAME_MESSAGE, column.toCQLString(), metadata);
                return new Assignment(receiver, value.prepare(receiver, bindVariables));
            }

            @Override
            public boolean hasSelfReference()
            {
                return value.hasSelfReference();
            }

            @Override
            public void setSelfSourceName(String name)
            {
                value.setSelfSourceName(name);
            }

            @Override
            public void setTableMetadata(TableMetadata metadata)
            {
                value.setTableMetadata(metadata);
            }
        }
    }
}
