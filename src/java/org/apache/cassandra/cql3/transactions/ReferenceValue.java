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

import org.apache.cassandra.cql3.ColumnReference;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.accord.txn.TxnReferenceValue;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

public abstract class ReferenceValue
{
    public abstract TxnReferenceValue bindAndGet(QueryOptions options);

    public static abstract class Raw
    {
        public abstract ReferenceValue prepare(ColumnMetadata receiver, VariableSpecifications bindVariables);
    }

    public static class Substitution extends ReferenceValue
    {
        private final ColumnReference reference;

        public Substitution(ColumnReference reference)
        {
            this.reference = reference;
        }

        @Override
        public TxnReferenceValue bindAndGet(QueryOptions options)
        {
            return new TxnReferenceValue.Substitution(reference.toValueReference(options));
        }

        public static class Raw extends ReferenceValue.Raw
        {
            private final ColumnReference.Raw reference;

            public Raw(ColumnReference.Raw reference)
            {
                this.reference = reference;
            }

            @Override
            public ReferenceValue prepare(ColumnMetadata receiver, VariableSpecifications bindVariables)
            {
                reference.checkResolved();
                checkTrue(reference.column() != null, "substitution references must reference a column (%s)", reference);
                return new Substitution((ColumnReference) reference.prepare("", receiver));
            }
        }
    }
}
