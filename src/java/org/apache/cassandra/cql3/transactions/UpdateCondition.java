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

import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.ColumnReference;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.service.accord.txn.TxnCondition;

public class UpdateCondition
{
    public enum Kind { EXISTS, NOT_EXISTS, EQ, NEQ, GT, GTE, LT, LTE }

    private final ColumnReference reference;
    private final Kind kind;
    private final Term value;

    public UpdateCondition(ColumnReference reference, Kind kind, Term value)
    {
        this.reference = reference;
        this.kind = kind;
        this.value = value;
    }

    public static class Raw
    {
        private final ColumnReference.Raw reference;
        private final Kind kind;
        private final Term.Raw value;

        public Raw(ColumnReference.Raw reference, Kind kind, Term.Raw value)
        {
            Preconditions.checkArgument(reference != null);
            Preconditions.checkArgument((value == null) == (kind == Kind.EXISTS || kind == Kind.NOT_EXISTS));
            this.reference = reference;
            this.kind = kind;
            this.value = value;
        }

        public UpdateCondition prepare(String keyspace, VariableSpecifications bindVariables)
        {
            ColumnReference preparedReference = reference.prepareAsReceiver();
            preparedReference.collectMarkerSpecification(bindVariables);
            Term preparedValue = null;
            if (value != null)
            {
                preparedValue = value.prepare(keyspace, preparedReference.column);
                preparedValue.collectMarkerSpecification(bindVariables);
            }

            return new UpdateCondition(preparedReference, kind, preparedValue);
        }
    }

    private static TxnCondition.Kind convertKind(Kind kind)
    {
        switch (kind)
        {
            case EQ:
                return TxnCondition.Kind.EQUAL;
            case NEQ:
                return TxnCondition.Kind.NOT_EQUAL;
            case GT:
                return TxnCondition.Kind.GREATER_THAN;
            case GTE:
                return TxnCondition.Kind.GREATER_THAN_OR_EQUAL;
            case LT:
                return TxnCondition.Kind.LESS_THAN;
            case LTE:
                return TxnCondition.Kind.LESS_THAN_OR_EQUAL;
            case EXISTS:
                return TxnCondition.Kind.EXISTS;
            case NOT_EXISTS:
                return TxnCondition.Kind.NOT_EXISTS;
            default:
                throw new IllegalArgumentException();
        }
    }

    public TxnCondition createCondition(QueryOptions options)
    {
        switch (kind)
        {
            case EXISTS:
            case NOT_EXISTS:
                return new TxnCondition.Exists(reference.toValueReference(options), convertKind(kind));
            case EQ:
            case NEQ:
            case GT:
            case GTE:
            case LT:
            case LTE:
                return new TxnCondition.Value(reference.toValueReference(options), convertKind(kind), value.bindAndGet(options));
            default:
                throw new IllegalStateException();
        }
    }
}
