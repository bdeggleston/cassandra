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

package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

public class ColumnReference implements Term
{
    private final List<Term> terms;
    private final SelectStatement select;
    private final ColumnMetadata column;

    public ColumnReference(List<Term> terms, SelectStatement select, ColumnMetadata column)
    {
        this.terms = terms;
        this.select = select;
        this.column = column;
    }

    @Override
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        // TODO: this
    }

    @Override
    public Terminal bind(QueryOptions options) throws InvalidRequestException
    {
        // TODO: this
        return null;
    }

    @Override
    public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException
    {
        // TODO: this
        return null;
    }

    @Override
    public boolean containsBindMarker()
    {
        // TODO: this
        return false;
    }

    @Override
    public void addFunctionsTo(List<Function> functions)
    {
        // TODO: this
    }

    public static class Raw extends Term.Raw
    {
        private static final ColumnSpecification TEXT_TERM = new ColumnSpecification(null, null, null, UTF8Type.instance);
        private static final ColumnSpecification INT_TERM = new ColumnSpecification(null, null, null, Int32Type.instance);

        private final List<Term.Raw> terms;
        private List<Term> preparedTerms;

        private boolean isResolved = false;
        private SelectStatement select;
        private ColumnMetadata column;

        private ColumnReference prepared;


        public Raw(List<Term.Raw> terms)
        {
            this.terms = terms;
        }

        public void resolveReference(Map<String, SelectStatement> selects)
        {
            if (isResolved)
                return;
            checkTrue(terms.size() > 1, "Incomplete column reference: %s", this);

            Constants.Literal literal;
            preparedTerms = new ArrayList<>(terms.size());

            // root level name
            literal = (Constants.Literal) terms.get(0);
            select = selects.get(literal.getRawText());
            checkNotNull(select, "%s doesn't reference a select", this);
            preparedTerms.add(literal.prepare(null, TEXT_TERM));

            // TODO: also support multi-row result indexing here
            // TODO: check if selection selects all primary key columns (and therefore returns a single row)... or always require use array indexing to keep it real
            literal = (Constants.Literal) terms.get(1);
            column = select.table.getColumn(new ColumnIdentifier(literal.getRawText(), true));
            checkNotNull(column, "%s doesn't reference a valid column", this);
            preparedTerms.add(literal.prepare(null, TEXT_TERM));

            checkTrue(select.getSelection().getColumns().contains(column), "%s refererences a column not included in the select", this);

            // TODO: confirm update partition key terms don't contain column references. This can't be done in prepare
            //   because there can be intermediate functions (ie: pk=row.v+1). Need a recursive Term visitor

            if (terms.size() > 2)
            {
                checkTrue(false, "TODO: support multiple rows, udts, etc");
            }
            isResolved = true;
        }

        private void checkResolved()
        {
            if (!isResolved)
                throw new IllegalStateException();
        }

        @Override
        public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            checkResolved();
            return column.testAssignment(keyspace, receiver);
        }

        @Override
        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            checkResolved();

            if (!column.testAssignment(keyspace, receiver).isAssignable())
                throw new InvalidRequestException(String.format("Invalid reference type %s (%s) for \"%s\" of type %s", column.type, column.name, receiver.name, receiver.type.asCQL3Type()));
            prepared = new ColumnReference(preparedTerms, select, column);
            return prepared;
        }

        public ColumnReference prepared()
        {
            Preconditions.checkState(prepared != null);
            return prepared;
        }

        @Override
        public String getText()
        {
            checkResolved();
            return terms.stream().map(Term.Raw::getText).reduce("", (l, r) -> l + '.' + r);
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            checkResolved();
            return column.type;
        }
    }
}
