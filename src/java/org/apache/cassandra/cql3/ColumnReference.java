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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.TransactionStatement;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.accord.txn.ValueReference;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;

public class ColumnReference implements Term
{
    public static final String CANNOT_FIND_TUPLE_MESSAGE = "Cannot resolve reference to tuple '%s'.";
    public static final String COLUMN_NOT_IN_TUPLE_MESSAGE = "Column '%s' does not exist in tuple '%s'.";

    private final String selectName;
    private final Term rowIndex;
    public final ColumnMetadata column;

    private final Term cellPath;
    public ColumnReference(String selectName, Term rowIndex, ColumnMetadata column, Term cellPath)
    {
        this.selectName = selectName;
        this.rowIndex = rowIndex;
        this.column = column;
        this.cellPath = cellPath;
    }

    @Override
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        if (rowIndex != null)
            rowIndex.collectMarkerSpecification(boundNames);
        if (cellPath != null)
            cellPath.collectMarkerSpecification(boundNames);
    }

    @Override
    public Terminal bind(QueryOptions options) throws InvalidRequestException
    {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException
    {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public boolean containsBindMarker()
    {
        return rowIndex.containsBindMarker() || (cellPath != null && cellPath.containsBindMarker());
    }

    @Override
    public void addFunctionsTo(List<Function> functions)
    {
        // TODO: this
    }

    private int bindRowIndex(QueryOptions options)
    {
        return ByteBufferUtil.toInt(rowIndex.bindAndGet(options));
    }

    private CellPath bindCellPath(QueryOptions options)
    {
        return cellPath != null ? CellPath.create(cellPath.bindAndGet(options)) : null;
    }

    public ValueReference toValueReference(QueryOptions options)
    {
        Preconditions.checkArgument(cellPath == null);
        return new ValueReference(selectName, bindRowIndex(options), column, bindCellPath(options));
    }

    public static class Raw extends Term.Raw
    {
        private static final ColumnSpecification TEXT_TERM = new ColumnSpecification(null, null, null, UTF8Type.instance);
        private static final ColumnSpecification INT_TERM = new ColumnSpecification(null, null, null, Int32Type.instance);
        private static final Constants.Value ROW_IDX_ZERO = new Constants.Value(ByteBufferUtil.bytes(0));

        private final List<Term.Raw> terms;
        private boolean isResolved = false;

        private String tupleName;
        private ColumnMetadata column;
        private Term rowIndex = null;
        private Term cellPath = null;

        private ColumnReference prepared;


        public Raw(List<Term.Raw> terms)
        {
            Preconditions.checkArgument(terms != null && !terms.isEmpty());
            this.terms = terms;
        }

        private void resolveFinished()
        {
            isResolved = true;
            if (rowIndex == null)
                rowIndex = ROW_IDX_ZERO;
        }

        public void resolveReference(Map<String, TransactionStatement.ReferenceSource> sources)
        {
            if (isResolved)
                return;

            Iterator<Term.Raw> termIterator = terms.iterator();

            // root level name
            Constants.Literal literal = (Constants.Literal) termIterator.next();
            tupleName = literal.getRawText();
            TransactionStatement.ReferenceSource source = sources.get(tupleName);
            checkNotNull(source, CANNOT_FIND_TUPLE_MESSAGE, tupleName);

            if (!termIterator.hasNext())
            {
                resolveFinished();
                return;
            }

            if (source.isPointSelect())
                rowIndex = new Constants.Value(ByteBufferUtil.bytes(0));
            else
                throw new UnsupportedOperationException("Multi-row reference sources are not allowed!");

            literal = (Constants.Literal) termIterator.next();
            column = source.getColumn(literal.getRawText());
            checkNotNull(column, COLUMN_NOT_IN_TUPLE_MESSAGE, literal.getRawText(), tupleName);

            // TODO: confirm update partition key terms don't contain column references. This can't be done in prepare
            //   because there can be intermediate functions (ie: pk=row.v+1 or pk=_add(row.v, 5)). Need a recursive Term visitor

            if (!termIterator.hasNext())
            {
                resolveFinished();
                return;
            }

            throw new UnsupportedOperationException("TODO: support collections, udts, etc");
        }

        public void manualResolve(String selectName, ColumnMetadata column, int rowIndex, Term cellPath)
        {
            Preconditions.checkState(!isResolved);
            this.tupleName = selectName;
            this.column = column;
            this.rowIndex = new Constants.Value(ByteBufferUtil.bytes(rowIndex));
            Preconditions.checkArgument(cellPath == null, "TODO: support collections etc");
            isResolved = true;

        }

        public void checkResolved()
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


        public static ColumnReference prepare(String keyspace, ColumnSpecification receiver,
                                              String selectName, Term rowIndex, ColumnMetadata column, Term cellPath)
        {
            if (!column.testAssignment(keyspace, receiver).isAssignable())
                throw new InvalidRequestException(String.format("Invalid reference type %s (%s) for \"%s\" of type %s", column.type, column.name, receiver.name, receiver.type.asCQL3Type()));
            return new ColumnReference(selectName, rowIndex, column, cellPath);
        }

        public static ColumnReference prepare(String keyspace, ColumnSpecification receiver,
                                              String selectName, int rowIndex, ColumnMetadata column, Term cellPath)
        {
            return prepare(keyspace, receiver, selectName, new Constants.Value(ByteBufferUtil.bytes(rowIndex)), column, cellPath);
        }

        @Override
        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            checkResolved();
            prepared = prepare(keyspace, receiver, tupleName, rowIndex, column, cellPath);
            return prepared;
        }

        public ColumnReference prepareAsReceiver()
        {
            checkResolved();
            prepared = new ColumnReference(tupleName, rowIndex, column, cellPath);
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
//            checkResolved();
            return terms.stream().map(Term.Raw::getText).reduce("", (l, r) -> l + '.' + r);
        }

        @Override
        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            checkResolved();
            return column.type;
        }

        public ColumnMetadata column()
        {
            return column;
        }
    }
}
