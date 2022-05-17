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

import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;

public class ValueReference
{
    private final String name;
    private final int rowIdx;
    private final ColumnMetadata column;
    private final CellPath path;

    public ValueReference(String name, int rowIdx, ColumnMetadata column, CellPath path)
    {
        this.name = name;
        this.rowIdx = rowIdx;
        this.column = column;
        this.path = path;
    }

    public ValueReference(String name, int rowIdx, ColumnMetadata column)
    {
        this(name, rowIdx, column, null);
    }

    public ColumnMetadata column()
    {
        return column;
    }

    public boolean selectsRow()
    {
        return rowIdx >= 0;
    }

    public boolean selectsCell()
    {
        return selectsRow() && column != null;
    }

    public boolean selectsPath()
    {
        return selectsCell() && path != null;
    }

    public FilteredPartition getPartition(TxnData data)
    {
        return data.get(name);
    }

    public Row getRow(TxnData data)
    {
        FilteredPartition partition = getPartition(data);
        return partition != null ? getRow(partition) : null;
    }

    public Row getRow(FilteredPartition partition)
    {
        int maxIdx = partition.rowCount() - 1;
        if (rowIdx > maxIdx)
            return null;
        return partition.getAtIdx(rowIdx);
    }

    public Cell<?> getCell(Row row)
    {
        return path != null ? row.getCell(column, path) : row.getCell(column);
    }

    public Cell<?> getCell(FilteredPartition partition)
    {
        Row row = getRow(partition);
        return row != null ? getCell(row) : null;
    }

    public Cell<?> getCell(TxnData data)
    {
        Row row = getRow(data);
        return row != null ? getCell(row) : null;
    }
}
