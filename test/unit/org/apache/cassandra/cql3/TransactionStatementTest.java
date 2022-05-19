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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.txn.Txn;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.statements.TransactionStatement;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.accord.txn.TxnBuilder;

import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class TransactionStatementTest
{
    private static final TableId TABLE1 = TableId.fromString("00000000-0000-0000-0000-000000000001");
    private static final TableId TABLE2 = TableId.fromString("00000000-0000-0000-0000-000000000002");

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl1 (k int, c int, v int, primary key (k, c))", "ks").id(TABLE1),
                                    parse("CREATE TABLE tbl2 (k int, c int, v int, primary key (k, c))", "ks").id(TABLE2));
    }

    @Test
    public void testNonTxnSelect()
    {
        // TODO: check txn scope throws exception in normal select
    }

    @Test
    public void testNonTxnUpdate()
    {
        // TODO: check scope reference throws exception in normal update
    }

    @Test
    public void simpleQueryTest()
    {
        String query = "BEGIN TRANSACTION;\n" +
                       "SELECT * FROM ks.tbl1 WHERE k=1 AND c=2 AS row1;\n" +
                       "SELECT * FROM ks.tbl2 WHERE k=2 AND c=2 AS row2;\n" +
                       "UPDATE ks.tbl1 SET v=1 WHERE k=1 AND c=2;\n" +
                       "COMMIT TRANSACTION IF\n" +
                       "  row1 EXISTS\n" +
                       "  AND row1.v = 3\n" +
                       "  AND row2.v=4;";

        Txn expected = TxnBuilder.builder()
                                 .withRead("row1", "SELECT * FROM ks.tbl1 WHERE k=1 AND c=2")
                                 .withRead("row2", "SELECT * FROM ks.tbl2 WHERE k=2 AND c=2")
                                 .withWrite("UPDATE ks.tbl1 SET v=1 WHERE k=1 AND c=2")
                                 .withExistsCondition("row1", 0, null)
                                 .withEqualsCondition("row1", 0, "ks.tbl1.v", bytes(3))
                                 .withEqualsCondition("row2", 0, "ks.tbl2.v", bytes(4))
                                 .build();

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        Assert.assertNotNull(parsed);
        TransactionStatement statement = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());
        Txn actual = statement.createTxn(QueryOptions.DEFAULT);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void parseQueryTest()
    {
        String query = "BEGIN TRANSACTION;\n" +
                       "SELECT * FROM ks.tbl1 WHERE k=1 AND c=2 AS row1;\n" +
                       "SELECT * FROM ks.tbl2 WHERE k=2 AND c=2 AS row2;\n" +
                       "UPDATE ks.tbl1 SET v=row1.v + 1 WHERE k=1 AND c=2;\n" +
                       "COMMIT TRANSACTION IF\n" +
                       "  row1.v = 3\n" +
                       "  AND row2.v=4;";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        TransactionStatement statement = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());
        // TODO: test stuff
    }

    @Test
    public void equalConditionTest()
    {
    }

    @Test
    public void existsConditionTest()
    {
        String query = "BEGIN TRANSACTION;\n" +
                       "SELECT * FROM ks.tbl1 WHERE k=1 AND c=2 AS row1;\n" +
                       "SELECT * FROM ks.tbl2 WHERE k=2 AND c=2 AS row2;\n" +
                       "UPDATE ks.tbl2 SET v=row1.v + 1 WHERE k=2 AND c=2;\n" +
                       "COMMIT TRANSACTION IF row1 EXISTS AND row2 NOT EXISTS;";
        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        TransactionStatement statement = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());
    }

    @Test
    public void notExistsConditionTest()
    {

    }

    @Test
    public void conflictingSelectName()
    {
        // TODO: test conflicting AS names, including autogenerated names
    }

    @Test
    public void partitionKeyColumnReferences()
    {
        // TODO: update partition key names derived from select results must fail validation
        // TODO: confirm functions (ie row.v + 1) also fail
    }
}
