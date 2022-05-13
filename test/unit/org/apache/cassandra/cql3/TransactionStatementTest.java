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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.txn.Txn;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.statements.TransactionStatement;
import org.apache.cassandra.db.BufferClustering;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.accord.txn.TxnBuilder;
import org.apache.cassandra.service.accord.txn.TxnReferenceOperation;
import org.apache.cassandra.service.accord.txn.TxnReferenceOperations;
import org.apache.cassandra.service.accord.txn.TxnReferenceValue.Addition;
import org.apache.cassandra.service.accord.txn.TxnReferenceValue.Constant;
import org.apache.cassandra.service.accord.txn.TxnReferenceValue.Substitution;
import org.apache.cassandra.service.accord.txn.TxnReferenceValue.Subtraction;
import org.apache.cassandra.service.accord.txn.ValueReference;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.ColumnReference.CANNOT_FIND_TUPLE_MESSAGE;
import static org.apache.cassandra.cql3.ColumnReference.COLUMN_NOT_IN_TUPLE_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.*;
import static org.apache.cassandra.cql3.statements.UpdateStatement.UPDATING_PRIMARY_KEY_MESSAGE;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.schema.TableMetadata.UNDEFINED_COLUMN_NAME_MESSAGE;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;

public class TransactionStatementTest
{
    private static final TableId TABLE1_ID = TableId.fromString("00000000-0000-0000-0000-000000000001");
    private static final TableId TABLE2_ID = TableId.fromString("00000000-0000-0000-0000-000000000002");

    private static TableMetadata TABLE1;
    private static TableMetadata TABLE2;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl1 (k int, c int, v int, primary key (k, c))", "ks").id(TABLE1_ID),
                                    parse("CREATE TABLE tbl2 (k int, c int, v int, primary key (k, c))", "ks").id(TABLE2_ID));
        TABLE1 = Schema.instance.getTableMetadata("ks", "tbl1");
        TABLE2 = Schema.instance.getTableMetadata("ks", "tbl2");
    }

    @Test
    public void shouldRejectReferenceSelectOutsideTxn()
    {
        Assertions.assertThatThrownBy(() -> QueryProcessor.parseStatement("SELECT row1.v, row2.v;"))
                  .isInstanceOf(SyntaxException.class)
                  .hasMessageContaining("expecting K_FROM");
    }

    @Test
    public void shouldRejectReferenceUpdateOutsideTxn()
    {
        Assertions.assertThatThrownBy(() -> QueryProcessor.parseStatement("UPDATE ks.tbl1 SET v=row2.v WHERE k=1 AND c=2;"))
                  .isInstanceOf(SyntaxException.class)
                  .hasMessageContaining("failed predicate");
    }

    @Test
    public void shouldRejectConditionalWithNoEndIf()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  IF row1 IS NOT NULL AND row1.v = 3 AND row2.v=4 THEN\n" +
                       "    UPDATE ks.tbl1 SET v=1 WHERE k=1 AND c=2;\n" +
                       "COMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> QueryProcessor.parseStatement(query))
                  .isInstanceOf(SyntaxException.class)
                  .hasMessageContaining("failed predicate");
    }

    @Test
    public void shouldRejectConditionalWithEndIfButNoIf()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "    UPDATE ks.tbl1 SET v=1 WHERE k=1 AND c=2;\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> QueryProcessor.parseStatement(query))
                  .isInstanceOf(SyntaxException.class)
                  .hasMessageContaining("failed predicate");
    }

    @Test
    public void shouldRejectLetOnlyStatement()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);

        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(EMPTY_TRANSACTION_MESSAGE);
    }

    @Test
    public void shouldRejectDuplicateTupleName()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl2 WHERE k=2 AND c=2);\n" +
                       "  SELECT row1.v;\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);

        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(DUPLICATE_TUPLE_NAME_MESSAGE, "row1"));
    }

    @Test
    public void shouldRejectIllegalLimit()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 LIMIT 2);\n" +
                       "  SELECT row1.v;\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);

        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(INVALID_LIMIT_MESSAGE);
    }

    @Test
    public void shouldRejectIncompletePrimaryKey()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1);\n" +
                       "  SELECT row1.v;\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);

        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(INCOMPLETE_PRIMARY_KEY_MESSAGE);
    }

    @Test
    public void shouldRejectUpdateWithCondition()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  INSERT INTO ks.tbl1 (k, c, v) VALUES (0, 0, 1) IF NOT EXISTS;\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);

        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(NO_CONDITIONS_IN_UPDATES_MESSAGE);
    }

    @Test
    public void shouldRejectUpdateWithCustomTimestamp()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  INSERT INTO ks.tbl1 (k, c, v) VALUES (0, 0, 1) USING TIMESTAMP 1;\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);

        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(NO_TIMESTAMPS_IN_UPDATES_MESSAGE);
    }

    @Test
    public void shouldRejectBothFullSelectAndSelectWithReferences()
    {
        String query = "BEGIN TRANSACTION\n" +
                "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                "  SELECT v FROM ks.tbl1 WHERE k=2 AND c=2;\n" +
                "  SELECT row1.v;\n" +
                "  IF row1 IS NOT NULL AND row1.v = 3 AND row2.v=4 THEN\n" +
                "    UPDATE ks.tbl1 SET v=1 WHERE k=1 AND c=2;\n" +
                "  END IF\n" +
                "COMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> QueryProcessor.parseStatement(query))
                  .isInstanceOf(SyntaxException.class)
                  .hasMessageContaining("no viable alternative");
    }

    @Test
    public void shouldRejectPrimaryKeyValueReference()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=1);\n" +
                       "  IF row1 IS NULL THEN\n" +
                       "    UPDATE ks.tbl1 SET c=row1.c WHERE k=1 AND c=2;\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(UPDATING_PRIMARY_KEY_MESSAGE, "c"));
    }

    @Test
    public void shouldRejectShorthandAssignmentToUnknownColumn()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  UPDATE ks.tbl1 SET q += 1 WHERE k=1 AND c=2;\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(UNDEFINED_COLUMN_NAME_MESSAGE, "q", "ks.tbl1"));
    }

    @Test
    public void shouldRejectAdditionToUnknownColumn()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  UPDATE ks.tbl1 SET v = q + 1 WHERE k=1 AND c=2;\n" +
                       "COMMIT TRANSACTION";

        Assertions.assertThatThrownBy(() -> QueryProcessor.parseStatement(query))
                  .isInstanceOf(SyntaxException.class)
                  .hasMessageContaining("Only expressions of the form X = X +<value> are supported.");
    }

    @Test
    public void shouldRejectUnknownSubstitutionTuple()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  UPDATE ks.tbl1 SET v = row1.v WHERE k=1 AND c=2;\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(CANNOT_FIND_TUPLE_MESSAGE, "row1"));
    }

    @Test
    public void shouldRejectUnknownSubstitutionColumn()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                       "  UPDATE ks.tbl1 SET v = row1.q WHERE k=1 AND c=2;\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining(String.format(COLUMN_NOT_IN_TUPLE_MESSAGE, "q", "row1"));
    }

    @Test
    public void shouldRejectLetUpdateNameConflict()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET update1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                       "  UPDATE ks.tbl1 SET v += 1 WHERE k=1 AND c=2;\n" +
                       "COMMIT TRANSACTION";

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        Assertions.assertThatThrownBy(() -> parsed.prepare(ClientState.forInternalCalls()))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(DUPLICATE_TUPLE_NAME_MESSAGE, "update1"));
    }

    @Test
    public void simpleQueryTest()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                       "  LET row2 = (SELECT * FROM ks.tbl2 WHERE k=2 AND c=2);\n" +
                       "  SELECT v FROM ks.tbl1 WHERE k=2 AND c=2;\n" +
                       "  IF row1 IS NOT NULL AND row1.v = 3 AND row2.v=4 THEN\n" +
                       "    UPDATE ks.tbl1 SET v=1 WHERE k=1 AND c=2;\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";

        Txn expected = TxnBuilder.builder()
                                 .withRead("row1", "SELECT * FROM ks.tbl1 WHERE k=1 AND c=2")
                                 .withRead("row2", "SELECT * FROM ks.tbl2 WHERE k=2 AND c=2")
                                 .withRead("returning", "SELECT v FROM ks.tbl1 WHERE k=2 AND c=2")
                                 .withWrite("UPDATE ks.tbl1 SET v=1 WHERE k=1 AND c=2")
                                 .withExistsCondition("row1", 0, null)
                                 .withEqualsCondition("row1", 0, "ks.tbl1.v", bytes(3))
                                 .withEqualsCondition("row2", 0, "ks.tbl2.v", bytes(4))
                                 .build();

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        Assert.assertNotNull(parsed);
        TransactionStatement statement = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());
        Txn actual = statement.createTxn(ClientState.forInternalCalls(), QueryOptions.DEFAULT);

        assertEquals(expected, actual);
    }

    @Test
    public void txnWithReturningStatement()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                       "  LET row2 = (SELECT * FROM ks.tbl2 WHERE k=2 AND c=2);\n" +
                       "  SELECT row1.v, row2.v;\n" +
                       "  IF row1 IS NOT NULL AND row1.v = 3 AND row2.v=4 THEN\n" +
                       "    UPDATE ks.tbl1 SET v=1 WHERE k=1 AND c=2;\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";

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
        Txn actual = statement.createTxn(ClientState.forInternalCalls(), QueryOptions.DEFAULT);

        assertEquals(expected, actual);
        assertEquals(2, statement.getReturningReferences().size());
    }

    @Test
    public void updateVariableSubstitutionTest()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                       "  LET row2 = (SELECT * FROM ks.tbl2 WHERE k=2 AND c=2);\n" +
                       "  SELECT v FROM ks.tbl1 WHERE k=1 AND c=2;\n" +
                       "  IF row1.v = 3 AND row2.v=4 THEN\n" +
                       "    UPDATE ks.tbl1 SET v=row2.v WHERE k=1 AND c=2;\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";

        List<TxnReferenceOperation> regularOps = new ArrayList<>();
        regularOps.add(new TxnReferenceOperation(column(TABLE1, "v"),
                                                 new Substitution(reference("row2", TABLE2, "v", 0))));
        TxnReferenceOperations referenceOps = new TxnReferenceOperations(TABLE1, Clustering.make(bytes(2)), regularOps, Collections.emptyList());
        Txn expected = TxnBuilder.builder()
                                 .withRead("row1", "SELECT * FROM ks.tbl1 WHERE k=1 AND c=2")
                                 .withRead("row2", "SELECT * FROM ks.tbl2 WHERE k=2 AND c=2")
                                 .withRead("returning", "SELECT v FROM ks.tbl1 WHERE k=1 AND c=2")
                                 .withWrite(emptyUpdate(TABLE1, 1, 2, false), referenceOps)
                                 .withEqualsCondition("row1", 0, "ks.tbl1.v", bytes(3))
                                 .withEqualsCondition("row2", 0, "ks.tbl2.v", bytes(4))
                                 .build();

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        Assert.assertNotNull(parsed);
        TransactionStatement statement = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());
        Txn actual = statement.createTxn(ClientState.forInternalCalls(), QueryOptions.DEFAULT);
        assertEquals(expected, actual);
    }

    @Test
    public void insertVariableSubstitutionTest()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT * FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                       "  LET row2 = (SELECT * FROM ks.tbl2 WHERE k=2 AND c=2);\n" +
                       "  SELECT v FROM ks.tbl1 WHERE k=1 AND c=2;\n" +
                       "  IF row1.v = 3 AND row2.v=4 THEN\n" +
                       "    INSERT INTO ks.tbl1 (k, c, v) VALUES (1, 2, row2.v);\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";

        List<TxnReferenceOperation> regularOps = new ArrayList<>();
        regularOps.add(new TxnReferenceOperation(column(TABLE1, "v"),
                                                   new Substitution(reference("row2", TABLE2, "v", 0))));
        TxnReferenceOperations referenceOps = new TxnReferenceOperations(TABLE1, Clustering.make(bytes(2)), regularOps, Collections.emptyList());
        Txn expected = TxnBuilder.builder()
                                 .withRead("row1", "SELECT * FROM ks.tbl1 WHERE k=1 AND c=2")
                                 .withRead("row2", "SELECT * FROM ks.tbl2 WHERE k=2 AND c=2")
                                 .withRead("returning", "SELECT v FROM ks.tbl1 WHERE k=1 AND c=2")
                                 .withWrite(emptyUpdate(TABLE1, 1, 2, true), referenceOps)
                                 .withEqualsCondition("row1", 0, "ks.tbl1.v", bytes(3))
                                 .withEqualsCondition("row2", 0, "ks.tbl2.v", bytes(4))
                                 .build();

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        Assert.assertNotNull(parsed);
        TransactionStatement statement = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());
        Txn actual = statement.createTxn(ClientState.forInternalCalls(), QueryOptions.DEFAULT);
        assertEquals(expected, actual);
    }

    // TODO: This may be logically a duplicate now, as we no longer support named updates.
    @Test
    public void readForUpdateTest()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT v FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                       "  LET row2 = (SELECT * FROM ks.tbl2 WHERE k=2 AND c=2);\n" +
                       "  SELECT v FROM ks.tbl1 WHERE k=1 AND c=2;\n" +
                       "  IF row1.v = 3 AND row2.v=4 THEN\n" +
                       "    UPDATE ks.tbl1 SET v=3 WHERE k=1 AND c=2;\n" +
                       "    UPDATE ks.tbl2 SET v=4 WHERE k=2 AND c=2;\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";

        Txn expected = TxnBuilder.builder()
                                 .withRead("row1", "SELECT v FROM ks.tbl1 WHERE k=1 AND c=2")
                                 .withRead("row2", "SELECT * FROM ks.tbl2 WHERE k=2 AND c=2")
                                 .withRead("returning", "SELECT v FROM ks.tbl1 WHERE k=1 AND c=2")
                                 .withWrite("UPDATE ks.tbl1 SET v=3 WHERE k=1 AND c=2")
                                 .withWrite("UPDATE ks.tbl2 SET v=4 WHERE k=2 AND c=2")
                                 .withEqualsCondition("row1", 0, "ks.tbl1.v", bytes(3))
                                 .withEqualsCondition("row2", 0, "ks.tbl2.v", bytes(4))
                                 .build();
        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        Assert.assertNotNull(parsed);
        TransactionStatement statement = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());
        Txn actual = statement.createTxn(ClientState.forInternalCalls(), QueryOptions.DEFAULT);
        assertEquals(expected, actual);
    }

    // TODO: This may be logically a duplicate now, as we no longer support named updates.
    @Test
    public void readForInsertTest()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  LET row1 = (SELECT v FROM ks.tbl1 WHERE k=1 AND c=2);\n" +
                       "  LET row2 = (SELECT * FROM ks.tbl2 WHERE k=2 AND c=2);\n" +
                       "  SELECT v FROM ks.tbl1 WHERE k=1 AND c=2;\n" +
                       "  IF row1.v = 3 AND row2.v=4 THEN\n" +
                       "    INSERT INTO ks.tbl1 (k, c, v) VALUES (1, 2, 3);\n" +
                       "    INSERT INTO ks.tbl2 (k, c, v) VALUES (2, 2, 4);\n" +
                       "  END IF\n" +
                       "COMMIT TRANSACTION";

        Txn expected = TxnBuilder.builder()
                                 .withRead("row1", "SELECT v FROM ks.tbl1 WHERE k=1 AND c=2")
                                 .withRead("row2", "SELECT * FROM ks.tbl2 WHERE k=2 AND c=2")
                                 .withRead("returning", "SELECT v FROM ks.tbl1 WHERE k=1 AND c=2")
                                 .withWrite("INSERT INTO ks.tbl1 (k, c, v) VALUES (1, 2, 3)")
                                 .withWrite("INSERT INTO ks.tbl2 (k, c, v) VALUES (2, 2, 4)")
                                 .withEqualsCondition("row1", 0, "ks.tbl1.v", bytes(3))
                                 .withEqualsCondition("row2", 0, "ks.tbl2.v", bytes(4))
                                 .build();
        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        Assert.assertNotNull(parsed);
        TransactionStatement statement = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());
        Txn actual = statement.createTxn(ClientState.forInternalCalls(), QueryOptions.DEFAULT);
        assertEquals(expected, actual);
    }

    @Test
    public void additionAssignmentTest()
    {
        String query = "BEGIN TRANSACTION\n" +
                       "  SELECT v FROM ks.tbl1 WHERE k=1 AND c=2;\n" +
                       "  UPDATE ks.tbl1 SET v+=1 WHERE k=1 AND c=2;\n" +
                       "  UPDATE ks.tbl2 SET v-=1 WHERE k=2 AND c=2;\n" +
                       "COMMIT TRANSACTION";

        List<TxnReferenceOperation> row1Values = new ArrayList<>();
        row1Values.add(new TxnReferenceOperation(column(TABLE1, "v"),
                                                 new Addition(new Substitution(reference("update1", TABLE1, "v", 0)),
                                                              new Constant(ByteBufferUtil.bytes(1)))));
        TxnReferenceOperations row1Ops = new TxnReferenceOperations(TABLE1, Clustering.make(bytes(2)), row1Values, Collections.emptyList());

        List<TxnReferenceOperation> row2Values = new ArrayList<>();
        row2Values.add(new TxnReferenceOperation(column(TABLE2, "v"),
                                              new Subtraction(new Substitution(reference("update2", TABLE2, "v", 0)),
                                                              new Constant(ByteBufferUtil.bytes(1)))));
        TxnReferenceOperations row2Ops = new TxnReferenceOperations(TABLE2, Clustering.make(bytes(2)), row2Values, Collections.emptyList());

        Txn expected = TxnBuilder.builder()
                                 .withRead("update1", "SELECT * FROM ks.tbl1 WHERE k=1 AND c=2 LIMIT 1")
                                 .withRead("update2", "SELECT * FROM ks.tbl2 WHERE k=2 AND c=2 LIMIT 1")
                                 .withRead("returning", "SELECT v FROM ks.tbl1 WHERE k=1 AND c=2")
                                 .withWrite(emptyUpdate(TABLE1, 1, 2, false), row1Ops)
                                 .withWrite(emptyUpdate(TABLE2, 2, 2, false), row2Ops)
                                 .build();

        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        Assert.assertNotNull(parsed);
        TransactionStatement statement = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());
        Txn actual = statement.createTxn(ClientState.forInternalCalls(), QueryOptions.DEFAULT);
        assertEquals(expected, actual);
    }

    private static PartitionUpdate emptyUpdate(TableMetadata metadata, int k, int c, boolean forInsert)
    {
        DecoratedKey dk = metadata.partitioner.decorateKey(bytes(k));
        RegularAndStaticColumns columns = new RegularAndStaticColumns(Columns.from(metadata.regularColumns()), Columns.NONE);
        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(metadata, dk, columns, 1);

        Row.Builder row = BTreeRow.unsortedBuilder();
        row.newRow(new BufferClustering(bytes(c)));
        if (forInsert)
            row.addPrimaryKeyLivenessInfo(LivenessInfo.create(0, 0));
        builder.add(row.build());

        return builder.build();
    }

    private static ColumnMetadata column(TableMetadata metadata, String name)
    {
        return metadata.getColumn(new ColumnIdentifier(name, true));
    }

    private static ValueReference reference(String name, TableMetadata metadata, String column, int idx)
    {
        return new ValueReference(name, idx, column(metadata, column), null);
    }
}
