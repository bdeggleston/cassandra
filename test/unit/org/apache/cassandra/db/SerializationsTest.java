/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db;

import org.apache.cassandra.AbstractSerializationsTester;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataOutputStreamAndChannel;
import org.apache.cassandra.locator.LocatorConfig;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.CallbackInfo;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class SerializationsTest extends AbstractSerializationsTester
{
    Statics statics = new Statics();

    private static final String KEYSPACE1 = "Keyspace1";
    private ByteBuffer startCol = ByteBufferUtil.bytes("Start");
    private ByteBuffer stopCol = ByteBufferUtil.bytes("Stop");
    private Composite emptyCol = Composites.EMPTY;
    public NamesQueryFilter namesPred = new NamesQueryFilter(statics.NamedCols, DBConfig.instance);
    public NamesQueryFilter namesSCPred = new NamesQueryFilter(statics.NamedSCCols, DBConfig.instance);
    public SliceQueryFilter emptyRangePred = new SliceQueryFilter(emptyCol, emptyCol, false, 100, DatabaseDescriptor.instance, Tracing.instance);
    public SliceQueryFilter nonEmptyRangePred = new SliceQueryFilter(CellNames.simpleDense(startCol), CellNames.simpleDense(stopCol), true, 100, DatabaseDescriptor.instance, Tracing.instance);
    public SliceQueryFilter nonEmptyRangeSCPred = new SliceQueryFilter(CellNames.compositeDense(statics.SC, startCol), CellNames.compositeDense(statics.SC, stopCol), true, 100, DatabaseDescriptor.instance, Tracing.instance);
    public SliceByNamesReadCommand.Serializer slicesByNamesSerializer = new SliceByNamesReadCommand.Serializer(Schema.instance, LocatorConfig.instance.getPartitioner(), MessagingService.instance.readCommandSerializer);
    public SliceFromReadCommand.Serializer slicefromReadSerializer = new SliceFromReadCommand.Serializer(Schema.instance, LocatorConfig.instance.getPartitioner(), MessagingService.instance.readCommandSerializer);

    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, "Standard1"),
                                    SchemaLoader.superCFMD(KEYSPACE1, "Super1", LongType.instance));
    }

    private void testRangeSliceCommandWrite() throws IOException
    {
        IPartitioner part = LocatorConfig.instance.getPartitioner();
        AbstractBounds<RowPosition> bounds = new Range<Token>(part.getRandomToken(), part.getRandomToken(), LocatorConfig.instance.getPartitioner()).toRowBounds();

        RangeSliceCommand.Serializer serializer = MessagingService.instance.rangeSliceCommandSerializer;

        RangeSliceCommand namesCmd = new RangeSliceCommand(statics.KS, "Standard1", statics.readTs, namesPred, bounds, 100, DatabaseDescriptor.instance, KeyspaceManager.instance, serializer);
        MessageOut<RangeSliceCommand> namesCmdMsg = namesCmd.createMessage();
        RangeSliceCommand emptyRangeCmd = new RangeSliceCommand(statics.KS, "Standard1", statics.readTs, emptyRangePred, bounds, 100, DatabaseDescriptor.instance, KeyspaceManager.instance, serializer);
        MessageOut<RangeSliceCommand> emptyRangeCmdMsg = emptyRangeCmd.createMessage();
        RangeSliceCommand regRangeCmd = new RangeSliceCommand(statics.KS, "Standard1", statics.readTs, nonEmptyRangePred, bounds, 100, DatabaseDescriptor.instance, KeyspaceManager.instance, serializer);
        MessageOut<RangeSliceCommand> regRangeCmdMsg = regRangeCmd.createMessage();
        RangeSliceCommand namesCmdSup = new RangeSliceCommand(statics.KS, "Super1", statics.readTs, namesSCPred, bounds, 100, DatabaseDescriptor.instance, KeyspaceManager.instance, serializer);
        MessageOut<RangeSliceCommand> namesCmdSupMsg = namesCmdSup.createMessage();
        RangeSliceCommand emptyRangeCmdSup = new RangeSliceCommand(statics.KS, "Super1", statics.readTs, emptyRangePred, bounds, 100, DatabaseDescriptor.instance, KeyspaceManager.instance, serializer);
        MessageOut<RangeSliceCommand> emptyRangeCmdSupMsg = emptyRangeCmdSup.createMessage();
        RangeSliceCommand regRangeCmdSup = new RangeSliceCommand(statics.KS, "Super1", statics.readTs, nonEmptyRangeSCPred, bounds, 100, DatabaseDescriptor.instance, KeyspaceManager.instance, serializer);
        MessageOut<RangeSliceCommand> regRangeCmdSupMsg = regRangeCmdSup.createMessage();

        DataOutputStreamAndChannel out = getOutput("db.RangeSliceCommand.bin");
        namesCmdMsg.serialize(out, getVersion());
        emptyRangeCmdMsg.serialize(out, getVersion());
        regRangeCmdMsg.serialize(out, getVersion());
        namesCmdSupMsg.serialize(out, getVersion());
        emptyRangeCmdSupMsg.serialize(out, getVersion());
        regRangeCmdSupMsg.serialize(out, getVersion());
        out.close();

        // test serializedSize
        testSerializedSize(namesCmd, serializer);
        testSerializedSize(emptyRangeCmd, serializer);
        testSerializedSize(regRangeCmd, serializer);
        testSerializedSize(namesCmdSup, serializer);
        testSerializedSize(emptyRangeCmdSup, serializer);
        testSerializedSize(regRangeCmdSup, serializer);
    }

    @Test
    public void testRangeSliceCommandRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testRangeSliceCommandWrite();

        DataInputStream in = getInput("db.RangeSliceCommand.bin");
        for (int i = 0; i < 6; i++)
            MessageIn.read(in, getVersion(), -1);
        in.close();
    }

    private void testSliceByNamesReadCommandWrite() throws IOException
    {
        SliceByNamesReadCommand standardCmd = new SliceByNamesReadCommand(statics.KS,
                                                                          statics.Key,
                                                                          statics.StandardCF,
                                                                          statics.readTs,
                                                                          namesPred,
                                                                          Schema.instance,
                                                                          LocatorConfig.instance.getPartitioner(),
                                                                          MessagingService.instance.readCommandSerializer);
        SliceByNamesReadCommand superCmd = new SliceByNamesReadCommand(statics.KS,
                                                                       statics.Key,
                                                                       statics.SuperCF,
                                                                       statics.readTs,
                                                                       namesSCPred,
                                                                       Schema.instance,
                                                                       LocatorConfig.instance.getPartitioner(),
                                                                       MessagingService.instance.readCommandSerializer);

        DataOutputStreamAndChannel out = getOutput("db.SliceByNamesReadCommand.bin");
        slicesByNamesSerializer.serialize(standardCmd, out, getVersion());
        slicesByNamesSerializer.serialize(superCmd, out, getVersion());
        MessagingService.instance.readCommandSerializer.serialize(standardCmd, out, getVersion());
        MessagingService.instance.readCommandSerializer.serialize(superCmd, out, getVersion());
        standardCmd.createMessage().serialize(out, getVersion());
        superCmd.createMessage().serialize(out, getVersion());
        out.close();

        // test serializedSize
        testSerializedSize(standardCmd, slicesByNamesSerializer);
        testSerializedSize(superCmd, slicesByNamesSerializer);
    }

    @Test
    public void testSliceByNamesReadCommandRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testSliceByNamesReadCommandWrite();

        DataInputStream in = getInput("db.SliceByNamesReadCommand.bin");
        assert slicesByNamesSerializer.deserialize(in, getVersion()) != null;
        assert slicesByNamesSerializer.deserialize(in, getVersion()) != null;
        assert MessagingService.instance.readCommandSerializer.deserialize(in, getVersion()) != null;
        assert MessagingService.instance.readCommandSerializer.deserialize(in, getVersion()) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        in.close();
    }

    private void testSliceFromReadCommandWrite() throws IOException
    {
        SliceFromReadCommand standardCmd = new SliceFromReadCommand(statics.KS,
                                                                    statics.Key,
                                                                    statics.StandardCF,
                                                                    statics.readTs,
                                                                    nonEmptyRangePred,
                                                                    Schema.instance,
                                                                    LocatorConfig.instance.getPartitioner(),
                                                                    MessagingService.instance.readCommandSerializer);
        SliceFromReadCommand superCmd = new SliceFromReadCommand(statics.KS,
                                                                 statics.Key,
                                                                 statics.SuperCF,
                                                                 statics.readTs,
                                                                 nonEmptyRangeSCPred,
                                                                 Schema.instance,
                                                                 LocatorConfig.instance.getPartitioner(),
                                                                 MessagingService.instance.readCommandSerializer);

        DataOutputStreamAndChannel out = getOutput("db.SliceFromReadCommand.bin");
        slicefromReadSerializer.serialize(standardCmd, out, getVersion());
        slicefromReadSerializer.serialize(superCmd, out, getVersion());
        MessagingService.instance.readCommandSerializer.serialize(standardCmd, out, getVersion());
        MessagingService.instance.readCommandSerializer.serialize(superCmd, out, getVersion());
        standardCmd.createMessage().serialize(out, getVersion());
        superCmd.createMessage().serialize(out, getVersion());

        out.close();

        // test serializedSize
        testSerializedSize(standardCmd, slicefromReadSerializer);
        testSerializedSize(superCmd, slicefromReadSerializer);
    }

    @Test
    public void testSliceFromReadCommandRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testSliceFromReadCommandWrite();

        DataInputStream in = getInput("db.SliceFromReadCommand.bin");
        assert slicefromReadSerializer.deserialize(in, getVersion()) != null;
        assert slicefromReadSerializer.deserialize(in, getVersion()) != null;
        assert MessagingService.instance.readCommandSerializer.deserialize(in, getVersion()) != null;
        assert MessagingService.instance.readCommandSerializer.deserialize(in, getVersion()) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        in.close();
    }

    private void testRowWrite() throws IOException
    {
        DataOutputStreamAndChannel out = getOutput("db.Row.bin");
        DBConfig.instance.rowSerializer.serialize(statics.StandardRow, out, getVersion());
        DBConfig.instance.rowSerializer.serialize(statics.SuperRow, out, getVersion());
        DBConfig.instance.rowSerializer.serialize(statics.NullRow, out, getVersion());
        out.close();

        // test serializedSize
        testSerializedSize(statics.StandardRow, DBConfig.instance.rowSerializer);
        testSerializedSize(statics.SuperRow, DBConfig.instance.rowSerializer);
        testSerializedSize(statics.NullRow, DBConfig.instance.rowSerializer);
    }

    @Test
    public void testRowRead() throws IOException
    {
        // Since every table creation generates different CF ID,
        // we need to generate file every time
        testRowWrite();

        DataInputStream in = getInput("db.Row.bin");
        assert DBConfig.instance.rowSerializer.deserialize(in, getVersion()) != null;
        assert DBConfig.instance.rowSerializer.deserialize(in, getVersion()) != null;
        assert DBConfig.instance.rowSerializer.deserialize(in, getVersion()) != null;
        in.close();
    }

    private void testMutationWrite() throws IOException
    {
        Mutation standardRowRm = MutationFactory.instance.create(statics.KS, statics.StandardRow);
        Mutation superRowRm = MutationFactory.instance.create(statics.KS, statics.SuperRow);
        Mutation standardRm = MutationFactory.instance.create(statics.KS, statics.Key, statics.StandardCf);
        Mutation superRm = MutationFactory.instance.create(statics.KS, statics.Key, statics.SuperCf);
        Map<UUID, ColumnFamily> mods = new HashMap<UUID, ColumnFamily>();
        mods.put(statics.StandardCf.metadata().cfId, statics.StandardCf);
        mods.put(statics.SuperCf.metadata().cfId, statics.SuperCf);
        Mutation mixedRm = MutationFactory.instance.create(statics.KS, statics.Key, mods);

        DataOutputStreamAndChannel out = getOutput("db.RowMutation.bin");
        MutationFactory.instance.serializer.serialize(standardRowRm, out, getVersion());
        MutationFactory.instance.serializer.serialize(superRowRm, out, getVersion());
        MutationFactory.instance.serializer.serialize(standardRm, out, getVersion());
        MutationFactory.instance.serializer.serialize(superRm, out, getVersion());
        MutationFactory.instance.serializer.serialize(mixedRm, out, getVersion());

        standardRowRm.createMessage().serialize(out, getVersion());
        superRowRm.createMessage().serialize(out, getVersion());
        standardRm.createMessage().serialize(out, getVersion());
        superRm.createMessage().serialize(out, getVersion());
        mixedRm.createMessage().serialize(out, getVersion());

        out.close();

        // test serializedSize
        testSerializedSize(standardRowRm, MutationFactory.instance.serializer);
        testSerializedSize(superRowRm, MutationFactory.instance.serializer);
        testSerializedSize(standardRm, MutationFactory.instance.serializer);
        testSerializedSize(superRm, MutationFactory.instance.serializer);
        testSerializedSize(mixedRm, MutationFactory.instance.serializer);
    }

    @Test
    public void testMutationRead() throws IOException
    {
        // mutation deserialization requires being able to look up the keyspace in the schema,
        // so we need to rewrite this each time. plus, CF ID is different for every run.
        testMutationWrite();

        DataInputStream in = getInput("db.RowMutation.bin");
        assert MutationFactory.instance.serializer.deserialize(in, getVersion()) != null;
        assert MutationFactory.instance.serializer.deserialize(in, getVersion()) != null;
        assert MutationFactory.instance.serializer.deserialize(in, getVersion()) != null;
        assert MutationFactory.instance.serializer.deserialize(in, getVersion()) != null;
        assert MutationFactory.instance.serializer.deserialize(in, getVersion()) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        in.close();
    }

    private void testTruncateWrite() throws IOException
    {
        Truncation tr = new Truncation(statics.KS, "Doesn't Really Matter");
        TruncateResponse aff = new TruncateResponse(statics.KS, "Doesn't Matter Either", true);
        TruncateResponse neg = new TruncateResponse(statics.KS, "Still Doesn't Matter", false);
        DataOutputStreamAndChannel out = getOutput("db.Truncation.bin");
        Truncation.serializer.serialize(tr, out, getVersion());
        TruncateResponse.serializer.serialize(aff, out, getVersion());
        TruncateResponse.serializer.serialize(neg, out, getVersion());

        tr.createMessage().serialize(out, getVersion());
        aff.createMessage().serialize(out, getVersion());
        neg.createMessage().serialize(out, getVersion());
        // todo: notice how CF names weren't validated.
        out.close();

        // test serializedSize
        testSerializedSize(tr, Truncation.serializer);
        testSerializedSize(aff, TruncateResponse.serializer);
        testSerializedSize(neg, TruncateResponse.serializer);
    }

    @Test
    public void testTruncateRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testTruncateWrite();

        DataInputStream in = getInput("db.Truncation.bin");
        assert Truncation.serializer.deserialize(in, getVersion()) != null;
        assert TruncateResponse.serializer.deserialize(in, getVersion()) != null;
        assert TruncateResponse.serializer.deserialize(in, getVersion()) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;

        // set up some fake callbacks so deserialization knows that what it's deserializing is a TruncateResponse
        MessagingService.instance.setCallbackForTests(1, new CallbackInfo(null, null, TruncateResponse.serializer));
        MessagingService.instance.setCallbackForTests(2, new CallbackInfo(null, null, TruncateResponse.serializer));

        assert MessageIn.read(in, getVersion(), 1) != null;
        assert MessageIn.read(in, getVersion(), 2) != null;
        in.close();
    }

    private void testWriteResponseWrite() throws IOException
    {
        WriteResponse aff = new WriteResponse();
        WriteResponse neg = new WriteResponse();
        DataOutputStreamAndChannel out = getOutput("db.WriteResponse.bin");
        WriteResponse.serializer.serialize(aff, out, getVersion());
        WriteResponse.serializer.serialize(neg, out, getVersion());
        out.close();

        // test serializedSize
        testSerializedSize(aff, WriteResponse.serializer);
        testSerializedSize(neg, WriteResponse.serializer);
    }

    @Test
    public void testWriteResponseRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testWriteResponseWrite();

        DataInputStream in = getInput("db.WriteResponse.bin");
        assert WriteResponse.serializer.deserialize(in, getVersion()) != null;
        assert WriteResponse.serializer.deserialize(in, getVersion()) != null;
        in.close();
    }

    private static ByteBuffer bb(String s)
    {
        return ByteBufferUtil.bytes(s);
    }

    private static CellName cn(String s)
    {
        return CellNames.simpleDense(ByteBufferUtil.bytes(s));
    }

    private static class Statics
    {
        private final String KS = KEYSPACE1;
        private final ByteBuffer Key = ByteBufferUtil.bytes("Key01");
        private final SortedSet<CellName> NamedCols = new TreeSet<CellName>(new SimpleDenseCellNameType(BytesType.instance, DatabaseDescriptor.instance, Tracing.instance, DBConfig.instance))
        {{
            add(CellNames.simpleDense(ByteBufferUtil.bytes("AAA")));
            add(CellNames.simpleDense(ByteBufferUtil.bytes("BBB")));
            add(CellNames.simpleDense(ByteBufferUtil.bytes("CCC")));
        }};
        private final ByteBuffer SC = ByteBufferUtil.bytes("SCName");
        private final SortedSet<CellName> NamedSCCols = new TreeSet<CellName>(new CompoundDenseCellNameType(Arrays.<AbstractType<?>>asList(BytesType.instance, BytesType.instance), DatabaseDescriptor.instance, Tracing.instance, DBConfig.instance))
        {{
            add(CellNames.compositeDense(SC, ByteBufferUtil.bytes("AAA")));
            add(CellNames.compositeDense(SC, ByteBufferUtil.bytes("BBB")));
            add(CellNames.compositeDense(SC, ByteBufferUtil.bytes("CCC")));
        }};
        private final String StandardCF = "Standard1";
        private final String SuperCF = "Super1";

        private final long readTs = 1369935512292L;

        private final ColumnFamily StandardCf = ArrayBackedSortedColumns.factory.create(KS, StandardCF, Schema.instance, DBConfig.instance);
        private final ColumnFamily SuperCf = ArrayBackedSortedColumns.factory.create(KS, SuperCF, Schema.instance, DBConfig.instance);

        private final Row StandardRow = new Row(Util.dk("key0"), StandardCf);
        private final Row SuperRow = new Row(Util.dk("key1"), SuperCf);
        private final Row NullRow = new Row(Util.dk("key2"), null);

        private Statics()
        {
            StandardCf.addColumn(new BufferCell(cn("aaaa")));
            StandardCf.addColumn(new BufferCell(cn("bbbb"), bb("bbbbb-value")));
            StandardCf.addColumn(new BufferCell(cn("cccc"), bb("ccccc-value"), 1000L));
            StandardCf.addColumn(new BufferDeletedCell(cn("dddd"), 500, 1000));
            StandardCf.addColumn(new BufferDeletedCell(cn("eeee"), bb("eeee-value"), 1001));
            StandardCf.addColumn(new BufferExpiringCell(cn("ffff"), bb("ffff-value"), 2000, 1000));
            StandardCf.addColumn(new BufferExpiringCell(cn("gggg"), bb("gggg-value"), 2001, 1000, 2002));

            SuperCf.addColumn(new BufferCell(CellNames.compositeDense(SC, bb("aaaa"))));
            SuperCf.addColumn(new BufferCell(CellNames.compositeDense(SC, bb("bbbb")), bb("bbbbb-value")));
            SuperCf.addColumn(new BufferCell(CellNames.compositeDense(SC, bb("cccc")), bb("ccccc-value"), 1000L));
            SuperCf.addColumn(new BufferDeletedCell(CellNames.compositeDense(SC, bb("dddd")), 500, 1000));
            SuperCf.addColumn(new BufferDeletedCell(CellNames.compositeDense(SC, bb("eeee")), bb("eeee-value"), 1001));
            SuperCf.addColumn(new BufferExpiringCell(CellNames.compositeDense(SC, bb("ffff")), bb("ffff-value"), 2000, 1000));
            SuperCf.addColumn(new BufferExpiringCell(CellNames.compositeDense(SC, bb("gggg")), bb("gggg-value"), 2001, 1000, 2002));
        }
    }
}
