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

package org.apache.cassandra.test.microbench;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ICompactionController;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.test.microbench.data.DataGenerator;
import org.apache.cassandra.utils.UUIDGen;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.round;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)

//@BenchmarkMode(Mode.SingleShotTime)
//@Warmup(iterations = 5, batchSize = 50000)
//@Measurement(iterations = 10, batchSize = 200000)

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1)
//@Fork(value = 1, jvmArgs = {"-agentpath:/Users/beggleston/libyjpagent.so=dir=/Users/beggleston/snapshots,sessionname=base,probe_disable=*,sampling,onexit=snapshot"})
@Threads(32)
@State(Scope.Benchmark)
public class CompactionIteratorBench
{
    final AtomicInteger uniqueThreadInitialisation = new AtomicInteger();

    private static final Curve PARTITON_CURVE = new Curve(512, 128,  64,  32,  32);
    private static final Curve ROW_CURVE =      new Curve(64,  128, 128, 128,  64);
    private static final Curve COL_CURVE =      new Curve(2,   4,   8, 16, 32);

    private static final Curve PART_OVERLAP_CURVE = new Curve(0, 0.25, 1.0, 1.0);
    private static final Curve ROW_OVERLAP_CURVE =  new Curve(0, 0,    0.5, 1.0);


    private static final Map<String, Integer> DEFAULT_STEPS = ImmutableMap.<String, Integer>builder()
                                                              .put("concentration", Curve.numSteps(PARTITON_CURVE, 4))
                                                              .put("overlap", 5)
                                                              .build();

    @Param({"0.0"})
    double concentration;

    @Param({"0"})
    float overlap;

    @Param({"4"})
    int clusteringCount;

    @Param({"8", "16"})
    int valueSize;

    @Param({"NORMAL", "COMPLEX"})
    DataGenerator.ColumnType columnType;

    @Param({"1", "8"})
    int streamCount;

    @Param({"SEQUENTIAL"})
    DataGenerator.Distribution distribution;

    @Param({"SEQUENTIAL"})
    DataGenerator.Distribution timestamps;

    @Param({"false"})
    boolean uniquePerTrial;

    @State(Scope.Benchmark)
    public static class GlobalState
    {
        DataGenerator generator;
        ICompactionController controller;

        int partitionCount;
        int rowCount;
        int colCount;

        float rowOverlap;
        float partitionOverlap;

        private static int getPartitionCount(double pos)
        {
            return PARTITON_CURVE.valueInt(pos);
        }

        private static int getRowCount(double pos)
        {
            return ROW_CURVE.valueInt(pos);
        }
        private static int geColCount(double pos)
        {
            return COL_CURVE.valueInt(pos);
        }

        @Setup(Level.Trial)
        public void setup(CompactionIteratorBench bench)
        {
            partitionCount = getPartitionCount(bench.concentration) / bench.streamCount;
            rowCount = getRowCount(bench.concentration);
            colCount = geColCount(bench.concentration);

            this.rowOverlap = ROW_OVERLAP_CURVE.valueInt(bench.overlap);
            this.partitionOverlap = PART_OVERLAP_CURVE.valueInt(bench.overlap);

            generator = new DataGenerator(bench.clusteringCount, colCount, bench.columnType, this.rowOverlap, this.rowCount, bench.valueSize, round(max(1, bench.streamCount * this.partitionOverlap)), bench.distribution, bench.timestamps);
            controller = controller(generator.schema());
        }

        static void expand(String name, double value, Map<String, String> params, Map<String, String> dst)
        {
            if (name.equals("concentration"))
            {
                dst.put("partitionCount", Integer.toString(getPartitionCount(value)));
                dst.put("rowCount", Integer.toString(getRowCount(value)));
                dst.put("colCount", Integer.toString(geColCount(value)));
            }
            if (name.equals("overlap"))
            {
                dst.put("rowOverlap", Integer.toString(ROW_OVERLAP_CURVE.valueInt(value)));
                dst.put("partitionOverlap", Integer.toString(ROW_OVERLAP_CURVE.valueInt(value)));
            }
        }
    }

    @State(Scope.Thread)
    public static class ThreadState
    {
        DataGenerator.PartitionGenerator generator;
        int partitionCount;
        boolean uniquePerTrial;
        int streamCount;
        ICompactionController controller;

        // maybe precomputed work, e.g. if measuring allocations to avoid counting construction of work
        OneCompaction[] compactions;
        int counter;

        // the work to do next invocation
        OneCompaction next;

        @Setup(Level.Trial)
        public void preTrial(CompactionIteratorBench bench, GlobalState state, BenchmarkParams params)
        {
            partitionCount = state.partitionCount;
            streamCount = bench.streamCount;
            uniquePerTrial = bench.uniquePerTrial;
            generator = state.generator.newGenerator(bench.uniqueThreadInitialisation.incrementAndGet());
            controller = state.controller;
            if (!bench.uniquePerTrial)
            {
                int uniqueCount = min(64, max(1, (int) (Runtime.getRuntime().maxMemory() / (4 * (params.getThreads() * partitionCount * bench.streamCount * generator.averageSizeInBytesOfOneBatch())))));
                compactions = IntStream.range(0, uniqueCount)
                                       .mapToObj(f -> generate())
                                       .toArray(OneCompaction[]::new);
            }
        }

        @Setup(Level.Invocation)
        public void preInvocation()
        {
            if (uniquePerTrial)
            {
                next = generate();
            }
            else
            {
                next = compactions[counter++];
                if (counter == compactions.length) counter = 0;
            }
        }

        private OneCompaction generate()
        {
            List<List<PartitionUpdate>> partitions = IntStream.range(0, partitionCount)
                                                              .mapToObj(p -> generator.generate(key(p)))
                                                              .collect(Collectors.toList());

            // input is a list of partitions, made up of multiple versions of the partition to merge
            // need to split this amongst the streams, randomly
            List<PartitionUpdate>[] inverted = new List[streamCount];
            for (int i = 0 ; i < inverted.length ; ++i)
                inverted[i] = new ArrayList<>();

            Random random = new Random(0);
            for (List<PartitionUpdate> partition : partitions)
            {
                shuffle(random, inverted, 0, partition.size(), 0, inverted.length);
                for (int i = 0 ; i < partition.size() ; ++i)
                    inverted[i].add(partition.get(i));
            }

            List<ISSTableScanner> scanners = new ArrayList<>(streamCount);
            for (int i = 0 ; i < inverted.length ; ++i)
                scanners.add(new OneSSTableScanner(inverted[i]));

            return new OneCompaction(controller, scanners);
        }
    }

    private static class OneSSTableScanner implements ISSTableScanner
    {
        final List<PartitionUpdate> partitions;
        int next;

        public OneSSTableScanner(List<PartitionUpdate> partitions)
        {
            this.partitions = partitions;
        }

        public long getLengthInBytes() { return 0; }
        public long getCompressedLengthInBytes() { return 0; }
        public long getCurrentPosition() { return 0; }
        public long getBytesScanned() { return 0; }
        public Set<SSTableReader> getBackingSSTables() { return Collections.emptySet(); }
        public TableMetadata metadata() { return partitions.get(0).metadata(); }
        public void close() { next = 0; }

        public boolean hasNext()
        {
            return next < partitions.size();
        }

        public UnfilteredRowIterator next()
        {
            return partitions.get(next++).unfilteredIterator();
        }
    }

    private static class OneCompaction
    {
        private static AtomicInteger COMPACTION_ID = new AtomicInteger();

        final ICompactionController controller;
        final List<ISSTableScanner> scanners;

        private final long compactionId = COMPACTION_ID.incrementAndGet();
        private long iteration = 0;

        public OneCompaction(ICompactionController controller, List<ISSTableScanner> scanners)
        {
            this.controller = controller;
            this.scanners = scanners;
        }

        void perform()
        {
            long time = (compactionId << 32) + iteration++;
            try (CompactionIterator iter = new CompactionIterator(OperationType.COMPACTION, scanners, controller, 0, UUIDGen.getTimeUUID(time));)
            {
                while (iter.hasNext())
                {
                    try (UnfilteredRowIterator partition = iter.next();)
                    {
                        while (partition.hasNext())
                            partition.next();
                    }
                }
            }
        }
    }

    private static DecoratedKey key(int i)
    {
        ByteBuffer v = Int32Type.instance.decompose(i);
        return new BufferDecoratedKey(new ByteOrderedPartitioner().getToken(v), v);
    }

    private static ICompactionController controller(TableMetadata metadata)
    {
        return new ICompactionController()
        {
            public boolean compactingRepaired()
            {
                return false;
            }

            public String getKeyspace()
            {
                return "";
            }

            public String getColumnFamily()
            {
                return "";
            }

            public TableMetadata metadata()
            {
                return metadata;
            }

            public Iterable<UnfilteredRowIterator> shadowSources(DecoratedKey key, boolean tombstoneOnly)
            {
                return null;
            }

            public int gcBefore()
            {
                return 0;
            }

            public CompactionParams.TombstoneOption tombstoneOption()
            {
                return CompactionParams.TombstoneOption.NONE;
            }

            public LongPredicate getPurgeEvaluator(DecoratedKey key)
            {
                return time -> false;
            }

            public boolean isActive()
            {
                return true;
            }

            public void invalidateCachedPartition(DecoratedKey key)
            {
            }

            public boolean onlyPurgeRepairedTombstones()
            {
                return false;
            }

            public SecondaryIndexManager indexManager()
            {
                return null;
            }

            public boolean hasIndexes()
            {
                return false;
            }

            public void close() throws Exception
            {
            }
        };
    }

    private static void shuffle(Random random, Object[] data, int trgOffset, int trgSize, int srcOffset, int srcSize)
    {
        for (int i = 0 ; i < trgSize ; ++i)
        {
            int swap = srcOffset + srcSize == 0 ? 0 : random.nextInt(srcSize);
            Object tmp = data[swap];
            data[swap] = data[i + trgOffset];
            data[i + trgOffset] = tmp;
        }
    }

    @Benchmark
    public void compact(ThreadState state)
    {
        state.next.perform();
    }

    public static void main(String... args) throws Exception
    {
        Curve.mainHelper(args, CompactionIteratorBench.class, GlobalState::expand, DEFAULT_STEPS);
    }
}
