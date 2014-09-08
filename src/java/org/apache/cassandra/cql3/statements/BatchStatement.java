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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Function;
import com.google.common.collect.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.locator.LocatorConfig;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;
import org.github.jamm.MemoryMeter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * A <code>BATCH</code> statement parsed from a CQL query.
 *
 */
public class BatchStatement implements CQLStatement, MeasurableForPreparedCache
{
    public static enum Type
    {
        LOGGED, UNLOGGED, COUNTER
    }

    private final int boundTerms;
    public final Type type;
    private final List<ModificationStatement> statements;
    private final Attributes attrs;
    private final boolean hasConditions;
    private static final Logger logger = LoggerFactory.getLogger(BatchStatement.class);

    private final DatabaseDescriptor databaseDescriptor;
    private final Tracing tracing;
    private final Schema schema;
    private final QueryProcessor queryProcessor;
    private final KeyspaceManager keyspaceManager;
    private final StorageProxy storageProxy;
    private final MutationFactory mutationFactory;
    private final CounterMutationFactory counterMutationFactory;
    private final MessagingService messagingService;
    private final DBConfig dbConfig;
    private final LocatorConfig locatorConfig;

    /**
     * Creates a new BatchStatement from a list of statements and a
     * Thrift consistency level.
     *
     * @param type type of the batch
     * @param statements a list of UpdateStatements
     * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
     */
    public BatchStatement(int boundTerms, Type type, List<ModificationStatement> statements, Attributes attrs,
                          DatabaseDescriptor databaseDescriptor, Tracing tracing, Schema schema, QueryProcessor queryProcessor,
                          KeyspaceManager keyspaceManager, StorageProxy storageProxy, MutationFactory mutationFactory,
                          CounterMutationFactory counterMutationFactory, MessagingService messagingService, DBConfig dbConfig, LocatorConfig locatorConfig)
    {
        boolean hasConditions = false;
        for (ModificationStatement statement : statements)
            hasConditions |= statement.hasConditions();

        this.boundTerms = boundTerms;
        this.type = type;
        this.statements = statements;
        this.attrs = attrs;
        this.hasConditions = hasConditions;

        this.databaseDescriptor = databaseDescriptor;
        this.tracing = tracing;
        this.schema = schema;
        this.queryProcessor = queryProcessor;
        this.keyspaceManager = keyspaceManager;
        this.storageProxy = storageProxy;
        this.mutationFactory = mutationFactory;
        this.counterMutationFactory = counterMutationFactory;
        this.messagingService = messagingService;
        this.dbConfig = dbConfig;
        this.locatorConfig = locatorConfig;
    }

    public long measureForPreparedCache(MemoryMeter meter)
    {
        long size = meter.measure(this)
                  + meter.measureDeep(type)
                  + meter.measure(statements)
                  + meter.measureDeep(attrs);
        for (ModificationStatement stmt : statements)
            size += stmt.measureForPreparedCache(meter);
        return size;
    }

    public int getBoundTerms()
    {
        return boundTerms;
    }

    public void checkAccess(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        for (ModificationStatement statement : statements)
            statement.checkAccess(state);
    }

    // Validates a prepared batch statement without validating its nested statements.
    public void validate() throws InvalidRequestException
    {
        if (attrs.isTimeToLiveSet())
            throw new InvalidRequestException("Global TTL on the BATCH statement is not supported.");

        boolean timestampSet = attrs.isTimestampSet();
        if (timestampSet)
        {
            if (hasConditions)
                throw new InvalidRequestException("Cannot provide custom timestamp for conditional BATCH");
            if (type == Type.COUNTER)
                throw new InvalidRequestException("Cannot provide custom timestamp for counter BATCH");
        }

        boolean hasCounters = false;
        boolean hasNonCounters = false;

        for (ModificationStatement statement : statements)
        {
            if (timestampSet && statement.isCounter())
                throw new InvalidRequestException("Cannot provide custom timestamp for a BATCH containing counters");

            if (timestampSet && statement.isTimestampSet())
                throw new InvalidRequestException("Timestamp must be set either on BATCH or individual statements");

            if (type == Type.COUNTER && !statement.isCounter())
                throw new InvalidRequestException("Cannot include non-counter statement in a counter batch");

            if (type == Type.LOGGED && statement.isCounter())
                throw new InvalidRequestException("Cannot include a counter statement in a logged batch");

            if (statement.isCounter())
                hasCounters = true;
            else
                hasNonCounters = true;
        }

        if (hasCounters && hasNonCounters)
            throw new InvalidRequestException("Counter and non-counter mutations cannot exist in the same batch");

        if (hasConditions)
        {
            String ksName = null;
            String cfName = null;
            for (ModificationStatement stmt : statements)
            {
                if (ksName != null && (!stmt.keyspace().equals(ksName) || !stmt.columnFamily().equals(cfName)))
                    throw new InvalidRequestException("Batch with conditions cannot span multiple tables");
                ksName = stmt.keyspace();
                cfName = stmt.columnFamily();
            }
        }
    }

    // The batch itself will be validated in either Parsed#prepare() - for regular CQL3 batches,
    //   or in QueryProcessor.instance.processBatch() - for native protocol batches.
    public void validate(ClientState state) throws InvalidRequestException
    {
        for (ModificationStatement statement : statements)
            statement.validate(state);
    }

    public List<ModificationStatement> getStatements()
    {
        return statements;
    }

    private Collection<? extends IMutation> getMutations(BatchQueryOptions options, boolean local, long now)
    throws RequestExecutionException, RequestValidationException
    {
        Map<String, Map<ByteBuffer, IMutation>> mutations = new HashMap<>();
        for (int i = 0; i < statements.size(); i++)
        {
            ModificationStatement statement = statements.get(i);
            QueryOptions statementOptions = options.forStatement(i);
            long timestamp = attrs.getTimestamp(now, statementOptions);
            addStatementMutations(statement, statementOptions, local, timestamp, mutations);
        }
        return unzipMutations(mutations);
    }

    private Collection<? extends IMutation> unzipMutations(Map<String, Map<ByteBuffer, IMutation>> mutations)
    {
        // The case where all statement where on the same keyspace is pretty common
        if (mutations.size() == 1)
            return mutations.values().iterator().next().values();

        List<IMutation> ms = new ArrayList<>();
        for (Map<ByteBuffer, IMutation> ksMap : mutations.values())
            ms.addAll(ksMap.values());
        return ms;
    }

    private void addStatementMutations(ModificationStatement statement,
                                       QueryOptions options,
                                       boolean local,
                                       long now,
                                       Map<String, Map<ByteBuffer, IMutation>> mutations)
    throws RequestExecutionException, RequestValidationException
    {
        String ksName = statement.keyspace();
        Map<ByteBuffer, IMutation> ksMap = mutations.get(ksName);
        if (ksMap == null)
        {
            ksMap = new HashMap<>();
            mutations.put(ksName, ksMap);
        }

        // The following does the same than statement.getMutations(), but we inline it here because
        // we don't want to recreate mutations every time as this is particularly inefficient when applying
        // multiple batch to the same partition (see #6737).
        List<ByteBuffer> keys = statement.buildPartitionKeyNames(options);
        Composite clusteringPrefix = statement.createClusteringPrefix(options);
        UpdateParameters params = statement.makeUpdateParameters(keys, clusteringPrefix, options, local, now);

        for (ByteBuffer key : keys)
        {
            IMutation mutation = ksMap.get(key);
            Mutation mut;
            if (mutation == null)
            {
                mut = mutationFactory.create(ksName, key);
                mutation = statement.cfm.isCounter() ? counterMutationFactory.create(mut, options.getConsistency()) : mut;
                ksMap.put(key, mutation);
            }
            else
            {
                mut = statement.cfm.isCounter() ? ((CounterMutation)mutation).getMutation() : (Mutation)mutation;
            }

            statement.addUpdateForKey(mut.addOrGet(statement.cfm), key, clusteringPrefix, params);
        }
    }

    /**
     * Checks batch size to ensure threshold is met. If not, a warning is logged.
     * @param cfs ColumnFamilies that will store the batch's mutations.
     */
    public static void verifyBatchSize(Iterable<ColumnFamily> cfs, DatabaseDescriptor databaseDescriptor)
    {
        long size = 0;
        long warnThreshold = databaseDescriptor.getBatchSizeWarnThreshold();

        for (ColumnFamily cf : cfs)
            size += cf.dataSize();

        if (size > warnThreshold)
        {
            Set<String> ksCfPairs = new HashSet<>();
            for (ColumnFamily cf : cfs)
                ksCfPairs.add(cf.metadata().ksName + "." + cf.metadata().cfName);

            String format = "Batch of prepared statements for {} is of size {}, exceeding specified threshold of {} by {}.";
            logger.warn(format, ksCfPairs, size, warnThreshold, size - warnThreshold);
        }
    }

    public ResultMessage execute(QueryState queryState, QueryOptions options) throws RequestExecutionException, RequestValidationException
    {
        return execute(queryState, BatchQueryOptions.withoutPerStatementVariables(options));
    }

    public ResultMessage execute(QueryState queryState, BatchQueryOptions options) throws RequestExecutionException, RequestValidationException
    {
        return execute(queryState, options, false, options.getTimestamp(queryState));
    }

    private ResultMessage execute(QueryState queryState, BatchQueryOptions options, boolean local, long now)
    throws RequestExecutionException, RequestValidationException
    {
        if (options.getConsistency() == null)
            throw new InvalidRequestException("Invalid empty consistency level");
        if (options.getSerialConsistency() == null)
            throw new InvalidRequestException("Invalid empty serial consistency level");

        if (hasConditions)
            return executeWithConditions(options, now);

        executeWithoutConditions(getMutations(options, local, now), options.getConsistency());
        return new ResultMessage.Void();
    }

    private void executeWithoutConditions(Collection<? extends IMutation> mutations, ConsistencyLevel cl) throws RequestExecutionException, RequestValidationException
    {
        // Extract each collection of cfs from it's IMutation and then lazily concatenate all of them into a single Iterable.
        Iterable<ColumnFamily> cfs = Iterables.concat(Iterables.transform(mutations, new Function<IMutation, Collection<ColumnFamily>>()
        {
            public Collection<ColumnFamily> apply(IMutation im)
            {
                return im.getColumnFamilies();
            }
        }));
        verifyBatchSize(cfs, databaseDescriptor);

        boolean mutateAtomic = (type == Type.LOGGED && mutations.size() > 1);
        storageProxy.mutateWithTriggers(mutations, cl, mutateAtomic);
    }

    private ResultMessage executeWithConditions(BatchQueryOptions options, long now)
    throws RequestExecutionException, RequestValidationException
    {
        ByteBuffer key = null;
        String ksName = null;
        String cfName = null;
        CQL3CasRequest casRequest = null;
        Set<ColumnDefinition> columnsWithConditions = new LinkedHashSet<>();

        for (int i = 0; i < statements.size(); i++)
        {
            ModificationStatement statement = statements.get(i);
            QueryOptions statementOptions = options.forStatement(i);
            long timestamp = attrs.getTimestamp(now, statementOptions);
            List<ByteBuffer> pks = statement.buildPartitionKeyNames(statementOptions);
            if (pks.size() > 1)
                throw new IllegalArgumentException("Batch with conditions cannot span multiple partitions (you cannot use IN on the partition key)");
            if (key == null)
            {
                key = pks.get(0);
                ksName = statement.cfm.ksName;
                cfName = statement.cfm.cfName;
                casRequest = new CQL3CasRequest(statement.cfm, key, true, databaseDescriptor, dbConfig, tracing);
            }
            else if (!key.equals(pks.get(0)))
            {
                throw new InvalidRequestException("Batch with conditions cannot span multiple partitions");
            }

            Composite clusteringPrefix = statement.createClusteringPrefix(statementOptions);
            if (statement.hasConditions())
            {
                statement.addConditions(clusteringPrefix, casRequest, statementOptions);
                // As soon as we have a ifNotExists, we set columnsWithConditions to null so that everything is in the resultSet
                if (statement.hasIfNotExistCondition() || statement.hasIfExistCondition())
                    columnsWithConditions = null;
                else if (columnsWithConditions != null)
                    Iterables.addAll(columnsWithConditions, statement.getColumnsWithConditions());
            }
            casRequest.addRowUpdate(clusteringPrefix, statement, statementOptions, timestamp);
        }

        ColumnFamily result = storageProxy.cas(ksName, cfName, key, casRequest, options.getSerialConsistency(), options.getConsistency());

        return new ResultMessage.Rows(ModificationStatement.buildCasResultSet(ksName,
                                                                              key,
                                                                              cfName,
                                                                              result,
                                                                              columnsWithConditions,
                                                                              true,
                                                                              options.forStatement(0),
                                                                              databaseDescriptor,
                                                                              tracing,
                                                                              schema,
                                                                              queryProcessor,
                                                                              keyspaceManager,
                                                                              storageProxy,
                                                                              messagingService,
                                                                              locatorConfig));
    }

    public ResultMessage executeInternal(QueryState queryState, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        assert !hasConditions;
        for (IMutation mutation : getMutations(BatchQueryOptions.withoutPerStatementVariables(options), true, queryState.getTimestamp()))
        {
            // We don't use counters internally.
            assert mutation instanceof Mutation;
            ((Mutation) mutation).apply();
        }
        return null;
    }

    public interface BatchVariables
    {
        public List<ByteBuffer> getVariablesForStatement(int statementInBatch);
    }

    public String toString()
    {
        return String.format("BatchStatement(type=%s, statements=%s)", type, statements);
    }

    public static class Parsed extends CFStatement
    {
        private final Type type;
        private final Attributes.Raw attrs;
        private final List<ModificationStatement.Parsed> parsedStatements;

        private final DatabaseDescriptor databaseDescriptor;
        private final Tracing tracing;
        private final Schema schema;
        private final QueryProcessor queryProcessor;
        private final KeyspaceManager keyspaceManager;
        private final StorageProxy storageProxy;
        private final MutationFactory mutationFactory;
        private final CounterMutationFactory counterMutationFactory;
        private final MessagingService messagingService;
        private final DBConfig dbConfig;
        private final LocatorConfig locatorConfig;

        public Parsed(Type type, Attributes.Raw attrs, List<ModificationStatement.Parsed> parsedStatements,
                      DatabaseDescriptor databaseDescriptor, Tracing tracing, Schema schema, QueryProcessor queryProcessor,
                      KeyspaceManager keyspaceManager, StorageProxy storageProxy, MutationFactory mutationFactory,
                      CounterMutationFactory counterMutationFactory, MessagingService messagingService, DBConfig dbConfig, LocatorConfig locatorConfig)
        {
            super(null);
            this.type = type;
            this.attrs = attrs;
            this.parsedStatements = parsedStatements;

            this.databaseDescriptor = databaseDescriptor;
            this.tracing = tracing;
            this.schema = schema;
            this.queryProcessor = queryProcessor;
            this.keyspaceManager = keyspaceManager;
            this.storageProxy = storageProxy;
            this.mutationFactory = mutationFactory;
            this.counterMutationFactory = counterMutationFactory;
            this.messagingService = messagingService;
            this.dbConfig = dbConfig;
            this.locatorConfig = locatorConfig;
        }

        @Override
        public void prepareKeyspace(ClientState state) throws InvalidRequestException
        {
            for (ModificationStatement.Parsed statement : parsedStatements)
                statement.prepareKeyspace(state);
        }

        public ParsedStatement.Prepared prepare() throws InvalidRequestException
        {
            VariableSpecifications boundNames = getBoundVariables();

            List<ModificationStatement> statements = new ArrayList<>(parsedStatements.size());
            for (ModificationStatement.Parsed parsed : parsedStatements)
                statements.add(parsed.prepare(boundNames));

            Attributes prepAttrs = attrs.prepare("[batch]", "[batch]");
            prepAttrs.collectMarkerSpecification(boundNames);

            BatchStatement batchStatement = new BatchStatement(boundNames.size(), type, statements, prepAttrs,
                                                               databaseDescriptor, tracing, schema, queryProcessor,
                                                               keyspaceManager, storageProxy, mutationFactory,
                                                               counterMutationFactory, messagingService, dbConfig, locatorConfig);
            batchStatement.validate();

            return new ParsedStatement.Prepared(batchStatement, boundNames);
        }
    }
}
