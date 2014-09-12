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
package org.apache.cassandra.dht;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.db.DBConfig;
import org.apache.cassandra.db.KeyspaceManager;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.locator.LocatorConfig;
import org.apache.cassandra.streaming.StreamManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;

public class BootStrapper
{
    private static final Logger logger = LoggerFactory.getLogger(BootStrapper.class);

    /* endpoint that needs to be bootstrapped */
    protected final InetAddress address;
    /* token of the node being bootstrapped. */
    protected final Collection<Token> tokens;
    protected final TokenMetadata tokenMetadata;

    private final DatabaseDescriptor databaseDescriptor;
    private final Schema schema;
    private final Gossiper gossiper;
    private final KeyspaceManager keyspaceManager;
    private final StreamManager streamManager;
    private final StorageService storageService;
    private final IFailureDetector failureDetector;
    private final DBConfig dbConfig;

    public BootStrapper(InetAddress address, Collection<Token> tokens, TokenMetadata tmd, DatabaseDescriptor databaseDescriptor, Schema schema,
                        Gossiper gossiper, KeyspaceManager keyspaceManager, StreamManager streamManager, StorageService storageService, IFailureDetector failureDetector, DBConfig dbConfig)
    {
        assert address != null;
        assert tokens != null && !tokens.isEmpty();

        this.address = address;
        this.tokens = tokens;
        tokenMetadata = tmd;

        this.databaseDescriptor = databaseDescriptor;
        this.schema = schema;
        this.gossiper = gossiper;
        this.keyspaceManager = keyspaceManager;
        this.streamManager = streamManager;
        this.storageService = storageService;
        this.failureDetector = failureDetector;
        this.dbConfig = dbConfig;
    }

    public void bootstrap()
    {
        if (logger.isDebugEnabled())
            logger.debug("Beginning bootstrap process");

        RangeStreamer streamer = new RangeStreamer(tokenMetadata, tokens, address, "Bootstrap", databaseDescriptor, schema, gossiper, streamManager, keyspaceManager, dbConfig);
        streamer.addSourceFilter(new RangeStreamer.FailureDetectorSourceFilter(failureDetector));

        for (String keyspaceName : schema.getNonSystemKeyspaces())
        {
            AbstractReplicationStrategy strategy = keyspaceManager.open(keyspaceName).getReplicationStrategy();
            streamer.addRanges(keyspaceName, strategy.getPendingAddressRanges(tokenMetadata, tokens, address));
        }

        try
        {
            streamer.fetchAsync().get();
            storageService.finishBootstrapping();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("Interrupted while waiting on boostrap to complete. Bootstrap will have to be restarted.");
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException("Error during boostrap: " + e.getCause().getMessage(), e.getCause());
        }
    }

    /**
     * if initialtoken was specified, use that (split on comma).
     * otherwise, if num_tokens == 1, pick a token to assume half the load of the most-loaded node.
     * else choose num_tokens tokens at random
     */
    public static Collection<Token> getBootstrapTokens(final TokenMetadata metadata, DatabaseDescriptor databaseDescriptor, LocatorConfig locatorConfig) throws ConfigurationException
    {
        Collection<String> initialTokens = locatorConfig.getInitialTokens();
        // if user specified tokens, use those
        if (initialTokens.size() > 0)
        {
            logger.debug("tokens manually specified as {}",  initialTokens);
            List<Token> tokens = new ArrayList<Token>(initialTokens.size());
            for (String tokenString : initialTokens)
            {
                Token token = locatorConfig.getPartitioner().getTokenFactory().fromString(tokenString);
                if (metadata.getEndpoint(token) != null)
                    throw new ConfigurationException("Bootstrapping to existing token " + tokenString + " is not allowed (decommission/removenode the old node first).");
                tokens.add(token);
            }
            return tokens;
        }

        int numTokens = databaseDescriptor.getNumTokens();
        if (numTokens < 1)
            throw new ConfigurationException("num_tokens must be >= 1");

        if (numTokens == 1)
            logger.warn("Picking random token for a single vnode.  You should probably add more vnodes; failing that, you should probably specify the token manually");

        return getRandomTokens(metadata, numTokens, locatorConfig.getPartitioner());
    }

    public static Collection<Token> getRandomTokens(TokenMetadata metadata, int numTokens, IPartitioner partitioner)
    {
        Set<Token> tokens = new HashSet<Token>(numTokens);
        while (tokens.size() < numTokens)
        {
            Token token = partitioner.getRandomToken();
            if (metadata.getEndpoint(token) == null)
                tokens.add(token);
        }
        return tokens;
    }

    public static class StringSerializer implements IVersionedSerializer<String>
    {
        public static final StringSerializer instance = new StringSerializer();

        public void serialize(String s, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(s);
        }

        public String deserialize(DataInput in, int version) throws IOException
        {
            return in.readUTF();
        }

        public long serializedSize(String s, int version)
        {
            return TypeSizes.NATIVE.sizeof(s);
        }
    }
}
