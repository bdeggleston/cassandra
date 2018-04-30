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
package org.apache.cassandra.locator;

import java.util.*;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Datacenters;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.TokenMetadata.Topology;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import com.google.common.collect.Multimap;

/**
 * <p>
 * This Replication Strategy takes a property file that gives the intended
 * replication factor in each datacenter.  The sum total of the datacenter
 * replication factor values should be equal to the keyspace replication
 * factor.
 * </p>
 * <p>
 * So for example, if the keyspace replication factor is 6, the
 * datacenter replication factors could be 3, 2, and 1 - so 3 replicas in
 * one datacenter, 2 in another, and 1 in another - totalling 6.
 * </p>
 * This class also caches the Endpoints and invalidates the cache if there is a
 * change in the number of tokens.
 */
public class NetworkTopologyStrategy extends AbstractReplicationStrategy
{
    private final Map<String, ReplicationFactor> datacenters;
    private final ReplicationFactor aggregateRf;
    private static final Logger logger = LoggerFactory.getLogger(NetworkTopologyStrategy.class);

    public NetworkTopologyStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions) throws ConfigurationException
    {
        super(keyspaceName, tokenMetadata, snitch, configOptions);

        int replicas = 0;
        int trans = 0;
        Map<String, ReplicationFactor> newDatacenters = new HashMap<>();
        if (configOptions != null)
        {
            for (Entry<String, String> entry : configOptions.entrySet())
            {
                String dc = entry.getKey();
                if (dc.equalsIgnoreCase("replication_factor"))
                    throw new ConfigurationException("replication_factor is an option for SimpleStrategy, not NetworkTopologyStrategy");
                ReplicationFactor rf = ReplicationFactor.fromString(entry.getValue());
                replicas += rf.replicas;
                trans += rf.trans;
                newDatacenters.put(dc, rf);
            }
        }

        datacenters = Collections.unmodifiableMap(newDatacenters);
        aggregateRf = ReplicationFactor.rf(replicas, trans);
        logger.info("Configured datacenter replicas are {}", FBUtilities.toString(datacenters));
    }

    /**
     * Endpoint adder applying the replication rules for a given DC.
     */
    private static final class DatacenterEndpoints
    {
        /** List accepted endpoints get pushed into. */
        Set<Replica> replicas;

        /**
         * Racks encountered so far. Replicas are put into separate racks while possible.
         * For efficiency the set is shared between the instances, using the location pair (dc, rack) to make sure
         * clashing names aren't a problem.
         */
        Set<Pair<String, String>> racks;

        /** Number of replicas left to fill from this DC. */
        int rfLeft;
        int acceptableRackRepeats;
        int transients;

        DatacenterEndpoints(ReplicationFactor rf, int rackCount, int nodeCount, Set<Replica> replicas, Set<Pair<String, String>> racks)
        {
            this.replicas = replicas;
            this.racks = racks;
            // If there aren't enough nodes in this DC to fill the RF, the number of nodes is the effective RF.
            this.rfLeft = Math.min(rf.replicas, nodeCount);
            // If there aren't enough racks in this DC to fill the RF, we'll still use at least one node from each rack,
            // and the difference is to be filled by the first encountered nodes.
            acceptableRackRepeats = rf.replicas - rackCount;

            // if we have fewer replicas than rf calls for, reduce transients accordingly
            int reduceTransients = rf.replicas - this.rfLeft;
            transients = Math.max(rf.trans - reduceTransients, 0);
            ReplicationFactor.validate(rfLeft, transients);
        }

        /**
         * Attempts to add an endpoint to the replicas for this datacenter, adding to the replicas set if successful.
         * Returns true if the endpoint was added, and this datacenter does not require further replicas.
         */
        boolean addEndpointAndCheckIfDone(InetAddressAndPort ep, Pair<String,String> location, Range<Token> replicatedRange)
        {
            if (done())
                return false;

            if (Replicas.containsEndpoint(replicas, ep))
                // Cannot repeat a node.
                return false;

            Replica replica = new Replica(ep, replicatedRange, rfLeft > transients);

            if (racks.add(location))
            {
                // New rack.
                --rfLeft;
                boolean added = replicas.add(replica);
                assert added;
                return done();
            }
            if (acceptableRackRepeats <= 0)
                // There must be rfLeft distinct racks left, do not add any more rack repeats.
                return false;

            boolean added = replicas.add(replica);
            assert added;
            // Added a node that is from an already met rack to match RF when there aren't enough racks.
            --acceptableRackRepeats;
            --rfLeft;
            return done();
        }

        boolean done()
        {
            assert rfLeft >= 0;
            return rfLeft == 0;
        }
    }

    /**
     * calculate endpoints in one pass through the tokens by tracking our progress in each DC.
     */
    public List<Replica> calculateNaturalReplicas(Token searchToken, TokenMetadata tokenMetadata)
    {
        // we want to preserve insertion order so that the first added endpoint becomes primary
        Set<Replica> replicas = new LinkedHashSet<>();
        Set<Pair<String, String>> seenRacks = new HashSet<>();

        Topology topology = tokenMetadata.getTopology();
        // all endpoints in each DC, so we can check when we have exhausted all the members of a DC
        Multimap<String, InetAddressAndPort> allEndpoints = topology.getDatacenterEndpoints();
        // all racks in a DC so we can check when we have exhausted all racks in a DC
        Map<String, Multimap<String, InetAddressAndPort>> racks = topology.getDatacenterRacks();
        assert !allEndpoints.isEmpty() && !racks.isEmpty() : "not aware of any cluster members";

        int dcsToFill = 0;
        Map<String, DatacenterEndpoints> dcs = new HashMap<>(datacenters.size() * 2);

        // Create a DatacenterEndpoints object for each non-empty DC.
        for (Map.Entry<String, ReplicationFactor> en : datacenters.entrySet())
        {
            String dc = en.getKey();
            ReplicationFactor rf = en.getValue();
            int nodeCount = sizeOrZero(allEndpoints.get(dc));

            if (rf.replicas <= 0 || nodeCount <= 0)
                continue;

            DatacenterEndpoints dcEndpoints = new DatacenterEndpoints(rf, sizeOrZero(racks.get(dc)), nodeCount, replicas, seenRacks);
            dcs.put(dc, dcEndpoints);
            ++dcsToFill;
        }

        ArrayList<Token> sortedTokens = tokenMetadata.sortedTokens();
        Token replicaEnd = TokenMetadata.firstToken(sortedTokens, searchToken);
        Token replicaStart = tokenMetadata.getPredecessor(replicaEnd);
        Range<Token> replicatedRange = new Range<>(replicaStart, replicaEnd);
        Iterator<Token> tokenIter = TokenMetadata.ringIterator(sortedTokens, searchToken, false);
        while (dcsToFill > 0 && tokenIter.hasNext())
        {
            Token next = tokenIter.next();
            InetAddressAndPort ep = tokenMetadata.getEndpoint(next);
            Pair<String, String> location = topology.getLocation(ep);
            DatacenterEndpoints dcEndpoints = dcs.get(location.left);
            if (dcEndpoints != null && dcEndpoints.addEndpointAndCheckIfDone(ep, location, replicatedRange))
                --dcsToFill;
        }
        return new ArrayList<>(replicas);
    }

    private int sizeOrZero(Multimap<?, ?> collection)
    {
        return collection != null ? collection.asMap().size() : 0;
    }

    private int sizeOrZero(Collection<?> collection)
    {
        return collection != null ? collection.size() : 0;
    }

    public ReplicationFactor getReplicationFactor()
    {
        return aggregateRf;
    }

    public ReplicationFactor getReplicationFactor(String dc)
    {
        ReplicationFactor replicas = datacenters.get(dc);
        return replicas == null ? ReplicationFactor.ZERO : replicas;
    }

    public Set<String> getDatacenters()
    {
        return datacenters.keySet();
    }

    public Collection<String> recognizedOptions()
    {
        // only valid options are valid DC names.
        return Datacenters.getValidDatacenters();
    }

    protected void validateExpectedOptions() throws ConfigurationException
    {
        // Do not accept query with no data centers specified.
        if (this.configOptions.isEmpty())
        {
            throw new ConfigurationException("Configuration for at least one datacenter must be present");
        }

        // Validate the data center names
        super.validateExpectedOptions();
    }

    public void validateOptions() throws ConfigurationException
    {
        for (Entry<String, String> e : this.configOptions.entrySet())
        {
            if (e.getKey().equalsIgnoreCase("replication_factor"))
                throw new ConfigurationException("replication_factor is an option for SimpleStrategy, not NetworkTopologyStrategy");
            validateReplicationFactor(e.getValue());
        }
    }

    @Override
    public boolean hasSameSettings(AbstractReplicationStrategy other)
    {
        return super.hasSameSettings(other) && ((NetworkTopologyStrategy) other).datacenters.equals(datacenters);
    }
}
