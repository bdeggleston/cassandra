package org.apache.cassandra.service.epaxos;

import com.google.common.base.Function;
import com.google.common.collect.LinkedListMultimap;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.epaxos.integration.AbstractEpaxosIntegrationTest;
import org.apache.cassandra.service.epaxos.integration.Messenger;
import org.apache.cassandra.service.epaxos.integration.Node;

import org.apache.cassandra.service.epaxos.integration.QueuedExecutor;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simulates a cluster running an epaxos workload.
 *
 * This runs in a single thread, and will randomly bring nodes up and down, using a configurable seed
 * to help identify and repeat edge failure cases. At the end of the test, the order that epaxos
 * instances were executed in are verified to be identical.
 */
public class EpaxosFuzzer
{
    private static final Logger logger = LoggerFactory.getLogger(EpaxosState.class);

    static
    {
        DatabaseDescriptor.getConcurrentWriters();
        DatabaseDescriptor.setPartitioner(new ByteOrderedPartitioner());
        MessagingService.instance();
        SchemaLoader.prepareServer();
        SystemKeyspace.finishStartup();
        try
        {
            AbstractEpaxosTest.setUpClass();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static final QueuedExecutor executor = (QueuedExecutor) Node.queuedExecutor;

    public static enum Workload
    {
        THROUGHPUT (new Function<Integer, SerializedRequest>()
        {
            @Override
            public SerializedRequest apply(Integer i)
            {
                return AbstractEpaxosTest.getSerializedCQLRequest(i, i);
            }
        }),

        CONTENTION (new Function<Integer, SerializedRequest>()
        {
            @Override
            public SerializedRequest apply(Integer i)
            {
                return AbstractEpaxosTest.getSerializedCQLRequest(0, i);
            }
        });

        private final Function<Integer, SerializedRequest> f;

        Workload(Function<Integer, SerializedRequest> f)
        {
            this.f = f;
        }

        public SerializedRequest getRequest(int i)
        {
            return f.apply(i);
        }
    }

    public static Random random = new Random();
    public static int MAX_QUERY_TASKS = -1;

    public static List<FuzzNode> createNodes(final String ksName, FuzzerOptions options, Messenger messenger)
    {
        List<FuzzNode> nodes = new ArrayList<>(options.nodes);
        for (int i=0; i<options.nodes; i++)
        {
            final int nodeNumber = i + 1;
            FuzzNode node = new FuzzNode(nodeNumber, messenger){

                @Override
                protected String keyspace()
                {
                    return ksName;
                }

                @Override
                protected String instanceTable()
                {
                    return String.format("%s_%s", SystemKeyspace.EPAXOS_INSTANCE, nodeNumber);
                }

                @Override
                protected String keyStateTable()
                {
                    return String.format("%s_%s", SystemKeyspace.EPAXOS_KEY_STATE, nodeNumber);
                }

                @Override
                protected String tokenStateTable()
                {
                    return String.format("%s_%s", SystemKeyspace.EPAXOS_TOKEN_STATE, nodeNumber);
                }
            };

            nodes.add(node);
            messenger.registerNode(node);
        }
        return nodes;
    }

    /**
     * number of tasks the queued executor can process
     * before we consider a query to have timed out
     */
    private static void setNumTasksPerQuery(FuzzerOptions options)
    {
        int size = 0;
        size++; // preaccept task
        size += options.nodes * 2; // preaccept request and response for each node
        size += options.nodes * 2; // accept request and responses
        size += options.nodes * 3; // commit request and responses and execution
        MAX_QUERY_TASKS = size * options.concurrency;
    }

    public static void main(String[] args) throws Exception
    {
        FuzzerOptions options = FuzzerOptions.parseArgs(args);

        assert MAX_QUERY_TASKS > 0;

        long seed = options.seed > 0 ? options.seed : System.currentTimeMillis();
        logger.info("Using seed value {}", seed);
        random.setSeed(seed);

        Messenger messenger = new Messenger(executor);
        int replicationFactor = options.nodes; // TODO: make this independent of node#
        String ksName = AbstractEpaxosIntegrationTest.createTestKeyspace(replicationFactor);
        List<FuzzNode> nodes = createNodes(ksName, options, messenger);

        // create the workload to be processed
        Queue<SerializedRequest> work = new LinkedBlockingQueue<>();
        for (int i=0; i<options.queries; i++)
        {
            work.add(options.workload.getRequest(i));
        }

        // create clients
        final List<Client> clients = new ArrayList<>(options.concurrency);
        for (int i=0; i<options.concurrency; i++)
        {
            Client client = new Client(work, options, nodes);
            clients.add(client);
            executor.addPostRunCallback(client);
        }

        Troublemaker troublemaker = new Troublemaker(nodes, options);
        executor.addPostRunCallback(troublemaker);

        if (options.suspend)
        {
            System.out.println("press any key to begin...");
            System.in.read();
        }

        MDC.put("seed", Long.toString(seed));
        executor.addPostRunCallback(new MDCUpdater(nodes));

        try
        {
            while (!work.isEmpty())
            {
                executor.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        for (Client client: clients)
                        {
                            client.run();
                        }
                    }
                });

                if (!work.isEmpty())
                {
                    logger.warn("executor finished with {} of {} left in the work queue", work.size(), options.queries);
                    for (Client client: clients)
                    {
                        client.forceTimeout();
                    }
                    troublemaker.recoverNode();
//                    System.exit(1);
                }
            }
        }
        catch (Throwable e)
        {
            logger.error("death", e);
        }

        checkConsistency(nodes);
        if (options.failures)
        {
            logger.info("{} failures", troublemaker.getFailures());
        }
        logger.info("Using seed value {}", seed);
        logger.info("done");
        System.exit(0);
    }

    // checks that all instances were executed
    // in the same order on every node
    public static void checkConsistency(List<FuzzNode> nodes)
    {
        logger.info("checking execution consistency...");
        Set<ByteBuffer> keys = new HashSet<>();
        for (FuzzNode node: nodes)
        {
            keys.addAll(node.executed.keySet());
        }

        keyLoop: for (ByteBuffer key: keys)
        {
            List<List<UUID>> executed = new ArrayList<>(nodes.size());

            int maxSize = 0;
            for (FuzzNode node: nodes)
            {
                List<UUID> idList = node.executed.get(key);
                idList = idList != null ? idList : Collections.<UUID>emptyList();
                executed.add(idList);
                maxSize = Math.max(maxSize, idList.size());
            }

            int maxNode = 0;
            for (int i=0; i<nodes.size(); i++)
            {
                if (executed.get(i).size() == maxSize)
                {
                    maxNode = i;
                    break;
                }
            }

            List<UUID> reference = nodes.get(maxNode).executed.get(key);
            for (int execNum=0; execNum<maxSize; execNum++)
            {
                for (int n=0; n<nodes.size(); n++)
                {
                    if (n == maxNode)
                        continue;

                    List<UUID> ids = nodes.get(n).executed.get(key);
                    if (!ids.get(execNum).equals(reference.get(execNum)))
                    {
                        reportMismatch(nodes, key, execNum);
                        continue keyLoop;
                    }
                }

            }
        }
        logger.info("looks good");
    }


    public static void reportMismatch(List<FuzzNode> nodes, ByteBuffer key, int location)
    {
        String NOID = "------------------------------------";
        logger.error("Execution mismatch at {} ({}) for execution {}",
                     key, ByteBufferUtil.toInt(key), location);

        for (int i=(location-2); i<=(location+2); i++)
        {
            if (i < 0)
                continue;

            String message = Integer.toString(i) + ": ";
            for (FuzzNode node: nodes)
            {
                List<UUID> ids = node.executed.get(key);
                String id = (ids == null || ids.size() <= i) ? NOID : ids.get(i).toString();
                message = message + id + " ";
            }

            if (i == location)
            {
                message = message + "<<";
            }
            logger.error(message);
        }
    }

    public static class FuzzerOptions
    {
        public int nodes = 3;
        public boolean failures = false;
        public long seed = 0;
        public Workload workload = Workload.THROUGHPUT;
        public int concurrency = 1;
        public int queries = 5000;
        public boolean suspend = false;

        private static final String NODES_OPTION = "nodes";
        private static final String FAILURES_OPTION = "failures";
        private static final String SEED_OPTION = "seed";
        private static final String WORKLOAD_OPTION = "workload";
        private static final String CONCURRENCY_OPTION = "concurrency";
        private static final String QUERIES_OPTION = "queries";
        private static final String SUSPEND_OPTION = "suspend";

        public static FuzzerOptions parseArgs(String[] args) throws ParseException
        {
            CommandLine cl = new GnuParser().parse(getCmdOptions(), args);
            FuzzerOptions options = new FuzzerOptions();
            options.nodes = Integer.parseInt(cl.getOptionValue(NODES_OPTION, "3"));
            options.failures = cl.hasOption(FAILURES_OPTION);
            options.seed = Long.parseLong(cl.getOptionValue(SEED_OPTION, "0"));
            options.workload = Workload.valueOf(cl.getOptionValue(WORKLOAD_OPTION, Workload.CONTENTION.toString()).toUpperCase());
            options.concurrency = Integer.parseInt(cl.getOptionValue(CONCURRENCY_OPTION, "3"));
            options.queries = Integer.parseInt(cl.getOptionValue(QUERIES_OPTION, "10000"));
            options.suspend = cl.hasOption(SUSPEND_OPTION);

            setNumTasksPerQuery(options);
            return options;
        }

        private static Options getCmdOptions()
        {
            Options options = new Options();
            options.addOption("n", NODES_OPTION, true, "number of nodes to simulate");
            options.addOption("s", SEED_OPTION, true, "value to seed random with");
            options.addOption("f", FAILURES_OPTION, false, "set if failures should be simulated");
            options.addOption("w", WORKLOAD_OPTION, true, "the type of workload to run");
            options.addOption("c", CONCURRENCY_OPTION, true, "simulated concurrency");
            options.addOption("q", QUERIES_OPTION, true, "num queries to run");
            options.addOption("d", SUSPEND_OPTION, false, "wait for user input to begin");
            return options;
        }
    }

    public static class FuzzNode extends Node.SingleThreaded
    {
        public LinkedListMultimap<ByteBuffer, UUID> executed = LinkedListMultimap.create();

        private final Map<UUID, Client> waitingClients = new HashMap<>();

        public FuzzNode(int number, Messenger messenger)
        {
            super(number, messenger);
        }

        @Override
        public <T> T query(SerializedRequest query) throws UnavailableException, WriteTimeoutException, ReadTimeoutException, InvalidRequestException
        {
            throw new AssertionError();
        }

        public UUID clientQuery(Client client, SerializedRequest request) throws UnavailableException
        {
            QueryInstance queryInstance = createQueryInstance(request);
            getParticipants(queryInstance).quorumExistsOrDie();
            waitingClients.put(queryInstance.getId(), client);
            preaccept(queryInstance);
            return queryInstance.getId();
        }

        public void cancelCallback(UUID id)
        {
            waitingClients.remove(id);
        }

        AtomicLong replayCounter = new AtomicLong(0);

        @Override
        protected Pair<ReplayPosition, Long> executeQueryInstance(QueryInstance instance) throws ReadTimeoutException, WriteTimeoutException
        {
            UUID id = instance.getId();

            SerializedRequest query = instance.getQuery();
            executed.put(query.getKey(), id);

            if (waitingClients.containsKey(id))
            {
                waitingClients.get(id).instanceExecuted(id);
                waitingClients.remove(id);
            }

            long next = replayCounter.getAndIncrement();
            Pair<ReplayPosition, Long> rp = Pair.create(new ReplayPosition(next / 100, (int) next % 100), next);
            return rp;
        }
    }

    public static class Client implements Runnable
    {
        private final Queue<SerializedRequest> work;
        private final FuzzerOptions options;
        private final List<FuzzNode> nodes;

        private volatile int startIdx = 0;
        private volatile UUID pendingQuery = null;
        private volatile FuzzNode pendingNode = null;
        private static volatile int executed = 0;

        public Client(Queue<SerializedRequest> work, FuzzerOptions options, List<FuzzNode> nodes)
        {
            this.work = work;
            this.options = options;
            this.nodes = nodes;
        }

        @Override
        public void run()
        {
            if (isTimedOut())
            {
                timeoutCleanup();
            }

            if (pendingQuery == null)
            {
                startQuery();
            }
        }

        public boolean startQuery()
        {
            assert pendingQuery == null;
            try
            {
                SerializedRequest query = work.remove();
                pendingNode = nodes.get(Math.abs(random.nextInt() % nodes.size()));
                pendingQuery = pendingNode.clientQuery(this, query);
                startIdx = executor.getExecuted() + executor.queueSize();
                return true;
            }
            catch (NoSuchElementException e)
            {
                return false;
            }
            catch (UnavailableException e)
            {
                return false;
            }
        }

        private void timeoutCleanup()
        {
            assert pendingQuery != null;
            logger.warn("Query {} timed out", pendingQuery);
            pendingNode.cancelCallback(pendingQuery);
            pendingNode = null;
            pendingQuery = null;
        }

        private boolean quorumExists()
        {
            int quorumSize = (nodes.size() / 2) + 1;
            int activeNodes = 0;
            for (Node node: nodes)
            {
                if (node.getState() == Node.State.UP)
                {
                    activeNodes++;
                }
            }
            return activeNodes >= quorumSize;
        }

        public boolean isTimedOut()
        {
            int currentIdx = executor.getExecuted();
            if (pendingNode != null && pendingNode.getState() == Node.State.DOWN)
            {
                // bail out if the node running our query is down
                return true;
            }
            if (pendingQuery != null)
            {
                if (currentIdx > startIdx + MAX_QUERY_TASKS && !quorumExists())
                {
                    return true;
                }
            }

            startIdx = currentIdx;
            return false;
        }

        public void forceTimeout()
        {
            timeoutCleanup();
        }

        public void instanceExecuted(UUID id)
        {
            if (pendingQuery != null && id.equals(pendingQuery))
            {
                pendingQuery = null;
                executed++;
                if (executed % 100 == 0)
                {
                    System.out.println(executed + " queries completed");
                }
                logger.info("{} executed: {}", executed, id);
            }
        }
    }

    public static class Troublemaker implements Runnable
    {
        private final List<FuzzNode> nodes;
        private final FuzzerOptions options;

        private final int REPAIR_CHANCE = 1500;
        private final int BREAKAGE_CHANCE = 15000;

        private final List<Node> problems;
        private volatile int failures = 0;

        Troublemaker(List<FuzzNode> nodes, FuzzerOptions options)
        {
            this.nodes = nodes;
            this.options = options;
            problems = new ArrayList<>(nodes.size());
        }

        @Override
        public void run()
        {
            if (!options.failures)
                return;

            if (random.nextInt() % REPAIR_CHANCE == 0)
            {
                recoverNode();
            }

            int breakageChange = (BREAKAGE_CHANCE + (BREAKAGE_CHANCE * problems.size()));
            if (problems.size() < nodes.size() && random.nextInt() % breakageChange == 0)
            {
                for (int i=0; i<nodes.size(); i++)
                {
                    Node node = nodes.get(Math.abs(random.nextInt() % nodes.size()));
                    if (node.getState() == Node.State.UP)
                    {
                        Node.State newState = random.nextBoolean() ? Node.State.DOWN : Node.State.NORESPONSE;
                        node.setState(newState);
                        problems.add(node);
                        logger.info("Node {} is {} ({}/{})", node.number, node.getState(), problems.size(), nodes.size());
                        failures++;
                        break;
                    }
                }
            }
        }

        public void recoverNode()
        {
            if (!problems.isEmpty())
            {
                Node problemNode = problems.remove(Math.abs(random.nextInt() % problems.size()));
                problemNode.setState(Node.State.UP);
                logger.info("Node {} is {} ({}/{})", problemNode.number, problemNode.getState(), problems.size(), nodes.size());
            }
        }

        public int getFailures()
        {
            return failures;
        }
    }

    public static class MDCUpdater implements Runnable
    {

        private final List<FuzzNode> nodes;

        public MDCUpdater(List<FuzzNode> nodes)
        {
            this.nodes = nodes;
        }

        @Override
        public void run()
        {
            MDC.put("executed", Integer.toString(executor.getExecuted()));
            MDC.put("queueSize", Integer.toString(executor.queueSize()));

            int down = 0;
            for (Node node: nodes)
            {
                if (node.getState() != Node.State.UP)
                {
                    down++;
                }
            }
            MDC.put("nodesDown", String.format("%s/%s", down, nodes.size()));
        }
    }
}
