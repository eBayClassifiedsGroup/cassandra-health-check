package com.lookout.cassandra;

import ch.qos.logback.classic.Level;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.SocketOptions;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

/**
 * Perform a Cassandra health check. Using only the node that is specified
 * in the arguments as a coordinator, ask that node to perform a CL_ALL read on a
 * known key, downgrade if necessary, but remember that we had to downgrade.
 *
 * If we downgrade, we remember that we downgraded, and our exit status is 1,
 * unless we can't even downgrade, in which case we return with a status of 2.
 */
public class CassandraHealthCheck {
    @Option(name="-host",usage="The cassandra host name for the coordinator")
    public transient String host = "localhost";

    @Option(name="-port",usage="The port to connect to")
    public transient Integer port = 9042;

    @Option(name="-username",usage="Username")
    private transient String username;

    @Option(name="-password",usage="Password")
    private transient String password;

    @Option(name="-debug",usage="Enable debugging")
    private transient boolean debugFlag;

    /**
     * The name of the healthcheck keyspace in cassandra. Note that this keyspace may be dropped
     * automatically if it does not have the appropriate replication factor!
     */
    private static final String HEALTHCHECK_KEYSPACE_NAME = "healthcheck";

    /**
     * A lock to prevent multiple processes from running. We will assume failure when another
     * process is found running
     */
    private final CrossJVMLock lock = new CrossJVMLock();

    /**
     * The retryPolicy will remember whether or not we retried, will log the retry,
     * and will downgrade, so we can see if we can get anything from even the local node
     */
    private final RecollectingRetryPolicy retryPolicy = new RecollectingRetryPolicy(
            new LoggingRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE));

    private static final Logger LOG = LoggerFactory.getLogger(CassandraHealthCheck.class);

    private transient Cluster cluster;
    private transient Session session;
    private transient Set<Host> hosts;

    private final SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSSS", Locale.ENGLISH);

    public CrossJVMLock getLock() {
        return lock;
    }

    @SuppressWarnings("PMD.ConfusingTernary")
    public static void main(final String[] args) throws IOException {
        final CassandraHealthCheck chc = new CassandraHealthCheck();
        final CmdLineParser parser = new CmdLineParser(chc);
        if (chc.debugFlag) {
            ((ch.qos.logback.classic.Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.DEBUG);
        }
        try {
            parser.parseArgument(args);
            chc.connect();
            final int hostCount = chc.hostCount();
            if (!chc.healthCheckKeyspaceExists()) {
                chc.createKeyspace(hostCount);
            } else if (chc.getHealthcheckKeyspaceReplicationFactor() != hostCount) {
                chc.dropKeyspace();
                chc.createKeyspace(hostCount);
            }
            int exitCode = chc.healthCheck();
            chc.shutdown();
            System.exit(exitCode);
        } catch (CmdLineException e) {
            LOG.error(e.getMessage());
            parser.printUsage(System.err);
            System.exit(2);
        }

    }

    /**
     * Connect to a cassandra cluster at a given host/port
     */
    public void connect() {
        try {
            lock.lock();
        } catch (IOException e) {
            throw new IllegalStateException("There appears to be another health check running", e);
        }
        final List<InetSocketAddress> whiteList= new ArrayList<>();
        whiteList.add(new InetSocketAddress(host, port));

        final LoadBalancingPolicy loadBalancingPolicy = new WhiteListPolicy(new RoundRobinPolicy(), whiteList);
        final Cluster.Builder cb = Cluster.builder()
                .addContactPoint(host)
                .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(2000))
                .withSocketOptions(new SocketOptions().setReadTimeoutMillis(12000))
                .withPort(port)
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .withRetryPolicy(retryPolicy);
        if (username != null) {
            cb.withCredentials(username, password);
        }
        cluster = cb.build();
        session = cluster.connect();
        hosts = cluster.getMetadata().getAllHosts();
    }

    public int healthCheck() {
        final Statement health = QueryBuilder.select().all().from(HEALTHCHECK_KEYSPACE_NAME, "healthcheck")
                .where(eq("healthkey", "healthy"));
        health.setConsistencyLevel(ConsistencyLevel.ALL);
        health.enableTracing();
        QueryTrace queryTrace;
        cluster.register(new LoggingLatencyTracker());
        try {
            final ResultSet results = session.execute(health);
            final ExecutionInfo executionInfo = results.getExecutionInfo();
            queryTrace = executionInfo.getQueryTrace();
        } catch (NoHostAvailableException e) {
            LOG.error("No hosts available", e);
            return 2;
        }
        if (retryPolicy.getLastDecision() != null) {
            LOG.warn("Could not query all hosts");
            if (queryTrace != null) {
                final Set<InetAddress> missingHosts = new HashSet<>(hosts.size());
                for (Host host : hosts) {
                    missingHosts.add(host.getSocketAddress().getAddress());
                }
                for (QueryTrace.Event event : queryTrace.getEvents()) {
                    missingHosts.remove(event.getSource());
                    LOG.debug("description={} elapsed={} source={} micros={}",
                            event.getDescription(),
                            millis2Date(event.getTimestamp()),
                            event.getSource(),
                            event.getSourceElapsedMicros());
                }
                if (!missingHosts.isEmpty()) {
                    LOG.error("Missing log entries from these hosts: {}", missingHosts);
                }
            }
            return 1;
        }
        return 0;
    }

    private String millis2Date(long timestamp) {
        return format.format(timestamp);
    }

    public int hostCount() {
        return hosts.size();
    }

    public boolean healthCheckKeyspaceExists() {
        Metadata metaData = cluster.getMetadata();
        KeyspaceMetadata kmd = metaData.getKeyspace(HEALTHCHECK_KEYSPACE_NAME);
        return kmd != null;
    }

    public int getHealthcheckKeyspaceReplicationFactor() {
        Metadata metaData = cluster.getMetadata();
        KeyspaceMetadata kmd = metaData.getKeyspace(HEALTHCHECK_KEYSPACE_NAME);
        Map<String, String> replicationInfo = kmd.getReplication();
        return Integer.parseInt(replicationInfo.get("replication_factor"));
    }

    /**
     * Create a keyspace with the supplied replication factor. The replication
     * factor will be the number of nodes in the cluster, so every node will
     * have a copy of the data.
     *
     * @param rf The desired replication factor.
     */
    public void createKeyspace(int rf) {
        session.execute("CREATE KEYSPACE " + HEALTHCHECK_KEYSPACE_NAME
            + " WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': "
            + rf
            + " }");
        session.execute("CREATE TABLE " + HEALTHCHECK_KEYSPACE_NAME + ".healthcheck ( healthkey varchar primary key )");
        session.execute("INSERT INTO " + HEALTHCHECK_KEYSPACE_NAME + ".healthcheck (healthkey) values ('healthy')");
    }

    public void dropKeyspace() {
        session.execute("DROP KEYSPACE " + HEALTHCHECK_KEYSPACE_NAME);
    }
    /**
     * This file channel is scoped to this object so that the corresponding lock sticks around longer
     */
    public void shutdown() throws IOException {
        session.close();
        cluster.close();
        lock.unlock();
    }

    /**
     * Check to see if we're the only process like us running. This is done by creating a lock file
     * @return
     */
}
