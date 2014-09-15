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
package org.apache.cassandra.thrift;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.cassandra.auth.Auth;
import org.apache.cassandra.config.CFMetaDataFactory;
import org.apache.cassandra.config.KSMetaDataFactory;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.db.CounterMutationFactory;
import org.apache.cassandra.db.DBConfig;
import org.apache.cassandra.db.KeyspaceManager;
import org.apache.cassandra.db.MutationFactory;
import org.apache.cassandra.locator.LocatorConfig;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

public class ThriftServer implements CassandraDaemon.Server
{
    private static Logger logger = LoggerFactory.getLogger(ThriftServer.class);
    protected final static String SYNC = "sync";
    protected final static String ASYNC = "async";
    protected final static String HSHA = "hsha";

    protected final InetAddress address;
    protected final int port;
    protected final int backlog;
    private volatile ThriftServerThread server;

    private final DatabaseDescriptor databaseDescriptor;
    private final Tracing tracing;
    private final Schema schema;
    private final Auth auth;
    private final StorageProxy storageProxy;
    private final MessagingService messagingService;
    private final KeyspaceManager keyspaceManager;
    private final MutationFactory mutationFactory;
    private final CounterMutationFactory counterMutationFactory;
    private final StorageService storageService;
    private final CFMetaDataFactory cfMetaDataFactory;
    private final MigrationManager migrationManager;
    private final KSMetaDataFactory ksMetaDataFactory;
    private final QueryHandler queryHandler;
    private final LocatorConfig locatorConfig;
    private final DBConfig dbConfig;
    private final ThriftSessionManager thriftSessionManager;
    private final ClientMetrics clientMetrics;

    public ThriftServer(InetAddress address, int port, int backlog, DatabaseDescriptor databaseDescriptor, Tracing tracing, Schema schema, Auth auth, StorageProxy storageProxy, MessagingService messagingService, KeyspaceManager keyspaceManager, MutationFactory mutationFactory, CounterMutationFactory counterMutationFactory, StorageService storageService, CFMetaDataFactory cfMetaDataFactory, MigrationManager migrationManager, KSMetaDataFactory ksMetaDataFactory, QueryHandler queryHandler, LocatorConfig locatorConfig, DBConfig dbConfig, ThriftSessionManager thriftSessionManager, ClientMetrics clientMetrics)
    {
        this.address = address;
        this.port = port;
        this.backlog = backlog;
        this.databaseDescriptor = databaseDescriptor;
        this.tracing = tracing;
        this.schema = schema;
        this.auth = auth;
        this.storageProxy = storageProxy;
        this.messagingService = messagingService;
        this.keyspaceManager = keyspaceManager;
        this.mutationFactory = mutationFactory;
        this.counterMutationFactory = counterMutationFactory;
        this.storageService = storageService;
        this.cfMetaDataFactory = cfMetaDataFactory;
        this.migrationManager = migrationManager;
        this.ksMetaDataFactory = ksMetaDataFactory;
        this.queryHandler = queryHandler;
        this.locatorConfig = locatorConfig;
        this.dbConfig = dbConfig;
        this.thriftSessionManager = thriftSessionManager;
        this.clientMetrics = clientMetrics;
    }

    public void start()
    {
        if (server == null)
        {
            CassandraServer iface = getCassandraServer();
            server = new ThriftServerThread(address, port, backlog, getProcessor(iface), getTransportFactory(), databaseDescriptor, thriftSessionManager);
            server.start();
        }
    }

    public void stop()
    {
        if (server != null)
        {
            server.stopServer();
            try
            {
                server.join();
            }
            catch (InterruptedException e)
            {
                logger.error("Interrupted while waiting thrift server to stop", e);
            }
            server = null;
        }
    }

    public boolean isRunning()
    {
        return server != null;
    }

    /*
     * These methods are intended to be overridden to provide custom implementations.
     */
    protected CassandraServer getCassandraServer()
    {
        return new CassandraServer(databaseDescriptor, tracing, schema, auth,
                                   storageProxy, messagingService, keyspaceManager,
                                   mutationFactory, counterMutationFactory, storageService,
                                   cfMetaDataFactory, migrationManager, ksMetaDataFactory, queryHandler,
                                   locatorConfig, dbConfig, thriftSessionManager, clientMetrics);
    }

    protected TProcessor getProcessor(CassandraServer server)
    {
        return new Cassandra.Processor<Cassandra.Iface>(server);
    }

    protected TTransportFactory getTransportFactory()
    {
        int tFramedTransportSize = databaseDescriptor.getThriftFramedTransportSize();
        return new TFramedTransport.Factory(tFramedTransportSize);
    }

    /**
     * Simple class to run the thrift connection accepting code in separate
     * thread of control.
     */
    private static class ThriftServerThread extends Thread
    {
        private final TServer serverEngine;

        public ThriftServerThread(InetAddress listenAddr,
                                  int listenPort,
                                  int listenBacklog,
                                  TProcessor processor,
                                  TTransportFactory transportFactory,
                                  DatabaseDescriptor databaseDescriptor,
                                  ThriftSessionManager thriftSessionManager)
        {
            // now we start listening for clients
            logger.info(String.format("Binding thrift service to %s:%s", listenAddr, listenPort));

            TServerFactory.Args args = new TServerFactory.Args();
            args.tProtocolFactory = new TBinaryProtocol.Factory(true, true);
            args.addr = new InetSocketAddress(listenAddr, listenPort);
            args.listenBacklog = listenBacklog;
            args.processor = processor;
            args.keepAlive = databaseDescriptor.getRpcKeepAlive();
            args.sendBufferSize = databaseDescriptor.getRpcSendBufferSize();
            args.recvBufferSize = databaseDescriptor.getRpcRecvBufferSize();
            args.inTransportFactory = transportFactory;
            args.outTransportFactory = transportFactory;
            serverEngine = new TServerCustomFactory(databaseDescriptor.getRpcServerType(), databaseDescriptor, thriftSessionManager).buildTServer(args);
        }

        public void run()
        {
            logger.info("Listening for thrift clients...");
            serverEngine.serve();
        }

        public void stopServer()
        {
            logger.info("Stop listening to thrift clients");
            serverEngine.stop();
        }
    }
}
