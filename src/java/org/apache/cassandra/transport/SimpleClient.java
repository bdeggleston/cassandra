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
package org.apache.cassandra.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.cassandra.auth.Auth;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.tracing.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.transport.messages.CredentialsMessage;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.apache.cassandra.utils.MD5Digest;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import static org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions;

public class SimpleClient
{
    static
    {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    private static final Logger logger = LoggerFactory.getLogger(SimpleClient.class);
    public final String host;
    public final int port;
    private final ClientEncryptionOptions encryptionOptions;

    protected final ResponseHandler responseHandler = new ResponseHandler();
    protected final Connection.Tracker tracker = new ConnectionTracker();
    // We don't track connection really, so we don't need one Connection per channel
    protected final Connection connection = new Connection(null, Server.CURRENT_VERSION, tracker);
    protected Bootstrap bootstrap;
    protected Channel channel;
    protected ChannelFuture lastWriteFuture;

    private final Connection.Factory connectionFactory = new Connection.Factory()
    {
        public Connection newConnection(Channel channel, int version)
        {
            assert version == Server.CURRENT_VERSION;
            return connection;
        }
    };

    protected final Tracing tracing;
    protected final Auth auth;
    protected final QueryHandler queryHandler;

    public SimpleClient(String host, int port, ClientEncryptionOptions encryptionOptions, Map<Message.Type, Message.Codec> codecs, Tracing tracing, Auth auth, QueryHandler queryHandler)
    {
        this.host = host;
        this.port = port;
        this.encryptionOptions = encryptionOptions;
        this.tracing = tracing;
        this.auth = auth;
        this.queryHandler = queryHandler;
        messageDecoder = new Message.ProtocolDecoder(codecs);
        messageEncoder = new Message.ProtocolEncoder(codecs);
    }

    public SimpleClient(String host, int port, Map<Message.Type, Message.Codec> codecs, Tracing tracing, Auth auth, QueryHandler queryHandler)
    {
        this(host, port, new ClientEncryptionOptions(), codecs, tracing, auth, queryHandler);
    }

    public void connect(boolean useCompression) throws IOException
    {
        establishConnection();

        Map<String, String> options = new HashMap<String, String>();
        options.put(StartupMessage.CQL_VERSION, "3.0.0");
        if (useCompression)
        {
            options.put(StartupMessage.COMPRESSION, "snappy");
            connection.setCompressor(FrameCompressor.SnappyCompressor.instance);
        }
        execute(new StartupMessage(options, auth.getAuthenticator()));
    }

    protected void establishConnection() throws IOException
    {
        // Configure the client.
        bootstrap = new Bootstrap()
                    .group(new NioEventLoopGroup())
                    .channel(io.netty.channel.socket.nio.NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true);

        // Configure the pipeline factory.
        if(encryptionOptions.enabled)
        {
            bootstrap.handler(new SecureInitializer());
        }
        else
        {
            bootstrap.handler(new Initializer());
        }
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));

        // Wait until the connection attempt succeeds or fails.
        channel = future.awaitUninterruptibly().channel();
        if (!future.isSuccess())
        {
            bootstrap.group().shutdownGracefully();
            throw new IOException("Connection Error", future.cause());
        }
    }

    public void login(Map<String, String> credentials)
    {
        CredentialsMessage msg = new CredentialsMessage(auth.getAuthenticator());
        msg.credentials.putAll(credentials);
        execute(msg);
    }

    public ResultMessage execute(String query, ConsistencyLevel consistency)
    {
        return execute(query, Collections.<ByteBuffer>emptyList(), consistency);
    }

    public ResultMessage execute(String query, List<ByteBuffer> values, ConsistencyLevel consistencyLevel)
    {
        Message.Response msg = execute(new QueryMessage(query, QueryOptions.forInternalCalls(consistencyLevel, values), tracing, queryHandler));
        assert msg instanceof ResultMessage;
        return (ResultMessage)msg;
    }

    public ResultMessage.Prepared prepare(String query)
    {
        Message.Response msg = execute(new PrepareMessage(query, tracing, queryHandler));
        assert msg instanceof ResultMessage.Prepared;
        return (ResultMessage.Prepared)msg;
    }

    public ResultMessage executePrepared(byte[] statementId, List<ByteBuffer> values, ConsistencyLevel consistency)
    {
        Message.Response msg = execute(new ExecuteMessage(MD5Digest.wrap(statementId), QueryOptions.forInternalCalls(consistency, values), tracing, queryHandler));
        assert msg instanceof ResultMessage;
        return (ResultMessage)msg;
    }

    public void close()
    {
        // Wait until all messages are flushed before closing the channel.
        if (lastWriteFuture != null)
            lastWriteFuture.awaitUninterruptibly();

        // Close the connection.  Make sure the close operation ends because
        // all I/O operations are asynchronous in Netty.
        channel.close().awaitUninterruptibly();

        // Shut down all thread pools to exit.
        bootstrap.group().shutdownGracefully();
    }

    protected Message.Response execute(Message.Request request)
    {
        try
        {
            request.attach(connection);
            lastWriteFuture = channel.writeAndFlush(request);
            Message.Response msg = responseHandler.responses.take();
            if (msg instanceof ErrorMessage)
                throw new RuntimeException((Throwable)((ErrorMessage)msg).error);
            return msg;
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    // Stateless handlers
    private final Message.ProtocolDecoder messageDecoder;
    private final Message.ProtocolEncoder messageEncoder;
    private static final Frame.Decompressor frameDecompressor = new Frame.Decompressor();
    private static final Frame.Compressor frameCompressor = new Frame.Compressor();
    private static final Frame.Encoder frameEncoder = new Frame.Encoder();

    private static class ConnectionTracker implements Connection.Tracker
    {
        public void addConnection(Channel ch, Connection connection) {}
        public void closeAll() {}
    }

    private class Initializer extends ChannelInitializer<Channel>
    {
        protected void initChannel(Channel channel) throws Exception
        {
            ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast("frameDecoder", new Frame.Decoder(connectionFactory,
                                                               DatabaseDescriptor.instance.getNativeTransportMaxFrameSize()));
            pipeline.addLast("frameEncoder", frameEncoder);

            pipeline.addLast("frameDecompressor", frameDecompressor);
            pipeline.addLast("frameCompressor", frameCompressor);

            pipeline.addLast("messageDecoder", messageDecoder);
            pipeline.addLast("messageEncoder", messageEncoder);

            pipeline.addLast("handler", responseHandler);
        }
    }

    private class SecureInitializer extends Initializer
    {
        private final SSLContext sslContext;

        public SecureInitializer() throws IOException
        {
            this.sslContext = SSLFactory.createSSLContext(encryptionOptions, true);
        }

        protected void initChannel(Channel channel) throws Exception
        {
            super.initChannel(channel);
            SSLEngine sslEngine = sslContext.createSSLEngine();
            sslEngine.setUseClientMode(true);
            sslEngine.setEnabledCipherSuites(encryptionOptions.cipher_suites);
            channel.pipeline().addFirst("ssl", new SslHandler(sslEngine));
        }
    }

    @ChannelHandler.Sharable
    private static class ResponseHandler extends SimpleChannelInboundHandler<Message.Response>
    {
        public final BlockingQueue<Message.Response> responses = new SynchronousQueue<Message.Response>(true);

        @Override
        public void channelRead0(ChannelHandlerContext ctx, Message.Response r)
        {
            try
            {
                responses.put(r);
            }
            catch (InterruptedException ie)
            {
                throw new RuntimeException(ie);
            }
        }

        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
        {
            if (this == ctx.pipeline().last())
                logger.error("Exception in response", cause);
            ctx.fireExceptionCaught(cause);
        }
    }
}
