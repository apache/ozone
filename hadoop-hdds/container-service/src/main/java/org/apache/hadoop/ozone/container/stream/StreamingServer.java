/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.stream;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.stream.ChunkedWriteHandler;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty based streaming server to replicate files from a directory.
 */
public class StreamingServer implements AutoCloseable {

  private static final Logger LOG =
      LoggerFactory.getLogger(StreamingServer.class);

  private int port;

  private StreamingSource source;

  private EventLoopGroup bossGroup;

  private EventLoopGroup workerGroup;

  private SslContext sslContext;

  public StreamingServer(
      StreamingSource source, int port
  ) {
    this(source, port, null);
  }

  public StreamingServer(
      StreamingSource source, int port, SslContext sslContext
  ) {
    this.port = port;
    this.source = source;
    this.sslContext = sslContext;
  }

  public void start() {
    try {
      ServerBootstrap b = new ServerBootstrap();
      bossGroup = new NioEventLoopGroup(100);
      workerGroup = new NioEventLoopGroup(100);

      b.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .option(ChannelOption.SO_BACKLOG, 100)

          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
              if (sslContext != null) {
                ch.pipeline().addLast(sslContext.newHandler(ch.alloc()));
              }
              ch.pipeline().addLast(
                  new ChunkedWriteHandler(),
                  new DirstreamServerHandler(source));


            }
          });

      ChannelFuture f = b.bind(port).sync();
      final InetSocketAddress socketAddress =
          (InetSocketAddress) f.channel().localAddress();
      port = socketAddress.getPort();
      LOG.info("Started streaming server on {}", port);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new StreamingException(ex);
    }
  }

  public void stop() {
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
  }

  public int getPort() {
    return port;
  }

  @Override
  public void close() {
    stop();
  }
}
