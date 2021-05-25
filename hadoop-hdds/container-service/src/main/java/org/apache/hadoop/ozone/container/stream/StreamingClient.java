/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.stream;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.container.stream.DirstreamServerHandler.END_MARKER;

/**
 * Client to stream huge binaries from a streamling server.
 */
public class StreamingClient implements AutoCloseable {

  private final Bootstrap bootstrap;
  private final DirstreamClientHandler dirstreamClientHandler;
  private EventLoopGroup group;
  private int port;
  private String host;

  public StreamingClient(
      String host,
      int port,
      StreamingDestination streamingDestination
  ) throws InterruptedException {
    this.port = port;
    this.host = host;

    group = new NioEventLoopGroup(100);
    dirstreamClientHandler = new DirstreamClientHandler(streamingDestination);
    bootstrap = new Bootstrap();
    bootstrap.group(group)
        .channel(NioSocketChannel.class)
        .option(ChannelOption.SO_RCVBUF, 1024 * 1024)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();
            p.addLast(new StringEncoder(CharsetUtil.UTF_8),
                dirstreamClientHandler
            );
          }
        });

  }


  public void stream(String id) {
    stream(id, 200L, TimeUnit.SECONDS);
  }

  public void stream(String id, long timeout, TimeUnit unit) {
    try {
      Channel channel = bootstrap.connect(host, port).sync().channel();
      channel.writeAndFlush(id + "\n")
          .await(timeout, unit);
      channel.closeFuture().await(timeout, unit);
      if (!dirstreamClientHandler.getCurrentFileName().equals(END_MARKER)) {
        throw new RuntimeException("Streaming is failed. Not all files " +
            "are streamed. Please check the log of the server." +
            " Last (partial?) streamed file: "
            + dirstreamClientHandler.getCurrentFileName());
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }


  @Override
  public void close() {
    group.shutdownGracefully();
  }
}
