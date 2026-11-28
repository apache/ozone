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

package org.apache.hadoop.ozone.container.common.transport.server;

import static org.apache.ratis.thirdparty.io.netty.handler.codec.http2.Http2CodecUtil.DEFAULT_WINDOW_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.ratis.thirdparty.io.netty.channel.WriteBufferWaterMark;
import org.apache.ratis.thirdparty.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http2.DefaultHttp2Connection;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http2.DefaultHttp2Headers;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http2.DefaultHttp2RemoteFlowController;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http2.Http2Connection;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http2.Http2ConnectionHandler;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http2.Http2ConnectionHandlerBuilder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http2.Http2FrameAdapter;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http2.Http2RemoteFlowController;
import org.apache.ratis.thirdparty.io.netty.handler.codec.http2.UniformStreamByteDistributor;
import org.junit.jupiter.api.Test;

/** Tests for {@link XceiverServerGrpc}'s connection settings. */
class TestXceiverServerGrpc {

  /** A positioned-read reply of an HBase block: a 64 KiB range plus the response's framing and metadata. */
  private static final int PREAD_REPLY_BYTES = (64 << 10) + 1024;
  /** Concurrent positioned reads on one connection. */
  private static final int CONCURRENT_REPLIES = 4;
  /** gRPC's default HTTP/2 flow-control window, which the Ozone client does not change. */
  private static final int FLOW_CONTROL_WINDOW = 1 << 20;

  @Test
  void defaultWaterMarkFlipsWritabilityOnEveryReply() throws Exception {
    assertThat(writabilityChanges(WriteBufferWaterMark.DEFAULT)).isGreaterThanOrEqualTo(2 * CONCURRENT_REPLIES);
  }

  @Test
  void childWaterMarkKeepsConnectionWritable() throws Exception {
    assertThat(writabilityChanges(XceiverServerGrpc.CHILD_WRITE_BUFFER_WATER_MARK)).isZero();
  }

  /**
   * Sends {@link #CONCURRENT_REPLIES} positioned-read replies in one flush on an HTTP/2 connection whose flow
   * controller is set up like gRPC's server sets it up.
   * @return how many times the connection's writability changed while sending them
   */
  private static int writabilityChanges(WriteBufferWaterMark waterMark) throws Exception {
    EmbeddedChannel channel = new EmbeddedChannel();
    try {
      channel.config().setWriteBufferWaterMark(waterMark);
      AtomicInteger changes = new AtomicInteger();
      channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) {
          changes.incrementAndGet();
          ctx.fireChannelWritabilityChanged();
        }
      });
      Http2Connection connection = new DefaultHttp2Connection(false);
      connection.remote().flowController(
          new DefaultHttp2RemoteFlowController(connection, new UniformStreamByteDistributor(connection)));
      Http2ConnectionHandler handler = new Http2ConnectionHandlerBuilder()
          .connection(connection).frameListener(new Http2FrameAdapter()).build();
      channel.pipeline().addLast(handler);
      ChannelHandlerContext ctx = channel.pipeline().context(handler);

      Http2RemoteFlowController flowController = handler.encoder().flowController();
      flowController.initialWindowSize(FLOW_CONTROL_WINDOW);
      flowController.incrementWindowSize(connection.connectionStream(), FLOW_CONTROL_WINDOW - DEFAULT_WINDOW_SIZE);
      channel.flush();

      for (int i = 0; i < CONCURRENT_REPLIES; i++) {
        int streamId = connection.local().incrementAndGetNextStreamId();
        handler.encoder().writeHeaders(ctx, streamId, new DefaultHttp2Headers(), 0, false, ctx.newPromise());
        handler.encoder().writeData(ctx, streamId, Unpooled.wrappedBuffer(new byte[PREAD_REPLY_BYTES]), 0, false,
            ctx.newPromise());
      }
      channel.flush();
      return changes.get();
    } finally {
      channel.finishAndReleaseAll();
    }
  }
}
