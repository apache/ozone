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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.stream.ChunkedFile;
import io.netty.util.ByteProcessor;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Protocol definition of the streaming.
 */
public class DirstreamServerHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOG =
      LoggerFactory.getLogger(DirstreamServerHandler.class);

  public static final String END_MARKER = "0 END";

  private StreamingSource source;

  private boolean headerProcessed = false;

  public DirstreamServerHandler(StreamingSource source) {
    this.source = source;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg)
      throws Exception {
    StringBuilder id = new StringBuilder();
    if (!headerProcessed) {
      ByteBuf buffer = (ByteBuf) msg;
      int eolPosition = buffer.forEachByte(ByteProcessor.FIND_LF) - buffer
          .readerIndex();
      if (eolPosition > 0) {
        headerProcessed = true;
        id.append(buffer.toString(Charset.defaultCharset()));
      } else {
        id.append(buffer.toString(0, eolPosition, Charset.defaultCharset()));
      }
      buffer.release();
    }

    if (headerProcessed) {
      final List<Entry<String, Path>> entriesToWrite = new ArrayList<>(
          source.getFilesToStream(id.toString().trim()).entrySet());

      writeOneElement(ctx, entriesToWrite, 0);

    }
  }

  public void writeOneElement(
      ChannelHandlerContext ctx,
      List<Entry<String, Path>> entriesToWrite,
      int i
  )
      throws IOException {
    final Entry<String, Path> entryToWrite = entriesToWrite.get(i);
    Path file = entryToWrite.getValue();
    String name = entryToWrite.getKey();
    long fileSize = Files.size(file);
    String identifier = fileSize + " " + name + "\n";
    ByteBuf identifierBuf =
        Unpooled.wrappedBuffer(identifier.getBytes(
            StandardCharsets.UTF_8));

    final int currentIndex = i;

    ChannelFuture lastFuture = ctx.writeAndFlush(identifierBuf);
    lastFuture.addListener(f -> {
      ChannelFuture nextFuture = ctx.writeAndFlush(
          new ChunkedFile(file.toFile()));
      if (currentIndex == entriesToWrite.size() - 1) {
        nextFuture.addListener(a -> {
          if (!a.isSuccess()) {
            LOG.error("Error on streaming file", a.cause());
          }
          ctx.writeAndFlush(
              Unpooled.wrappedBuffer(
                  END_MARKER.getBytes(StandardCharsets.UTF_8)))
              .addListener(b -> {
                ctx.channel().close();
              });
        });
      } else {
        nextFuture.addListener(
            a -> writeOneElement(ctx, entriesToWrite,
                currentIndex + 1));
      }
    });

  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    cause.printStackTrace();
    if (ctx.channel().isActive()) {
      ctx.writeAndFlush("ERR: " +
          cause.getClass().getSimpleName() + ": " +
          cause.getMessage() + '\n').addListener(
          ChannelFutureListener.CLOSE);
    }
    ctx.close();
  }
}
