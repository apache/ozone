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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ByteProcessor;
import io.netty.util.ReferenceCountUtil;

/**
 * Protocol definition from streaming binary files.
 *
 * Format of the protocol (TCP/IP):
 *
 * LOGICAL_NAME SIZE
 * ... (binary content)
 * LOGICAL_NAME SIZE
 * ... (binary content)
 * END 0
 */
public class DirstreamClientHandler extends ChannelInboundHandlerAdapter {

  private final StreamingDestination destination;
  private boolean headerMode = true;
  private StringBuilder currentFileName = new StringBuilder();
  private RandomAccessFile destFile;

  private FileChannel destFileChannel;

  private long remaining;

  public DirstreamClientHandler(StreamingDestination streamingDestination) {
    this.destination = streamingDestination;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg)
      throws IOException {
    try {
      ByteBuf buffer = (ByteBuf) msg;
      doRead(ctx, buffer);
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }

  public void doRead(ChannelHandlerContext ctx, ByteBuf buffer)
      throws IOException {
    if (headerMode) {
      int eolPosition = buffer.forEachByte(ByteProcessor.FIND_LF) - buffer
          .readerIndex();
      if (eolPosition > 0) {
        headerMode = false;
        final ByteBuf name = buffer.readBytes(eolPosition);
        currentFileName.append(name
            .toString(StandardCharsets.UTF_8));
        name.release();
        buffer.skipBytes(1);
        String[] parts = currentFileName.toString().split(" ", 2);
        remaining = Long.parseLong(parts[0]);
        Path destFilePath = destination.mapToDestination(parts[1]);
        final Path destfileParent = destFilePath.getParent();
        if (destfileParent == null) {
          throw new IllegalArgumentException("Streaming destination " +
              "provider return with invalid path: " + destFilePath);
        }
        Files.createDirectories(destfileParent);
        this.destFile =
            new RandomAccessFile(destFilePath.toFile(), "rw");
        destFileChannel = this.destFile.getChannel();

      } else {
        currentFileName
            .append(buffer.toString(StandardCharsets.UTF_8));
      }
    }
    if (!headerMode) {
      final int readableBytes = buffer.readableBytes();
      if (remaining >= readableBytes) {
        remaining -=
            buffer.readBytes(destFileChannel, readableBytes);
      } else {
        remaining -= buffer.readBytes(destFileChannel, (int) remaining);
        currentFileName = new StringBuilder();
        headerMode = true;
        destFile.close();
        if (readableBytes > 0) {
          doRead(ctx, buffer);
        }
      }
    }
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    try {
      if (destFile != null) {
        destFile.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
    try {
      destFileChannel.close();
      destFile.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    ctx.close();
  }

  public String getCurrentFileName() {
    return currentFileName.toString();
  }
}
