package org.apache.hadoop.ozone.container.stream;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.DefaultFileRegion;
import io.netty.util.ByteProcessor;

public class DirstreamServerHandler extends ChannelInboundHandlerAdapter {

  private final StringBuilder id = new StringBuilder();
  private StreamingSource source;
  private int writtenIndex = 0;
  private boolean headerProcessed = false;

  public DirstreamServerHandler(StreamingSource source) {
    this.source = source;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg)
          throws Exception {
    if (!headerProcessed) {
      ByteBuf buffer = (ByteBuf) msg;
      int eolPosition = buffer.forEachByte(ByteProcessor.FIND_LF) - buffer
              .readerIndex();
      if (eolPosition > 0) {
        headerProcessed = true;
        id.append(buffer.toString(Charset.defaultCharset()));
      } else {
        id.append(buffer.toString(0,eolPosition, Charset.defaultCharset()));
      }
      buffer.release();
    }

    if (headerProcessed) {
      ChannelFuture lastFuture = null;
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
              new DefaultFileRegion(file.toFile(), 0, fileSize));
      if (currentIndex == entriesToWrite.size() - 1) {
        nextFuture.addListener(a -> ctx.channel().close());
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
