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

package org.apache.hadoop.fs.ozone;

import static java.util.Objects.requireNonNull;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import org.apache.hadoop.fs.ByteBufferPositionedReadable;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.VectoredReadUtils;
import org.apache.hadoop.util.StringUtils;

final class CapableOzoneFSInputStream extends OzoneFSInputStream
    implements StreamCapabilities {

  // ponytail: four shared workers cap active reads; tune this limit after measuring real workloads.
  private static final int VECTORED_READ_THREADS = 4;
  private static final ThreadPoolExecutor VECTORED_READ_EXECUTOR = new ThreadPoolExecutor(
      VECTORED_READ_THREADS, VECTORED_READ_THREADS, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(64),
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ozone-vectored-read-%d").build());

  static {
    VECTORED_READ_EXECUTOR.allowCoreThreadTimeOut(true);
  }

  private volatile boolean closed;

  CapableOzoneFSInputStream(InputStream inputStream, Statistics statistics) {
    super(inputStream, statistics);
  }

  @Override
  public boolean hasCapability(String capability) {
    switch (StringUtils.toLowerCase(capability)) {
    case StreamCapabilities.READBYTEBUFFER:
    case StreamCapabilities.UNBUFFER:
      return true;
    case StreamCapabilities.PREADBYTEBUFFER:
    case StreamCapabilities.VECTOREDIO:
      return getWrappedInputStream() instanceof ByteBufferPositionedReadable;
    default:
      return false;
    }
  }

  @Override
  public void close() throws IOException {
    closed = true;
    super.close();
  }

  @Override
  public void readVectored(List<? extends FileRange> ranges, IntFunction<ByteBuffer> allocate) throws IOException {
    readVectored(ranges, allocate, buffer -> { });
  }

  @Override
  public void readVectored(List<? extends FileRange> ranges, IntFunction<ByteBuffer> allocate,
      Consumer<ByteBuffer> release) throws IOException {
    requireNonNull(allocate, "allocate");
    requireNonNull(release, "release");
    if (closed) {
      throw new IOException("Stream is closed");
    }
    if (!hasCapability(StreamCapabilities.VECTOREDIO)) {
      throw new UnsupportedOperationException("Vectored reads require positioned-read support");
    }
    List<? extends FileRange> sorted = Arrays.asList(VectoredReadUtils.sortRanges(ranges));
    VectoredReadUtils.validateVectoredReadRanges(sorted);
    if (!VectoredReadUtils.isOrderedDisjoint(sorted, 1, 0)) {
      throw new IllegalArgumentException("Overlapping ranges");
    }
    List<CompletableFuture<ByteBuffer>> results = new ArrayList<>(sorted.size());
    for (FileRange range : sorted) {
      CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
      range.setData(result);
      results.add(result);
    }
    AtomicInteger next = new AtomicInteger();
    Runnable worker = () -> {
      int index;
      while ((index = next.getAndIncrement()) < sorted.size()) {
        readRange(sorted.get(index), results.get(index), allocate, release);
      }
    };
    // Any accepted worker can drain the whole batch, allocating one buffer at a time.
    for (int i = 0; i < Math.min(VECTORED_READ_THREADS, sorted.size()); i++) {
      try {
        VECTORED_READ_EXECUTOR.execute(worker);
      } catch (RejectedExecutionException e) {
        if (i == 0) {
          results.forEach(result -> result.completeExceptionally(e));
        }
        break;
      }
    }
  }

  private void readRange(FileRange range, CompletableFuture<ByteBuffer> result,
      IntFunction<ByteBuffer> allocate, Consumer<ByteBuffer> release) {
    if (result.isDone()) {
      return;
    }
    ByteBuffer buffer = null;
    Throwable failure = null;
    try {
      if (closed) {
        throw new IOException("Stream is closed");
      }
      buffer = allocate.apply(range.getLength());
      readFully(range.getOffset(), buffer);
      buffer.flip();
    } catch (IOException | RuntimeException e) {
      failure = e;
    }
    if (buffer != null && (failure != null || !result.complete(buffer))) {
      try {
        release.accept(buffer);
      } catch (RuntimeException e) {
        // A faulty releaser must not strand the remaining range futures.
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    if (failure != null) {
      result.completeExceptionally(failure);
    }
  }
}
