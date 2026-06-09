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

package org.apache.hadoop.hdds.scm.storage;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamBufferArgs;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.ozone.test.GenericTestUtils;
import org.slf4j.event.Level;

/**
 * Client-side {@link RatisBlockOutputStream} write benchmark using the
 * {@code BlockOutputStream} and {@code BufferPool} sources currently in the workspace.
 *
 * <p>Mocked container RPCs ({@link BenchmarkMockXceiverClient}) keep datanode I/O out of
 * the measurement. Run twice and compare offline:
 * <ol>
 *   <li>On current workspace sources (after HDDS-15486)</li>
 *   <li>After temporarily checking out pre-HDDS-15486 {@code BlockOutputStream} and
 *       {@code BufferPool} (see {@code dev-support/run-block-output-stream-write-benchmark.sh})</li>
 * </ol>
 *
 * <p>Run from the repo root:
 * <pre>
 *   mvn -pl hadoop-hdds/client -q test-compile exec:java \
 *     -Dexec.mainClass=org.apache.hadoop.hdds.scm.storage.BlockOutputStreamWriteBenchmark \
 *     -Dexec.classpathScope=test
 * </pre>
 *
 * <p>Or use the helper script for both runs with automatic restore:
 * <pre>
 *   hadoop-hdds/client/dev-support/run-block-output-stream-write-benchmark.sh
 * </pre>
 */
public final class BlockOutputStreamWriteBenchmark {

  private static final int WARMUP_SECONDS = 10;
  private static final int BENCHMARK_SECONDS = 20;
  private static final int WRITE_SIZE = 4 * 1024;
  private static final int STREAM_BUFFER_SIZE = 4 * 1024 * 1024;
  /** User read buffer; one stream write cycle fills this buffer at advancing offsets. */
  private static final int SOURCE_BUFFER_SIZE = STREAM_BUFFER_SIZE;
  private static final int POOL_CAPACITY = 8;
  private static final int WRITES_PER_STREAM = SOURCE_BUFFER_SIZE / WRITE_SIZE;

  private static final DecimalFormat MBPS = new DecimalFormat("#,##0.0");
  private static final DecimalFormat NS = new DecimalFormat("#,##0");
  private static final DecimalFormat COUNT = new DecimalFormat("#,##0");

  private BlockOutputStreamWriteBenchmark() {
  }

  public static void main(String[] args) throws Exception {
    GenericTestUtils.setLogLevel(BufferPool.class, Level.INFO);
    GenericTestUtils.setLogLevel(BlockOutputStream.class, Level.INFO);

    final String label = System.getProperty("benchmark.label", "workspace");

    System.out.println("BlockOutputStream client write benchmark (mocked container RPCs)");
    System.out.println("Profile: " + label);
    System.out.println("Log level: INFO for BufferPool and BlockOutputStream");
    System.out.println("JVM: " + System.getProperty("java.version")
        + " on " + System.getProperty("os.arch"));
    System.out.printf("Write=%dKB sourceBuffer=%dMB streamBuffer=%dMB poolCapacity=%d "
            + "writes/stream=%d%n",
        WRITE_SIZE / 1024, SOURCE_BUFFER_SIZE / (1024 * 1024),
        STREAM_BUFFER_SIZE / (1024 * 1024), POOL_CAPACITY,
        WRITES_PER_STREAM);
    System.out.println();

    final byte[] sourceBuffer = new byte[SOURCE_BUFFER_SIZE];
    ThreadLocalRandom.current().nextBytes(sourceBuffer);

    try (BenchmarkSession session = BenchmarkSession.open()) {
      runTimedWrite(session, sourceBuffer, WARMUP_SECONDS, "warmup");
      final Result result = runTimedWrite(session, sourceBuffer, BENCHMARK_SECONDS, "benchmark");

      System.out.printf("elapsed: %.2fs%n", result.elapsedSeconds);
      System.out.printf("bytes written: %s%n", COUNT.format(result.bytesWritten));
      System.out.printf("write ops (%dKB): %s%n", WRITE_SIZE / 1024, COUNT.format(result.writeOps));
      System.out.printf("blocks written: %s%n", COUNT.format(result.blocksWritten));
      System.out.printf("throughput: %s MB/s%n", MBPS.format(result.mbPerSec));
      System.out.printf("ns/write: %s%n", NS.format(result.nsPerWrite));
      System.out.printf("writeChunks metric: %s%n",
          COUNT.format(result.writeChunksDuringWrite));
      System.out.printf("flushes metric: %s%n",
          COUNT.format(result.flushesDuringWrite));
    }
  }

  private static Result runTimedWrite(BenchmarkSession session, byte[] sourceBuffer,
      int seconds, String phase) throws Exception {
    final long start = System.nanoTime();
    final long deadline = start + seconds * 1_000_000_000L;
    long bytesWritten = 0;
    long writeOps = 0;
    long blocksWritten = 0;

    final long writeChunksBefore = session.metrics.getWriteChunksDuringWrite().value();
    final long flushesBefore = session.metrics.getFlushesDuringWrite().value();

    while (System.nanoTime() < deadline) {
      try (BlockOutputStream stream = session.newStream()) {
        for (int offset = 0; offset + WRITE_SIZE <= sourceBuffer.length
            && System.nanoTime() < deadline; offset += WRITE_SIZE) {
          stream.write(sourceBuffer, offset, WRITE_SIZE);
          bytesWritten += WRITE_SIZE;
          writeOps++;
        }
      }
      blocksWritten++;
    }

    final long elapsedNanos = System.nanoTime() - start;
    final double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
    System.out.printf("%s complete (%.2fs)%n", phase, elapsedSeconds);

    return new Result(
        bytesWritten,
        writeOps,
        blocksWritten,
        elapsedSeconds,
        bytesWritten / elapsedSeconds / (1024.0 * 1024.0),
        (double) elapsedNanos / writeOps,
        session.metrics.getWriteChunksDuringWrite().value() - writeChunksBefore,
        session.metrics.getFlushesDuringWrite().value() - flushesBefore);
  }

  private static final class BenchmarkSession implements AutoCloseable {
    private final Pipeline pipeline;
    private final ContainerClientMetrics metrics;
    private final BufferPool bufferPool;
    private final OzoneClientConfig config;
    private final StreamBufferArgs streamBufferArgs;
    private final XceiverClientManager xceiverClientManager;
    private final ExecutorService responseExecutor;

    private BenchmarkSession(Pipeline pipeline, ContainerClientMetrics metrics,
        BufferPool bufferPool, OzoneClientConfig config, StreamBufferArgs streamBufferArgs,
        XceiverClientManager xceiverClientManager, ExecutorService responseExecutor) {
      this.pipeline = pipeline;
      this.metrics = metrics;
      this.bufferPool = bufferPool;
      this.config = config;
      this.streamBufferArgs = streamBufferArgs;
      this.xceiverClientManager = xceiverClientManager;
      this.responseExecutor = responseExecutor;
    }

    static BenchmarkSession open() throws IOException {
      final Pipeline pipeline = MockPipeline.createRatisPipeline();
      final XceiverClientManager xcm = mock(XceiverClientManager.class);
      when(xcm.acquireClient(any())).thenReturn(new BenchmarkMockXceiverClient(pipeline));

      final OzoneClientConfig config = new OzoneClientConfig();
      config.setStreamBufferSize(STREAM_BUFFER_SIZE);
      config.setStreamBufferMaxSize(32 * 1024 * 1024);
      config.setStreamBufferFlushDelay(false);
      config.setStreamBufferFlushSize(16 * 1024 * 1024);
      config.setChecksumType(ChecksumType.NONE);
      config.setBytesPerChecksum(256 * 1024);

      final StreamBufferArgs streamBufferArgs =
          StreamBufferArgs.getDefaultStreamBufferArgs(pipeline.getReplicationConfig(), config);

      return new BenchmarkSession(
          pipeline,
          ContainerClientMetrics.acquire(),
          new BufferPool(STREAM_BUFFER_SIZE, POOL_CAPACITY),
          config,
          streamBufferArgs,
          xcm,
          newFixedThreadPool(4));
    }

    BlockOutputStream newStream() throws IOException {
      return new RatisBlockOutputStream(
          new BlockID(1L, 1L),
          -1,
          xceiverClientManager,
          pipeline,
          bufferPool,
          config,
          null,
          metrics,
          streamBufferArgs,
          () -> responseExecutor);
    }

    @Override
    public void close() {
      bufferPool.clearBufferPool();
      responseExecutor.shutdownNow();
    }
  }

  private static final class Result {
    private final long bytesWritten;
    private final long writeOps;
    private final long blocksWritten;
    private final double elapsedSeconds;
    private final double mbPerSec;
    private final double nsPerWrite;
    private final long writeChunksDuringWrite;
    private final long flushesDuringWrite;

    private Result(long bytesWritten, long writeOps, long blocksWritten, double elapsedSeconds,
        double mbPerSec, double nsPerWrite, long writeChunksDuringWrite,
        long flushesDuringWrite) {
      this.bytesWritten = bytesWritten;
      this.writeOps = writeOps;
      this.blocksWritten = blocksWritten;
      this.elapsedSeconds = elapsedSeconds;
      this.mbPerSec = mbPerSec;
      this.nsPerWrite = nsPerWrite;
      this.writeChunksDuringWrite = writeChunksDuringWrite;
      this.flushesDuringWrite = flushesDuringWrite;
    }
  }
}
