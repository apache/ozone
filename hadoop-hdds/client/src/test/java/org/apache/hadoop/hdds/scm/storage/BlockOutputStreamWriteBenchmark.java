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

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.scm.ByteStringConversion;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamBufferArgs;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.ozone.test.GenericTestUtils;
import org.slf4j.event.Level;

/**
 * Client-side {@link RatisBlockOutputStream} write benchmark using mocked
 * container RPCs ({@link BenchmarkMockXceiverClient}).
 *
 * <p>Mimics the write path exercised by {@code ozone freon dsfg} (DataStream
 * File Generator) — each worker thread repeatedly fills a 4 MB block via
 * sequential writes and closes the stream, driving the chunk serialisation and
 * gRPC framing code at full speed without real network or disk I/O.
 *
 * <p>Uses heap {@code ByteBuffer} backed chunks. {@code UnsafeByteOperations.unsafeWrap()}
 * returns a {@code BoundedByteString} with a backing array, so gRPC serialises via
 * a single {@code System.arraycopy} into the Netty wire buffer.
 *
 * <p>On-demand benchmark (not part of {@code mvn test}). Run from the repo root:
 * <pre>
 *   mvn -pl hadoop-hdds/client -q test-compile exec:java \
 *     -Dexec.mainClass=org.apache.hadoop.hdds.scm.storage.BlockOutputStreamWriteBenchmark \
 *     -Dexec.classpathScope=test
 * </pre>
 *
 * <p>Optional system properties:
 * <ul>
 *   <li>{@code benchmark.label} – profile label (default {@code workspace})</li>
 *   <li>{@code benchmark.writeSize} – single write size in bytes (default: run
 *       256 KB, 512 KB, 1 MB, 2 MB and 4 MB)</li>
 *   <li>{@code benchmark.checksumType} – {@code NONE} or {@code CRC32}
 *       (default {@code CRC32})</li>
 *   <li>{@code benchmark.threads} – worker count (default {@code CPUs * 2})</li>
 *   <li>{@code benchmark.dnLatencyMs} – simulated Raft commit latency in milliseconds
 *       (default 0 = instant). The writer thread parks in {@code watchForCommit} for this
 *       duration, filling the {@link BufferPool} and creating back-pressure that reproduces
 *       the cold-staging-buffer scenario from the real-cluster async-profiler.</li>
 *   <li>{@code benchmark.scaling=true} – run multiple thread counts and print
 *       a scaling summary</li>
 *   <li>{@code benchmark.scaling.threads} – comma-separated thread counts for
 *       scaling (default {@code 1,2,4,7,14,28,42})</li>
 *   <li>{@code benchmark.scaling.writeSizes} – comma-separated write sizes in
 *       bytes (overrides default list when set)</li>
 * </ul>
 */
public final class BlockOutputStreamWriteBenchmark {

  private static final int WARMUP_SECONDS = 10;
  private static final int BENCHMARK_SECONDS = 20;
  private static final int STREAM_BUFFER_SIZE = 4 * 1024 * 1024;
  private static final int SOURCE_BUFFER_SIZE = STREAM_BUFFER_SIZE;
  private static final int POOL_CAPACITY = 8;
  private static final long DN_LATENCY_MS = Long.getLong("benchmark.dnLatencyMs", 0);
  private static final boolean UNSAFE_BYTE_OPS =
      Boolean.parseBoolean(System.getProperty("benchmark.unsafeByteOps", "true"));

  private static final int[] DEFAULT_WRITE_SIZES = {
      256 * 1024,
      512 * 1024,
      1024 * 1024,
      2 * 1024 * 1024,
      4 * 1024 * 1024
  };

  private static final DecimalFormat MBPS = new DecimalFormat("#,##0.0");
  private static final DecimalFormat NS = new DecimalFormat("#,##0");
  private static final DecimalFormat COUNT = new DecimalFormat("#,##0");

  private BlockOutputStreamWriteBenchmark() {
  }

  public static void main(String[] args) throws Exception {
    GenericTestUtils.setLogLevel(BufferPool.class, Level.INFO);
    GenericTestUtils.setLogLevel(BlockOutputStream.class, Level.INFO);

    final String label = System.getProperty("benchmark.label", "workspace");
    final ChecksumType checksumType = ChecksumType.valueOf(
        System.getProperty("benchmark.checksumType", ChecksumType.CRC32.name()));
    final int cpus = Runtime.getRuntime().availableProcessors();

    System.out.println("BlockOutputStream client write benchmark (mocked container RPCs)");
    System.out.println("Profile: " + label);
    System.out.println("JVM: " + System.getProperty("java.version")
        + " on " + System.getProperty("os.arch"));
    System.out.printf(
        "CPUs=%d sourceBuffer=%dMB streamBuffer=%dMB poolCapacity=%d checksum=%s dnLatency=%dms byteOps=%s%n",
        cpus, SOURCE_BUFFER_SIZE / (1024 * 1024),
        STREAM_BUFFER_SIZE / (1024 * 1024), POOL_CAPACITY, checksumType, DN_LATENCY_MS,
        UNSAFE_BYTE_OPS ? "unsafe" : "safe");
    System.out.println();

    final byte[] sourceBuffer = new byte[SOURCE_BUFFER_SIZE];
    ThreadLocalRandom.current().nextBytes(sourceBuffer);

    if (Boolean.parseBoolean(System.getProperty("benchmark.scaling", "false"))) {
      runScalingStudy(sourceBuffer, checksumType, cpus);
    } else {
      final int threadCount = resolveThreadCount(cpus);
      System.out.printf("threads=%d%n%n", threadCount);
      final Pipeline pipeline = MockPipeline.createRatisPipeline();
      final BenchmarkMockXceiverClient client = new BenchmarkMockXceiverClient(pipeline, DN_LATENCY_MS * 1_000_000L);
      try {
        for (int writeSize : resolveWriteSizes()) {
          runProfile(sourceBuffer, writeSize, checksumType, threadCount, client);
          System.out.println();
        }
      } finally {
        client.close();
      }
    }
  }

  private static void runScalingStudy(byte[] sourceBuffer, ChecksumType checksumType,
      int cpus) throws Exception {
    final int[] threadCounts = resolveScalingThreadCounts(cpus);
    System.out.printf("scaling thread counts: %s%n%n", formatThreadCounts(threadCounts));
    final List<ScalingRow> rows = new ArrayList<>();

    // One shared gRPC client for the entire scaling study.  Reusing the same
    // event loop thread (and therefore the same PooledByteBufAllocator arena)
    // across all phases lets the arena recycle pool chunks between phases.
    // Creating a new client per phase assigns each new event loop to a
    // different arena (round-robin), and 0%-usage chunks in old arenas cannot
    // be reused by threads on new arenas, so direct memory grows unboundedly.
    final Pipeline pipeline = MockPipeline.createRatisPipeline();
    final BenchmarkMockXceiverClient sharedClient =
        new BenchmarkMockXceiverClient(pipeline, DN_LATENCY_MS * 1_000_000L);
    try {
      for (int writeSize : resolveWriteSizes()) {
        System.out.printf("===== writeSize=%dKB scaling study =====%n%n", writeSize / 1024);
        for (int threadCount : threadCounts) {
          final Result result = runProfile(sourceBuffer, writeSize, checksumType,
              threadCount, sharedClient);
          rows.add(new ScalingRow(writeSize, threadCount, result.mbPerSec));
          System.out.println();
        }
        printScalingSummary(writeSize, threadCounts, rows);
        System.out.println();
      }
    } finally {
      sharedClient.close();
    }
  }

  private static void printScalingSummary(int writeSize, int[] threadCounts,
      List<ScalingRow> allRows) {
    final List<ScalingRow> rows = new ArrayList<>();
    for (ScalingRow row : allRows) {
      if (row.writeSize == writeSize) {
        rows.add(row);
      }
    }

    final double baseline = findMbPerSec(rows, 1);

    System.out.printf("--- scaling summary writeSize=%dKB ---%n", writeSize / 1024);
    System.out.printf("%8s | %12s | %10s%n", "threads", "MB/s", "efficiency");
    System.out.println("---------|--------------|------------");

    for (int threadCount : threadCounts) {
      final double mbPerSec = findMbPerSec(rows, threadCount);
      final double eff = baseline == 0 ? 0 : mbPerSec / baseline / threadCount;
      System.out.printf("%8d | %12s | %10.2f%n", threadCount, MBPS.format(mbPerSec), eff);
    }
    System.out.println();
    System.out.println("efficiency = throughput vs 1-thread baseline / thread count (1.0 = linear scaling)");
  }

  private static double findMbPerSec(List<ScalingRow> rows, int threadCount) {
    for (ScalingRow row : rows) {
      if (row.threadCount == threadCount) {
        return row.mbPerSec;
      }
    }
    return 0;
  }

  private static final class ScalingRow {
    private final int writeSize;
    private final int threadCount;
    private final double mbPerSec;

    private ScalingRow(int writeSize, int threadCount, double mbPerSec) {
      this.writeSize = writeSize;
      this.threadCount = threadCount;
      this.mbPerSec = mbPerSec;
    }
  }

  private static int resolveThreadCount(int cpus) {
    final String property = System.getProperty("benchmark.threads");
    if (property != null && !property.isEmpty()) {
      return Integer.parseInt(property);
    }
    return cpus * 2;
  }

  private static int[] resolveScalingThreadCounts(int cpus) {
    final String property = System.getProperty("benchmark.scaling.threads");
    if (property == null || property.isEmpty()) {
      return new int[] {1, 2, 4, 7, 14, 28, 42};
    }
    return parseCommaSeparatedInts(property);
  }

  private static String formatThreadCounts(int[] threadCounts) {
    final StringBuilder builder = new StringBuilder();
    for (int i = 0; i < threadCounts.length; i++) {
      if (i > 0) {
        builder.append(", ");
      }
      builder.append(threadCounts[i]);
    }
    return builder.toString();
  }

  private static Result runProfile(byte[] sourceBuffer, int writeSize,
      ChecksumType checksumType, int threadCount,
      BenchmarkMockXceiverClient client)
      throws Exception {
    System.out.printf("--- writeSize=%dKB threads=%d writes/stream=%d ---%n",
        writeSize / 1024, threadCount, SOURCE_BUFFER_SIZE / writeSize);

    final Result result = runProfileMeasured(sourceBuffer, writeSize,
        checksumType, threadCount, client);

    System.out.printf("elapsed: %.2fs%n", result.elapsedSeconds);
    System.out.printf("bytes written: %s%n", COUNT.format(result.bytesWritten));
    System.out.printf("write ops (%dKB): %s%n",
        writeSize / 1024, COUNT.format(result.writeOps));
    System.out.printf("blocks written: %s%n", COUNT.format(result.blocksWritten));
    System.out.printf("aggregate throughput: %s MB/s%n", MBPS.format(result.mbPerSec));
    System.out.printf("ns/write (aggregate): %s%n", NS.format(result.nsPerWrite));
    System.out.printf("writeChunks metric: %s%n",
        COUNT.format(result.writeChunksDuringWrite));
    System.out.printf("flushes metric: %s%n",
        COUNT.format(result.flushesDuringWrite));
    return result;
  }

  private static Result runProfileMeasured(byte[] sourceBuffer, int writeSize,
      ChecksumType checksumType, int threadCount,
      BenchmarkMockXceiverClient client)
      throws Exception {
    runTimedWrite(sourceBuffer, writeSize, checksumType, threadCount, WARMUP_SECONDS, client);
    return runTimedWrite(sourceBuffer, writeSize, checksumType, threadCount, BENCHMARK_SECONDS, client);
  }

  private static int[] resolveWriteSizes() {
    final String scalingSizes = System.getProperty("benchmark.scaling.writeSizes");
    if (scalingSizes != null && !scalingSizes.isEmpty()) {
      return parseCommaSeparatedInts(scalingSizes);
    }
    final String property = System.getProperty("benchmark.writeSize");
    if (property == null || property.isEmpty()) {
      return DEFAULT_WRITE_SIZES;
    }
    return new int[] {Integer.parseInt(property)};
  }

  private static int[] parseCommaSeparatedInts(String property) {
    final String[] parts = property.split(",");
    final int[] values = new int[parts.length];
    for (int i = 0; i < parts.length; i++) {
      values[i] = Integer.parseInt(parts[i].trim());
    }
    return values;
  }

  private static Result runTimedWrite(byte[] sourceBuffer, int writeSize,
      ChecksumType checksumType, int threadCount,
      int seconds, BenchmarkMockXceiverClient client) throws Exception {
    final long writeChunksBefore =
        ContainerClientMetrics.acquire().getWriteChunksDuringWrite().value();
    final long flushesBefore =
        ContainerClientMetrics.acquire().getFlushesDuringWrite().value();

    final long start = System.nanoTime();
    final long deadline = start + seconds * 1_000_000_000L;

    final ExecutorService workers = Executors.newFixedThreadPool(threadCount);
    try {
      final List<Future<WorkerResult>> futures = new ArrayList<>(threadCount);
      for (int i = 0; i < threadCount; i++) {
        futures.add(workers.submit(() ->
            runWorker(sourceBuffer, writeSize, checksumType, deadline, client)));
      }

      long bytesWritten = 0;
      long writeOps = 0;
      long blocksWritten = 0;
      for (Future<WorkerResult> future : futures) {
        final WorkerResult workerResult = future.get();
        bytesWritten += workerResult.bytesWritten;
        writeOps += workerResult.writeOps;
        blocksWritten += workerResult.blocksWritten;
      }

      final long elapsedNanos = System.nanoTime() - start;
      final double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
      System.out.printf("complete (%.2fs)%n", elapsedSeconds);

      return new Result(
          bytesWritten,
          writeOps,
          blocksWritten,
          elapsedSeconds,
          ContainerClientMetrics.acquire().getWriteChunksDuringWrite().value()
              - writeChunksBefore,
          ContainerClientMetrics.acquire().getFlushesDuringWrite().value()
              - flushesBefore);
    } finally {
      workers.shutdownNow();
      try {
        workers.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static WorkerResult runWorker(byte[] sourceBuffer, int writeSize,
      ChecksumType checksumType, long deadline,
      BenchmarkMockXceiverClient mockClient)
      throws Exception {
    long bytesWritten = 0;
    long writeOps = 0;
    long blocksWritten = 0;

    try (BenchmarkSession session = BenchmarkSession.open(checksumType, mockClient)) {
      while (System.nanoTime() < deadline) {
        try (BlockOutputStream stream = session.newStream()) {
          for (int offset = 0; offset + writeSize <= sourceBuffer.length
              && System.nanoTime() < deadline; offset += writeSize) {
            stream.write(sourceBuffer, offset, writeSize);
            bytesWritten += writeSize;
            writeOps++;
          }
        }
        blocksWritten++;
      }
    }
    return new WorkerResult(bytesWritten, writeOps, blocksWritten);
  }

  private static final class WorkerResult {
    private final long bytesWritten;
    private final long writeOps;
    private final long blocksWritten;

    private WorkerResult(long bytesWritten, long writeOps, long blocksWritten) {
      this.bytesWritten = bytesWritten;
      this.writeOps = writeOps;
      this.blocksWritten = blocksWritten;
    }
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

    static BenchmarkSession open(ChecksumType checksumType,
        BenchmarkMockXceiverClient mockClient) throws IOException {
      final Pipeline pipeline = mockClient.getPipeline();
      final XceiverClientManager xcm = mock(XceiverClientManager.class);
      when(xcm.acquireClient(any())).thenReturn(mockClient);

      final OzoneClientConfig config = new OzoneClientConfig();
      config.setStreamBufferSize(STREAM_BUFFER_SIZE);
      config.setStreamBufferMaxSize(32 * 1024 * 1024);
      config.setStreamBufferFlushDelay(false);
      config.setStreamBufferFlushSize(16 * 1024 * 1024);
      config.setChecksumType(checksumType);
      config.setBytesPerChecksum(64 * 1024);
      config.validate();

      final StreamBufferArgs streamBufferArgs =
          StreamBufferArgs.getDefaultStreamBufferArgs(pipeline.getReplicationConfig(), config);

      return new BenchmarkSession(
          pipeline,
          ContainerClientMetrics.acquire(),
          new BufferPool(STREAM_BUFFER_SIZE, POOL_CAPACITY,
              ByteStringConversion.createByteBufferConversion(UNSAFE_BYTE_OPS)),
          config,
          streamBufferArgs,
          xcm,
          newSingleThreadExecutor());
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
      // mockClient is shared across all sessions in a phase; runTimedWrite owns it.
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
        long writeChunksDuringWrite, long flushesDuringWrite) {
      this.bytesWritten = bytesWritten;
      this.writeOps = writeOps;
      this.blocksWritten = blocksWritten;
      this.elapsedSeconds = elapsedSeconds;
      this.mbPerSec = bytesWritten / elapsedSeconds / (1024.0 * 1024.0);
      this.nsPerWrite = writeOps == 0 ? 0 : elapsedSeconds * 1e9 / writeOps;
      this.writeChunksDuringWrite = writeChunksDuringWrite;
      this.flushesDuringWrite = flushesDuringWrite;
    }
  }
}
