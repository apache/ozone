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
import org.apache.hadoop.ozone.common.ChunkBuffer;
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
 * <p>Supports two comparison modes:
 * <ul>
 *   <li><b>heap vs direct</b> (default) — compares {@code ByteBuffer.allocate()} chunks
 *       (heap, {@code BoundedByteString}, single arraycopy to gRPC wire buffer) against
 *       {@code ByteBuffer.allocateDirect()} chunks (direct, {@code NioByteString}, two copies).</li>
 *   <li><b>checksum overhead</b> ({@code benchmark.checksumComparison=true}) — runs
 *       {@code NONE} then {@code CRC32} back-to-back with heap buffers and prints the
 *       per-write checksum overhead and throughput delta. Use this to quantify the cost of
 *       {@code Checksum.computeChecksum} (before the incremental-CRC change) and verify
 *       its elimination (after).</li>
 * </ul>
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
 *       1 MB, 2 MB, 3 MB and 4 MB)</li>
 *   <li>{@code benchmark.heapBuffer} – {@code true} or {@code false}
 *       (default: run both)</li>
 *   <li>{@code benchmark.checksumType} – {@code NONE} or {@code CRC32}
 *       (default {@code CRC32})</li>
 *   <li>{@code benchmark.threads} – worker count (default {@code CPUs * 2})</li>
 *   <li>{@code benchmark.scaling=true} – run multiple thread counts for both
 *       allocation modes and print a scaling summary</li>
 *   <li>{@code benchmark.scaling.threads} – comma-separated thread counts for
 *       scaling (default {@code 1,2,7,14,28,42,56})</li>
 *   <li>{@code benchmark.scaling.writeSizes} – comma-separated write sizes in
 *       bytes (overrides default 1/2/3/4 MB list when set)</li>
 *   <li>{@code benchmark.streamBufferSize} – chunk buffer size in bytes (default 4 MB).
 *       Use a small value (e.g. {@code 65536}) to call {@link BufferPool#allocateBuffer}
 *       64× more often, making per-call locking overhead visible in a profiler.</li>
 *   <li>{@code benchmark.fileSize} – bytes per stream open/close cycle
 *       (default 2 × poolCapacity × streamBufferSize = 64 MB with defaults).</li>
 *   <li>{@code benchmark.dnLatencyMs} – simulated Raft commit latency in milliseconds
 *       (default 0 = instant). The writer thread parks in {@code watchForCommit} for this
 *       duration, filling the {@link BufferPool} and creating concurrent L3 cache pressure
 *       across all writer threads. Use {@code 2} to match typical three-way Raft latency.
 *       Combined with {@code benchmark.threads ≥ 14}, this reproduces the cold-staging-buffer
 *       scenario captured by the real-cluster async-profiler.</li>
 *   <li>{@code benchmark.checksumComparison} – {@code true} to run {@code NONE} then
 *       {@code CRC32} back-to-back with heap buffers and print the per-write checksum
 *       overhead and throughput delta.</li>
 * </ul>
 *
 * <p>Or use
 * {@code hadoop-hdds/client/dev-support/run-block-output-stream-write-benchmark.sh}.
 */
public final class BlockOutputStreamWriteBenchmark {

  private static final int WARMUP_SECONDS = Integer.getInteger("benchmark.warmupSeconds", 10);
  private static final int BENCHMARK_SECONDS = Integer.getInteger("benchmark.benchmarkSeconds", 20);
  // Configurable via -Dbenchmark.streamBufferSize=<bytes> (default 4 MB).
  // Use a small value (e.g. 65536) to call allocateBuffer more frequently and
  // expose per-call BufferPool locking overhead in a profiler.
  private static final int STREAM_BUFFER_SIZE =
      Integer.getInteger("benchmark.streamBufferSize", 4 * 1024 * 1024);
  private static final int POOL_CAPACITY = 8;
  // Total bytes written per stream open/close cycle. Default = 2× pool capacity.
  private static final int FILE_SIZE =
      Integer.getInteger("benchmark.fileSize", 2 * POOL_CAPACITY * STREAM_BUFFER_SIZE);
  // Simulated Raft commit latency. Causes the calling thread to park in watchForCommit(),
  // filling the BufferPool and creating concurrent L3 cache pressure across all writer threads.
  // Use 1-5ms to match real three-way Raft commit latency.
  private static final long DN_LATENCY_MS = Long.getLong("benchmark.dnLatencyMs", 0);
  // Source byte array — kept at STREAM_BUFFER_SIZE for memory efficiency; reused in a loop.
  private static final int SOURCE_BUFFER_SIZE = STREAM_BUFFER_SIZE;

  private static final int[] DEFAULT_WRITE_SIZES = {
      STREAM_BUFFER_SIZE / 4,
      STREAM_BUFFER_SIZE / 2,
      STREAM_BUFFER_SIZE * 3 / 4,
      STREAM_BUFFER_SIZE
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
    System.out.println("Comparing: heap ByteBuffer (this patch) vs direct ByteBuffer (pre-patch)");
    System.out.println("JVM: " + System.getProperty("java.version")
        + " on " + System.getProperty("os.arch"));
    System.out.printf("CPUs=%d fileSize=%dKB streamBuffer=%dKB poolCapacity=%d checksum=%s dnLatency=%dms%n",
        cpus, FILE_SIZE / 1024, STREAM_BUFFER_SIZE / 1024, POOL_CAPACITY, checksumType, DN_LATENCY_MS);
    System.out.println();

    final byte[] sourceBuffer = new byte[SOURCE_BUFFER_SIZE];
    ThreadLocalRandom.current().nextBytes(sourceBuffer);

    if (Boolean.parseBoolean(System.getProperty("benchmark.scaling", "false"))) {
      runScalingStudy(sourceBuffer, checksumType, cpus);
    } else if (Boolean.parseBoolean(System.getProperty("benchmark.crcMatrix", "false"))) {
      runChecksumMatrix(sourceBuffer, resolveScalingThreadCounts(cpus), resolveWriteSizes());
    } else if (Boolean.parseBoolean(System.getProperty("benchmark.checksumComparison", "false"))) {
      final int threadCount = resolveThreadCount(cpus);
      System.out.printf("threads=%d%n%n", threadCount);
      runChecksumComparison(sourceBuffer, threadCount);
    } else {
      final int threadCount = resolveThreadCount(cpus);
      System.out.printf("threads=%d%n%n", threadCount);
      final Pipeline pipeline = MockPipeline.createRatisPipeline();
      final BenchmarkMockXceiverClient client = new BenchmarkMockXceiverClient(pipeline, DN_LATENCY_MS * 1_000_000L);
      try {
        for (int writeSize : resolveWriteSizes()) {
          for (boolean heapBuffer : resolveHeapBufferModes()) {
            runProfile(sourceBuffer, writeSize, heapBuffer, checksumType, threadCount, client);
            System.out.println();
          }
        }
      } finally {
        client.close();
      }
    }
  }

  /**
   * Runs NONE then CRC32 back-to-back with heap buffers and prints the per-write checksum
   * overhead. Use this to quantify computeChecksum cost before and after the incremental-CRC change.
   */
  private static void runChecksumComparison(byte[] sourceBuffer, int threadCount) throws Exception {
    final Pipeline pipeline = MockPipeline.createRatisPipeline();
    final BenchmarkMockXceiverClient client = new BenchmarkMockXceiverClient(pipeline, DN_LATENCY_MS * 1_000_000L);
    try {
      System.out.println("=== Checksum overhead comparison (heapBuffer=true) ===");
      System.out.println();
      for (int writeSize : resolveWriteSizes()) {
        final Result none = runProfile(sourceBuffer, writeSize, true, ChecksumType.NONE, threadCount, client);
        System.out.println();
        final Result crc32 = runProfile(sourceBuffer, writeSize, true, ChecksumType.CRC32, threadCount, client);
        System.out.println();
        final double overheadNs = crc32.nsPerWrite - none.nsPerWrite;
        final double overheadPct = none.mbPerSec == 0 ? 0 : (none.mbPerSec - crc32.mbPerSec) / none.mbPerSec * 100;
        System.out.printf(">>> writeSize=%dKB  NONE=%s MB/s  CRC32=%s MB/s  overhead=%.0fns/write (%.1f%%)%n%n",
            writeSize / 1024, MBPS.format(none.mbPerSec), MBPS.format(crc32.mbPerSec),
            overheadNs, overheadPct);
      }
    } finally {
      client.close();
    }
  }

  /**
   * Runs a thread-count × write-size matrix comparing NONE vs CRC32 with heap buffers.
   * Emits one MATRIX line per cell for easy grep/diff between before and after runs.
   * Use {@code benchmark.warmupSeconds} / {@code benchmark.benchmarkSeconds} to tune
   * duration (e.g. 3/7 for fast matrix sweeps).
   */
  private static void runChecksumMatrix(byte[] sourceBuffer, int[] threadCounts,
      int[] writeSizes) throws Exception {
    final Pipeline pipeline = MockPipeline.createRatisPipeline();
    final BenchmarkMockXceiverClient client = new BenchmarkMockXceiverClient(pipeline, DN_LATENCY_MS * 1_000_000L);
    try {
      System.out.printf("crcMatrix: %d thread counts × %d write sizes × {NONE,CRC32}"
          + "  dnLatency=%dms warmup=%ds benchmark=%ds%n%n",
          threadCounts.length, writeSizes.length, DN_LATENCY_MS, WARMUP_SECONDS, BENCHMARK_SECONDS);
      System.out.printf("%-10s %-10s %14s %14s %16s %8s%n",
          "threads", "writeKB", "NONE MB/s", "CRC32 MB/s", "overhead ns/w", "overhead%");
      System.out.println("----------------------------------------------------------------------------");
      for (int threadCount : threadCounts) {
        for (int writeSize : writeSizes) {
          final Result none = runProfile(sourceBuffer, writeSize, true, ChecksumType.NONE, threadCount, client);
          final Result crc32 = runProfile(sourceBuffer, writeSize, true, ChecksumType.CRC32, threadCount, client);
          final double overheadNs = crc32.nsPerWrite - none.nsPerWrite;
          final double overheadPct = none.mbPerSec == 0 ? 0 : (none.mbPerSec - crc32.mbPerSec) / none.mbPerSec * 100;
          System.out.printf("MATRIX %-8d %-8d %14s %14s %16.0f %7.1f%%%n",
              threadCount, writeSize / 1024,
              MBPS.format(none.mbPerSec), MBPS.format(crc32.mbPerSec),
              overheadNs, overheadPct);
        }
        System.out.println();
      }
    } finally {
      client.close();
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
    final BenchmarkMockXceiverClient sharedClient = new BenchmarkMockXceiverClient(pipeline, DN_LATENCY_MS * 1_000_000L);
    try {
      for (int writeSize : resolveWriteSizes()) {
        System.out.printf("===== writeSize=%dKB scaling study =====%n%n", writeSize / 1024);
        for (int threadCount : threadCounts) {
          for (boolean heapBuffer : new boolean[] {false, true}) {
              final Result result = runProfile(sourceBuffer, writeSize, heapBuffer,
                checksumType, threadCount, sharedClient);
            rows.add(new ScalingRow(writeSize, threadCount, heapBuffer, result.mbPerSec));
            System.out.println();
          }
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

    final double directBaseline = findMbPerSec(rows, 1, false);
    final double heapBaseline = findMbPerSec(rows, 1, true);

    System.out.printf("--- scaling summary writeSize=%dKB ---%n", writeSize / 1024);
    System.out.printf("%8s | %12s | %12s | %10s | %10s | %10s%n",
        "threads", "direct MB/s", "heap MB/s", "heap/direct", "direct eff.", "heap eff.");
    System.out.println("---------|--------------|--------------|------------|------------|------------");

    for (int threadCount : threadCounts) {
      final double direct = findMbPerSec(rows, threadCount, false);
      final double heap = findMbPerSec(rows, threadCount, true);
      final double heapVsDirect = direct == 0 ? 0 : heap / direct;
      final double directEff = directBaseline == 0 ? 0 : direct / directBaseline / threadCount;
      final double heapEff = heapBaseline == 0 ? 0 : heap / heapBaseline / threadCount;
      System.out.printf("%8d | %12s | %12s | %10.2fx | %10.2f | %10.2f%n",
          threadCount, MBPS.format(direct), MBPS.format(heap), heapVsDirect,
          directEff, heapEff);
    }
    System.out.println();
    System.out.println("eff. = throughput vs 1-thread baseline / thread count (1.0 = linear scaling)");
  }

  private static double findMbPerSec(List<ScalingRow> rows, int threadCount,
      boolean heapBuffer) {
    for (ScalingRow row : rows) {
      if (row.threadCount == threadCount && row.heapBuffer == heapBuffer) {
        return row.mbPerSec;
      }
    }
    return 0;
  }

  private static final class ScalingRow {
    private final int writeSize;
    private final int threadCount;
    private final boolean heapBuffer;
    private final double mbPerSec;

    private ScalingRow(int writeSize, int threadCount, boolean heapBuffer, double mbPerSec) {
      this.writeSize = writeSize;
      this.threadCount = threadCount;
      this.heapBuffer = heapBuffer;
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
      return new int[] {1, 2, 7, 14, 28, 42, 56};
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
      boolean heapBuffer, ChecksumType checksumType, int threadCount,
      BenchmarkMockXceiverClient client)
      throws Exception {
    System.out.printf("--- writeSize=%dKB heapBuffer=%s threads=%d writes/stream=%d ---%n",
        writeSize / 1024, heapBuffer, threadCount, FILE_SIZE / writeSize);

    final Result result = runProfileMeasured(sourceBuffer, writeSize, heapBuffer,
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
      boolean heapBuffer, ChecksumType checksumType, int threadCount,
      BenchmarkMockXceiverClient client)
      throws Exception {
    runTimedWrite(sourceBuffer, writeSize, heapBuffer, checksumType,
        threadCount, WARMUP_SECONDS, "warmup", client);
    return runTimedWrite(sourceBuffer, writeSize, heapBuffer, checksumType,
        threadCount, BENCHMARK_SECONDS, "benchmark", client);
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

  private static boolean[] resolveHeapBufferModes() {
    final String property = System.getProperty("benchmark.heapBuffer");
    if (property == null || property.isEmpty()) {
      return new boolean[] {false, true};
    }
    return new boolean[] {Boolean.parseBoolean(property)};
  }

  private static Result runTimedWrite(byte[] sourceBuffer, int writeSize,
      boolean heapBuffer, ChecksumType checksumType, int threadCount,
      int seconds, String phase, BenchmarkMockXceiverClient client) throws Exception {
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
            runWorker(sourceBuffer, writeSize, heapBuffer, checksumType, deadline, client)));
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
      System.out.printf("%s complete (%.2fs)%n", phase, elapsedSeconds);

      return new Result(
          bytesWritten,
          writeOps,
          blocksWritten,
          elapsedSeconds,
          bytesWritten / elapsedSeconds / (1024.0 * 1024.0),
          writeOps == 0 ? 0 : (double) elapsedNanos / writeOps,
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
      boolean heapBuffer, ChecksumType checksumType, long deadline,
      BenchmarkMockXceiverClient mockClient)
      throws Exception {
    long bytesWritten = 0;
    long writeOps = 0;
    long blocksWritten = 0;

    try (BenchmarkSession session = BenchmarkSession.open(heapBuffer, checksumType, mockClient)) {
      while (System.nanoTime() < deadline) {
        try (BlockOutputStream stream = session.newStream()) {
          int remaining = FILE_SIZE;
          while (remaining > 0 && System.nanoTime() < deadline) {
            int chunk = Math.min(writeSize, remaining);
            stream.write(sourceBuffer, 0, chunk);
            bytesWritten += chunk;
            writeOps++;
            remaining -= chunk;
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

    static BenchmarkSession open(boolean heapBuffer,
        ChecksumType checksumType, BenchmarkMockXceiverClient mockClient) throws IOException {
      final Pipeline pipeline = mockClient.getPipeline();
      final XceiverClientManager xcm = mock(XceiverClientManager.class);
      when(xcm.acquireClient(any())).thenReturn(mockClient);

      final OzoneClientConfig config = new OzoneClientConfig();
      config.setStreamBufferSize(STREAM_BUFFER_SIZE);
      config.setStreamBufferMaxSize(POOL_CAPACITY * STREAM_BUFFER_SIZE);
      config.setStreamBufferFlushDelay(false);
      config.setStreamBufferFlushSize(POOL_CAPACITY * STREAM_BUFFER_SIZE / 2);
      config.setChecksumType(checksumType);
      config.setBytesPerChecksum(Math.min(64 * 1024, STREAM_BUFFER_SIZE));
      config.validate();

      final StreamBufferArgs streamBufferArgs =
          StreamBufferArgs.getDefaultStreamBufferArgs(pipeline.getReplicationConfig(), config);

      // Both modes use the same BufferPool; ChunkBuffer.ALLOCATE_DIRECT controls
      // which allocator is called inside ChunkBuffer.allocate(), giving a true
      // apples-to-apples comparison of heap vs direct buffer serialisation overhead.
      ChunkBuffer.ALLOCATE_DIRECT.set(!heapBuffer);

      return new BenchmarkSession(
          pipeline,
          ContainerClientMetrics.acquire(),
          new BufferPool(STREAM_BUFFER_SIZE, POOL_CAPACITY,
              ByteStringConversion.createByteBufferConversion(true)),
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
