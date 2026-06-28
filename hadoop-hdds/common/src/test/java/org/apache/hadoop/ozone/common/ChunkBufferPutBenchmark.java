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

package org.apache.hadoop.ozone.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.ozone.common.JfrByteBufferAllocations.AllocationStats;

/**
 * Microbenchmark for ChunkBuffer.put(byte[]) direct copy vs ByteBuffer.wrap path.
 *
 * <p>Focused on the scenarios where HDDS-15485 shows the clearest benefit:
 * <ul>
 *   <li>Throughput: 4KB stream fill with incremental buffer (64KB increment)</li>
 *   <li>Allocations: same 4KB / 64KB-increment incremental buffer path (JFR + wrap calls)</li>
 * </ul>
 *
 * <p>Run from the repo root:
 * <pre>
 *   mvn -pl hadoop-hdds/common -q test-compile exec:java \
 *     -Dexec.mainClass=org.apache.hadoop.ozone.common.ChunkBufferPutBenchmark \
 *     -Dexec.classpathScope=test \
 *     -Dexec.args="--add-opens jdk.jfr/jdk.jfr=ALL-UNNAMED --add-opens jdk.jfr/jdk.jfr.consumer=ALL-UNNAMED"
 * </pre>
 * JFR ByteBuffer counts are sampled; put-op count reports exact wrap calls.
 */
public final class ChunkBufferPutBenchmark {

  private static final int WARMUP_SECONDS = 10;
  private static final int BENCHMARK_SECONDS = 20;
  private static final int ALLOCATION_BENCHMARK_SECONDS = 5;
  private static final int THROUGHPUT_ROUNDS = 3;
  private static final DecimalFormat MBPS = new DecimalFormat("#,##0.0");
  private static final DecimalFormat NS = new DecimalFormat("#,##0");
  private static final DecimalFormat PCT = new DecimalFormat("#,##0.0");
  private static final DecimalFormat RATIO = new DecimalFormat("0.00");
  private static final DecimalFormat COUNT = new DecimalFormat("#,##0");

  /** ozone.client.stream.buffer.size default. */
  private static final int DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024;

  /** Showcase: ozone.client.stream.buffer.increment for incremental buffer. */
  private static final int INCREMENTAL_BUFFER_INCREMENT = 64 * 1024;

  /** Hadoop io.file.buffer.size / FSDataOutputStream default. */
  private static final int HADOOP_FS_BUFFER_SIZE = 4 * 1024;

  private static final ThreadLocal<byte[]> SOURCE = ThreadLocal.withInitial(() -> {
    byte[] bytes = new byte[DEFAULT_CHUNK_SIZE];
    ThreadLocalRandom.current().nextBytes(bytes);
    return bytes;
  });

  private ChunkBufferPutBenchmark() {
  }

  public static void main(String[] args) throws IOException {
    System.out.println("ChunkBuffer.put(byte[]) microbenchmark (pre-allocated buffer, put-only)");
    System.out.println("JVM: " + System.getProperty("java.version")
        + " on " + System.getProperty("os.arch"));
    System.out.println();

    final Scenario showcase = new Scenario(
        "Incremental buffer showcase",
        "ozone.client.stream.buffer.size=4MB, "
            + "ozone.client.stream.buffer.increment=64KB, io.file.buffer.size=4KB",
        "4KB stream fill into IncrementalChunkBuffer (64KB steps)",
        DEFAULT_CHUNK_SIZE,
        INCREMENTAL_BUFFER_INCREMENT,
        HADOOP_FS_BUFFER_SIZE);

    System.out.println("=== Throughput showcase ===");
    runThroughputComparison(showcase);
    System.out.println();

    System.out.println("=== Allocation showcase ===");
    runAllocationComparison(showcase);
  }

  private static void runThroughputComparison(Scenario scenario) {
    printScenarioHeader(scenario);
    warmupBothPaths(scenario);
    final double[] improvements = new double[THROUGHPUT_ROUNDS];
    for (int round = 0; round < THROUGHPUT_ROUNDS; round++) {
      final boolean directFirst = round % 2 == 0;
      final Result direct;
      final Result wrap;
      if (directFirst) {
        direct = benchmarkThroughput(scenario, ChunkBufferPutBenchmark::loopStreamFillDirect);
        wrap = benchmarkThroughput(scenario, ChunkBufferPutBenchmark::loopStreamFillWrap);
      } else {
        wrap = benchmarkThroughput(scenario, ChunkBufferPutBenchmark::loopStreamFillWrap);
        direct = benchmarkThroughput(scenario, ChunkBufferPutBenchmark::loopStreamFillDirect);
      }
      improvements[round] = wrap.nsPerOp / direct.nsPerOp - 1.0;
      System.out.printf("  round %d:%n", round + 1);
      printThroughputComparison(scenario.writeSize, direct, wrap, "    ");
    }
    final double medianImprovement = median(improvements) * 100.0;
    System.out.printf("  median improvement over %d rounds: %s%%%n",
        THROUGHPUT_ROUNDS, PCT.format(medianImprovement));
  }

  private static void warmupBothPaths(Scenario scenario) {
    final byte[] source = SOURCE.get();
    final int writesPerChunk = scenario.chunkSize / scenario.writeSize;
    try (ChunkBuffer buffer = ChunkBuffer.allocate(scenario.chunkSize,
        scenario.bufferIncrement)) {
      for (int i = 0; i < 2; i++) {
        loopStreamFillDirect(buffer, source, scenario.writeSize, writesPerChunk,
            WARMUP_SECONDS);
        loopStreamFillWrap(buffer, source, scenario.writeSize, writesPerChunk,
            WARMUP_SECONDS);
      }
    }
  }

  private static double median(double[] values) {
    final double[] sorted = values.clone();
    java.util.Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  private static void runAllocationComparison(Scenario scenario) throws IOException {
    printScenarioHeader(scenario);
    if (!JfrByteBufferAllocations.isAvailable()) {
      System.out.println("  JFR unavailable; reporting wrap-call count from put ops only.");
    }
    final AllocationResult direct = benchmarkAllocations(scenario,
        ChunkBufferPutBenchmark::loopStreamFillDirect);
    final AllocationResult wrap = benchmarkAllocations(scenario,
        ChunkBufferPutBenchmark::loopStreamFillWrap);
    printAllocationComparison(direct, wrap);
  }

  private static void printScenarioHeader(Scenario scenario) {
    System.out.println("--- " + scenario.name + " ---");
    System.out.println("Config: " + scenario.config);
    System.out.println("Pattern: " + scenario.pattern);
    System.out.printf("Chunk=%dKB increment=%dKB write=%dKB%n",
        scenario.chunkSize / 1024,
        scenario.bufferIncrement / 1024,
        scenario.writeSize / 1024);
  }

  private static void printThroughputComparison(int writeSize, Result direct, Result wrap,
      String prefix) {
    final double speedup = wrap.nsPerOp / direct.nsPerOp;
    final double pctFaster = (speedup - 1.0) * 100.0;
    final double throughputSpeedup = direct.mbPerSec / wrap.mbPerSec;
    final double throughputPct = (throughputSpeedup - 1.0) * 100.0;
    System.out.printf("%sdirect put(byte[]):   %s MB/s | %s ns/op | %.2fs | %s ops%n",
        prefix, MBPS.format(direct.mbPerSec), NS.format(direct.nsPerOp),
        direct.elapsedSeconds, COUNT.format(direct.ops));
    System.out.printf("%swrap put(ByteBuffer): %s MB/s | %s ns/op | %.2fs | %s ops%n",
        prefix, MBPS.format(wrap.mbPerSec), NS.format(wrap.nsPerOp),
        wrap.elapsedSeconds, COUNT.format(wrap.ops));
    System.out.printf("%simprovement: %s%% faster (%sx) per %dKB write; "
            + "throughput %s%% (%sx)%n",
        prefix, PCT.format(pctFaster), RATIO.format(speedup), writeSize / 1024,
        PCT.format(throughputPct), RATIO.format(throughputSpeedup));
  }

  private static void printAllocationComparison(AllocationResult direct,
      AllocationResult wrap) {
    System.out.printf("  direct put(byte[]):   %s put ops | %s ByteBuffer TLAB allocs | %s alloc bytes%n",
        COUNT.format(direct.putOps), COUNT.format(direct.byteBufferAllocCount),
        COUNT.format(direct.byteBufferAllocBytes));
    System.out.printf("  wrap put(ByteBuffer): %s put ops | %s ByteBuffer TLAB allocs | %s alloc bytes%n",
        COUNT.format(wrap.putOps), COUNT.format(wrap.byteBufferAllocCount),
        COUNT.format(wrap.byteBufferAllocBytes));
    System.out.printf("  ByteBuffer.wrap calls on wrap path (1 per put): %s%n",
        COUNT.format(wrap.putOps));
    System.out.printf("  direct path avoids %s ByteBuffer.wrap calls per run%n",
        COUNT.format(wrap.putOps));
    if (wrap.byteBufferAllocCount > 0 && direct.byteBufferAllocCount == 0) {
      System.out.printf("  JFR confirms zero ByteBuffer TLAB allocations on direct path%n");
    }
    if (wrap.byteBufferAllocCount > direct.byteBufferAllocCount) {
      final long saved = wrap.byteBufferAllocCount - direct.byteBufferAllocCount;
      System.out.printf("  JFR sampled ByteBuffer TLAB allocations avoided on direct path: %s%n",
          COUNT.format(saved));
      System.out.println("  (JFR samples TLAB events; put-op count is the exact wrap-call metric)");
    }
  }

  private static Result benchmarkThroughput(Scenario scenario, TimedLoop loop) {
    final byte[] source = SOURCE.get();
    final int writesPerChunk = scenario.chunkSize / scenario.writeSize;
    try (ChunkBuffer buffer = ChunkBuffer.allocate(scenario.chunkSize,
        scenario.bufferIncrement)) {
      loop.run(buffer, source, scenario.writeSize, writesPerChunk, WARMUP_SECONDS);
      final LoopResult benchmark = loop.run(buffer, source, scenario.writeSize,
          writesPerChunk, BENCHMARK_SECONDS);
      return toResult(benchmark, scenario.writeSize);
    }
  }

  private static AllocationResult benchmarkAllocations(Scenario scenario, TimedLoop loop)
      throws IOException {
    final byte[] source = SOURCE.get();
    final int writesPerChunk = scenario.chunkSize / scenario.writeSize;
    try (ChunkBuffer buffer = ChunkBuffer.allocate(scenario.chunkSize,
        scenario.bufferIncrement)) {
      loop.run(buffer, source, scenario.writeSize, writesPerChunk, WARMUP_SECONDS);
      final LoopResult[] benchmark = new LoopResult[1];
      final AllocationStats stats = JfrByteBufferAllocations.measure(
          () -> benchmark[0] = loop.run(buffer, source, scenario.writeSize,
              writesPerChunk, ALLOCATION_BENCHMARK_SECONDS));
      final long putOps = benchmark[0].totalBytes / scenario.writeSize;
      return new AllocationResult(putOps, stats.getByteBufferAllocCount(),
          stats.getByteBufferAllocBytes());
    }
  }

  private static Result toResult(LoopResult benchmark, int writeSize) {
    final long elapsedNanos = benchmark.elapsedNanos;
    final long benchmarkBytes = benchmark.totalBytes;
    final double seconds = elapsedNanos / 1_000_000_000.0;
    final double mbPerSec = benchmarkBytes / seconds / (1024.0 * 1024.0);
    final long ops = benchmarkBytes / writeSize;
    final double nsPerOp = (double) elapsedNanos / ops;
    return new Result(mbPerSec, nsPerOp, seconds, ops);
  }

  @FunctionalInterface
  private interface TimedLoop {
    LoopResult run(ChunkBuffer buffer, byte[] source, int writeSize,
        int writesPerChunk, int seconds);
  }

  private static LoopResult loopStreamFillDirect(ChunkBuffer buffer, byte[] source,
      int writeSize, int writesPerChunk, int seconds) {
    long totalBytes = 0;
    final long start = System.nanoTime();
    final long deadline = start + seconds * 1_000_000_000L;
    while (System.nanoTime() < deadline) {
      buffer.clear();
      int off = 0;
      for (int i = 0; i < writesPerChunk; i++) {
        buffer.put(source, off, writeSize);
        off += writeSize;
        totalBytes += writeSize;
      }
    }
    return new LoopResult(totalBytes, System.nanoTime() - start);
  }

  private static LoopResult loopStreamFillWrap(ChunkBuffer buffer, byte[] source,
      int writeSize, int writesPerChunk, int seconds) {
    long totalBytes = 0;
    final long start = System.nanoTime();
    final long deadline = start + seconds * 1_000_000_000L;
    while (System.nanoTime() < deadline) {
      buffer.clear();
      int off = 0;
      for (int i = 0; i < writesPerChunk; i++) {
        buffer.put(ByteBuffer.wrap(source, off, writeSize));
        off += writeSize;
        totalBytes += writeSize;
      }
    }
    return new LoopResult(totalBytes, System.nanoTime() - start);
  }

  private static final class LoopResult {
    private final long totalBytes;
    private final long elapsedNanos;

    private LoopResult(long totalBytes, long elapsedNanos) {
      this.totalBytes = totalBytes;
      this.elapsedNanos = elapsedNanos;
    }
  }

  private static final class AllocationResult {
    private final long putOps;
    private final long byteBufferAllocCount;
    private final long byteBufferAllocBytes;

    private AllocationResult(long putOps, long byteBufferAllocCount,
        long byteBufferAllocBytes) {
      this.putOps = putOps;
      this.byteBufferAllocCount = byteBufferAllocCount;
      this.byteBufferAllocBytes = byteBufferAllocBytes;
    }
  }

  private static final class Scenario {
    private final String name;
    private final String config;
    private final String pattern;
    private final int chunkSize;
    private final int bufferIncrement;
    private final int writeSize;

    private Scenario(String name, String config, String pattern, int chunkSize,
        int bufferIncrement, int writeSize) {
      this.name = name;
      this.config = config;
      this.pattern = pattern;
      this.chunkSize = chunkSize;
      this.bufferIncrement = bufferIncrement;
      this.writeSize = writeSize;
    }
  }

  private static final class Result {
    private final double mbPerSec;
    private final double nsPerOp;
    private final double elapsedSeconds;
    private final long ops;

    private Result(double mbPerSec, double nsPerOp, double elapsedSeconds, long ops) {
      this.mbPerSec = mbPerSec;
      this.nsPerOp = nsPerOp;
      this.elapsedSeconds = elapsedSeconds;
      this.ops = ops;
    }
  }
}
