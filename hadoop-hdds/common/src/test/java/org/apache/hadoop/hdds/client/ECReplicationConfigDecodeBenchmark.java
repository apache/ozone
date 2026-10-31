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

package org.apache.hadoop.hdds.client;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntFunction;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Benchmark for decoding EC replication configs from protos, as done for every container, pipeline and key that
 * OM, SCM and clients read. It reports how much heap the decoded objects retain, how many distinct
 * {@link ECReplicationConfig} instances they hold, the time and allocation per decode on one thread, and the
 * throughput with several threads.
 *
 * <p>The last case decodes configs that are not allowed by default by {@code ozone.replication.allowed-configs}, as a
 * client sending arbitrary values could, to measure the cost for configs that are not shared.
 *
 * <p>Only public API is used, so the same class can be run before and after a change for an A/B comparison.
 *
 * <p>Run with:
 * <pre>
 *   mvn test -pl :hdds-common \
 *     -Dtest=ECReplicationConfigDecodeBenchmark \
 *     -Dgroups=benchmark -Dexcluded-test-groups= \
 *     -Dsurefire.failIfNoSpecifiedTests=false
 * </pre>
 */
@Tag("benchmark")
public class ECReplicationConfigDecodeBenchmark {

  /** Number of decoded objects kept alive, like the containers in SCM's container map. */
  private static final int RETAINED = 1_000_000;
  private static final int[] THREAD_COUNTS = {1, 4, 8, 16};
  private static final int ROUNDS = 5;
  private static final int WARMUP_MS = 2_000;
  private static final int MEASURE_MS = 1_000;
  /** Calls per {@code System.nanoTime()} poll to amortise the timer cost. */
  private static final int BATCH = 1_000;

  /** The EC configs a cluster typically uses. */
  private static final String[] EC_STRINGS = {"rs-3-2-1024k", "rs-6-3-1024k", "rs-10-4-1024k"};
  private static final HddsProtos.ECReplicationConfig[] EC_PROTOS = protos(EC_STRINGS);
  private static final HddsProtos.ContainerInfoProto[] CONTAINER_PROTOS = containerProtos(EC_PROTOS);

  /** Valid configs that ozone.replication.allowed-configs does not allow by default. */
  private static final String[] OTHER_EC_STRINGS = {"rs-4-2-1024k", "rs-6-3-8192k", "rs-12-4-1024k"};
  private static final HddsProtos.ECReplicationConfig[] OTHER_EC_PROTOS = protos(OTHER_EC_STRINGS);
  private static final HddsProtos.ContainerInfoProto[] OTHER_CONTAINER_PROTOS = containerProtos(OTHER_EC_PROTOS);

  @Test
  public void benchmarkRetainedHeap() {
    printRetainedHeader("3 EC configs");
    retained("ReplicationConfig", fromProto(EC_PROTOS));
    retained("ContainerInfo", fromProtobuf(CONTAINER_PROTOS));
  }

  @Test
  public void benchmarkDecode() {
    printDecodeHeader("3 EC configs");
    decode("ReplicationConfig", fromProto(EC_PROTOS));
    decode("ContainerInfo", fromProtobuf(CONTAINER_PROTOS));
    decode("ECReplicationConfig(String)", fromString(EC_STRINGS));
  }

  @Test
  public void benchmarkThroughput() throws Exception {
    System.out.println();
    System.out.printf("=== Decode throughput, 3 EC configs, best of %d rounds of %d ms ===%n", ROUNDS, MEASURE_MS);
    System.out.printf("%-9s  %22s  %8s  %22s  %8s%n", "Threads", "ReplicationConfig/ms", "GC %", "ContainerInfo/ms",
        "GC %");
    for (int threads : THREAD_COUNTS) {
      final double[] config = bestThroughput(threads, fromProto(EC_PROTOS));
      final double[] container = bestThroughput(threads, fromProtobuf(CONTAINER_PROTOS));
      System.out.printf("%-9d  %22.1f  %8.1f  %22.1f  %8.1f%n", threads, config[0], config[1], container[0],
          container[1]);
    }
  }

  @Test
  public void benchmarkOtherConfigs() {
    printRetainedHeader("3 other EC configs");
    retained("ReplicationConfig", fromProto(OTHER_EC_PROTOS));
    retained("ContainerInfo", fromProtobuf(OTHER_CONTAINER_PROTOS));
    printDecodeHeader("3 other EC configs");
    decode("ReplicationConfig", fromProto(OTHER_EC_PROTOS));
    decode("ContainerInfo", fromProtobuf(OTHER_CONTAINER_PROTOS));
    decode("ECReplicationConfig(String)", fromString(OTHER_EC_STRINGS));
  }

  private static HddsProtos.ECReplicationConfig[] protos(String... configs) {
    final HddsProtos.ECReplicationConfig[] protos = new HddsProtos.ECReplicationConfig[configs.length];
    for (int i = 0; i < configs.length; i++) {
      protos[i] = new ECReplicationConfig(configs[i]).toProto();
    }
    return protos;
  }

  private static HddsProtos.ContainerInfoProto[] containerProtos(HddsProtos.ECReplicationConfig[] ecProtos) {
    final HddsProtos.ContainerInfoProto[] protos = new HddsProtos.ContainerInfoProto[ecProtos.length];
    for (int i = 0; i < ecProtos.length; i++) {
      protos[i] = HddsProtos.ContainerInfoProto.newBuilder()
          .setContainerID(i + 1)
          .setState(HddsProtos.LifeCycleState.CLOSED)
          .setUsedBytes(5L << 30)
          .setNumberOfKeys(1000)
          .setOwner("om")
          .setReplicationType(HddsProtos.ReplicationType.EC)
          .setEcReplicationConfig(ecProtos[i])
          .build();
    }
    return protos;
  }

  private static IntFunction<Object> fromProto(HddsProtos.ECReplicationConfig[] protos) {
    return i -> ReplicationConfig.fromProto(HddsProtos.ReplicationType.EC, null, protos[i % protos.length]);
  }

  private static IntFunction<Object> fromString(String[] strings) {
    return i -> new ECReplicationConfig(strings[i % strings.length]);
  }

  private static IntFunction<Object> fromProtobuf(HddsProtos.ContainerInfoProto[] protos) {
    return i -> ContainerInfo.fromProtobuf(protos[i % protos.length]);
  }

  private static void printRetainedHeader(String what) {
    System.out.println();
    System.out.printf("=== Retained heap for %,d decoded objects, %s ===%n", RETAINED, what);
    System.out.printf("%-26s  %14s  %12s  %20s%n", "Objects", "retained MB", "bytes/object", "distinct EC configs");
  }

  private static void printDecodeHeader(String what) {
    System.out.println();
    System.out.printf("=== Decode, single thread, %s, best of %d rounds of %d ms ===%n", what, ROUNDS, MEASURE_MS);
    System.out.printf("%-26s  %12s  %14s%n", "Operation", "ns/call", "bytes/call");
  }

  private static void retained(String name, IntFunction<Object> decode) {
    final Object[] objects = new Object[RETAINED];
    final long afterArray = usedHeapAfterGc();
    for (int i = 0; i < RETAINED; i++) {
      objects[i] = decode.apply(i);
    }
    final long after = usedHeapAfterGc();

    final Set<Object> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Object o : objects) {
      distinct.add(o instanceof ContainerInfo ? ((ContainerInfo) o).getReplicationConfig() : o);
    }
    // the array itself is not counted
    final long bytes = after - afterArray;
    System.out.printf("%-26s  %14.1f  %12.1f  %,20d%n", name, bytes / 1048576.0, (double) bytes / RETAINED,
        distinct.size());
  }

  private static long usedHeapAfterGc() {
    long used = Long.MAX_VALUE;
    for (int i = 0; i < 5; i++) {
      System.gc();
      used = Math.min(used, ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
    }
    return used;
  }

  private static void decode(String name, IntFunction<Object> decode) {
    com.sun.management.ThreadMXBean threadBean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    long tid = Thread.currentThread().getId();
    final Object[] ring = newRing();
    runFor(decode, WARMUP_MS, ring);
    double bestNs = Double.MAX_VALUE;
    double bytes = 0;
    for (int round = 0; round < ROUNDS; round++) {
      long bytesBefore = threadBean.getThreadAllocatedBytes(tid);
      long start = System.nanoTime();
      long calls = runFor(decode, MEASURE_MS, ring);
      long elapsed = System.nanoTime() - start;
      bytes = (double) (threadBean.getThreadAllocatedBytes(tid) - bytesBefore) / calls;
      bestNs = Math.min(bestNs, (double) elapsed / calls);
    }
    System.out.printf("%-26s  %12.1f  %14.0f%n", name, bestNs, bytes);
  }

  /** @return calls/ms of the best round, and the share of that round's wall time spent in GC, in percent. */
  private static double[] bestThroughput(int threads, IntFunction<Object> decode) throws Exception {
    double[] best = {0, 0};
    for (int round = 0; round < ROUNDS; round++) {
      final long gcBefore = gcMillis();
      final long start = System.nanoTime();
      final double callsPerMs = throughput(threads, decode);
      final long wallMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      final double gcPercent = 100.0 * (gcMillis() - gcBefore) / wallMillis;
      if (callsPerMs > best[0]) {
        best = new double[] {callsPerMs, gcPercent};
      }
    }
    return best;
  }

  private static long gcMillis() {
    long total = 0;
    for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      total += Math.max(0, gc.getCollectionTime());
    }
    return total;
  }

  private static double throughput(int threads, IntFunction<Object> decode) throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    LongAdder total = new LongAdder();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    try {
      for (int t = 0; t < threads; t++) {
        pool.submit(() -> {
          try {
            final Object[] ring = newRing();
            start.await();
            runFor(decode, WARMUP_MS / 4, ring);
            total.add(runFor(decode, MEASURE_MS, ring));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            done.countDown();
          }
        });
      }
      start.countDown();
      if (!done.await(60, TimeUnit.SECONDS)) {
        throw new AssertionError("benchmark threads did not finish within 60 s");
      }
    } finally {
      pool.shutdownNow();
    }
    return (double) total.sum() / MEASURE_MS;
  }

  /**
   * Decoded objects escape into this array, as they do into SCM's and OM's maps, so the JIT cannot eliminate them.
   * Each thread has its own, so threads do not contend on it.
   */
  private static Object[] newRing() {
    return new Object[1024];
  }

  /** Calls {@code decode} in batches for {@code millis} and returns the number of calls. */
  private static long runFor(IntFunction<Object> decode, int millis, Object[] ring) {
    long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
    long calls = 0;
    while (System.nanoTime() < end) {
      for (int i = 0; i < BATCH; i++) {
        ring[i & (ring.length - 1)] = decode.apply(i);
      }
      calls += BATCH;
    }
    return calls;
  }
}
