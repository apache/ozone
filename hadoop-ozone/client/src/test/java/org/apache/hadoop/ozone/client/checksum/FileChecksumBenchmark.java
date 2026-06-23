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

package org.apache.hadoop.ozone.client.checksum;

import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Benchmark for ECFileChecksumHelper that measures the actual checksum
 * collection code path. Not part of the regular test suite — it takes ~2min
 * per run (warmup + measurement across two EC configs × three latency buckets).
 *
 * Validates fixes to BaseFileChecksumHelper and ECFileChecksumHelper:
 *   Fix 1: BaseFileChecksumHelper 7-arg constructor no longer chains to 6-arg
 *           (redundant OM lookupKey RPC eliminated: 2 calls/file -> 1).
 *   Fix 2: ECFileChecksumHelper uses a deterministic pipeline ID so
 *           XceiverClientManager can cache and reuse gRPC connections
 *           (new connection per file -> one connection per placement group).
 *   Fix 3: Pipeline.newBuilder() instead of toBuilder() avoids an unnecessary
 *           SecureRandom.nextBytes call on every file.
 *
 * Covers RS-3-2 (5 nodes, 3 data + 2 parity) and RS-6-3 (9 nodes, 6 data + 3 parity).
 * For RS-3-2 the standalone pipeline has 3 selected nodes (index=1 + parity {4,5}),
 * stripe checksum = 12 bytes. For RS-6-3: 4 selected nodes (index=1 + parity {7,8,9}),
 * stripe checksum = 16 bytes.
 *
 * Each latency bucket runs a 10-second warmup followed by a 20-second
 * measurement window. Throughput and per-file RPC counts are reported from
 * the measurement window only.
 *
 * Run with:
 *   mvn test -pl hadoop-ozone/client \
 *     -Dtest=FileChecksumBenchmark#runBenchmark \
 *     -Dsurefire.failIfNoSpecifiedTests=false
 *
 * To profile with async-profiler, pass the agent via surefire argLine, e.g.:
 *   mvn test -pl hadoop-ozone/client \
 *     -Dtest=FileChecksumBenchmark#runBenchmark \
 *     -Dsurefire.failIfNoSpecifiedTests=false \
 *     -DargLine="-agentpath:/opt/homebrew/lib/libasyncProfiler.dylib=start,event=wall,\
 *       interval=10ms,file=/tmp/profile.html"
 */
@Tag("benchmark")
public class FileChecksumBenchmark {

  private static final int NUM_THREADS = 5;
  private static final int WARMUP_SECS = 10;
  private static final int MEASURE_SECS = 20;
  private static final int[] LATENCIES_MS = {0, 5, 10};
  private static final long CONTAINER_ID = 1001L;
  private static final long FILE_SIZE = 5000L;
  // 136 = 4 x 34: every 4th file (idx % 4 == 0) gets a freshly built OmKeyInfo
  // with random node UUIDs on each call, making the deterministic pipeline ID
  // also effectively random -> guaranteed cache miss for those files.
  // Max theoretical cache hit rate = 75% (102 stable / 136 total).
  private static final int KEY_POOL = 136;
  private static final int UNSTABLE_STEP = 4;

  private static final ECReplicationConfig EC32 = new ECReplicationConfig(3, 2);
  private static final ECReplicationConfig EC63 = new ECReplicationConfig(6, 3);

  // RS-3-2: selected nodes = index 1 + parity {4, 5} = 3 nodes → 3 × 4 = 12 stripe bytes
  private static final List<DatanodeDetails> EC_NODES = buildEcNodes(5);
  private static final Pipeline EC_PIPELINE = buildEcPipeline(EC_NODES, EC32);
  private static final OmKeyInfo[] KEY_INFOS = buildKeyInfos(EC_PIPELINE, EC32);
  private static final ContainerProtos.ContainerCommandResponseProto GET_BLOCK_RESPONSE =
      buildGetBlockResponse(12);

  // RS-6-3: selected nodes = index 1 + parity {7, 8, 9} = 4 nodes → 4 × 4 = 16 stripe bytes
  private static final List<DatanodeDetails> EC63_NODES = buildEcNodes(9);
  private static final Pipeline EC63_PIPELINE = buildEcPipeline(EC63_NODES, EC63);
  private static final OmKeyInfo[] EC63_KEY_INFOS = buildKeyInfos(EC63_PIPELINE, EC63);
  private static final ContainerProtos.ContainerCommandResponseProto EC63_GET_BLOCK_RESPONSE =
      buildGetBlockResponse(16);

  // ---------------------------------------------------------------------------
  // Instrumented XceiverClientFactory
  // ---------------------------------------------------------------------------

  static class CountingXceiverClientFactory implements XceiverClientFactory {
    // Stable pipeline IDs = KEY_POOL - KEY_POOL/UNSTABLE_STEP = 102.
    // Cap the pool just above that so stable files always hit the cache while
    // unstable files (random pipeline IDs) are never stored and are GC'd immediately.
    private static final int MAX_POOL_SIZE = KEY_POOL - KEY_POOL / UNSTABLE_STEP + 10;
    private final ContainerProtos.ContainerCommandResponseProto blockResponse;
    private final AtomicLong newConnectionCount = new AtomicLong();
    private final AtomicLong reuseCount = new AtomicLong();
    // clientPool is intentionally NOT cleared between warmup and measurement:
    // simulates a warmed-up JVM where connections established during warmup
    // remain available -- the same state as a long-running service.
    private final ConcurrentHashMap<String, XceiverClientSpi> clientPool =
        new ConcurrentHashMap<>();

    CountingXceiverClientFactory(
        ContainerProtos.ContainerCommandResponseProto blockResponse) {
      this.blockResponse = blockResponse;
    }

    void resetCounters() {
      newConnectionCount.set(0);
      reuseCount.set(0);
    }

    long getNewConnectionCount() {
      return newConnectionCount.get();
    }

    long getReuseCount() {
      return reuseCount.get();
    }

    @Override
    public XceiverClientSpi acquireClientForReadData(Pipeline pipeline)
        throws IOException {
      String key = pipeline.getId().toString();
      XceiverClientSpi existing = clientPool.get(key);
      if (existing != null) {
        reuseCount.incrementAndGet();
        return existing;
      }
      XceiverClientSpi newClient = createMockDnClient(pipeline, blockResponse);
      if (clientPool.size() < MAX_POOL_SIZE) {
        XceiverClientSpi winner = clientPool.putIfAbsent(key, newClient);
        if (winner != null) {
          reuseCount.incrementAndGet();
          return winner;
        }
      }
      newConnectionCount.incrementAndGet();
      return newClient;
    }

    @Override
    public void releaseClientForReadData(XceiverClientSpi client,
        boolean invalidate) { }

    @Override
    public XceiverClientSpi acquireClient(Pipeline pipeline) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void releaseClient(XceiverClientSpi client, boolean invalidate) { }

    @Override
    public XceiverClientSpi acquireClient(Pipeline pipeline,
        boolean topologyAware) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void releaseClient(XceiverClientSpi client, boolean invalidate,
        boolean topologyAware) { }

    @Override
    public void close() { }
  }

  // ---------------------------------------------------------------------------
  // Benchmark result
  // ---------------------------------------------------------------------------

  static class BenchmarkResult {
    private final long measuredFiles;
    private final long wallMs;
    private final long omCalls;
    private final long xceiverNewCount;
    private final long xceiverReuseCount;
    private final int latencyMs;

    BenchmarkResult(long measuredFiles, long wallMs, long omCalls,
        long xceiverNewCount, long xceiverReuseCount, int latencyMs) {
      this.measuredFiles = measuredFiles;
      this.wallMs = wallMs;
      this.omCalls = omCalls;
      this.xceiverNewCount = xceiverNewCount;
      this.xceiverReuseCount = xceiverReuseCount;
      this.latencyMs = latencyMs;
    }

    long getMeasuredFiles() {
      return measuredFiles;
    }

    long getWallMs() {
      return wallMs;
    }

    long getOmCalls() {
      return omCalls;
    }

    long getXceiverNewCount() {
      return xceiverNewCount;
    }

    long getXceiverReuseCount() {
      return xceiverReuseCount;
    }

    int getLatencyMs() {
      return latencyMs;
    }

    double filesPerSec() {
      return wallMs == 0
          ? Double.POSITIVE_INFINITY : measuredFiles * 1000.0 / wallMs;
    }

    double omCallsPerFile() {
      return measuredFiles == 0 ? 0 : (double) omCalls / measuredFiles;
    }

    long cacheHitPercent() {
      long total = xceiverNewCount + xceiverReuseCount;
      return total == 0 ? 0 : xceiverReuseCount * 100 / total;
    }
  }

  // ---------------------------------------------------------------------------
  // Core runner
  // ---------------------------------------------------------------------------

  private static BenchmarkResult measure(int latencyMs, ECReplicationConfig repConfig,
      OmKeyInfo[] keyPool, ContainerProtos.ContainerCommandResponseProto blockResponse)
      throws Exception {
    AtomicLong omCallCount = new AtomicLong();
    CountingXceiverClientFactory xceiverFactory =
        new CountingXceiverClientFactory(blockResponse);
    AtomicLong fileIdx = new AtomicLong();

    OzoneManagerProtocol mockOm = mock(OzoneManagerProtocol.class);
    when(mockOm.lookupKey(any(OmKeyArgs.class))).thenAnswer(invocation -> {
      omCallCount.incrementAndGet();
      if (latencyMs > 0) {
        try {
          Thread.sleep(latencyMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException(ie);
        }
      }
      OmKeyArgs args = invocation.getArgument(0);
      int idx = Integer.parseInt(args.getKeyName().split("-")[1]);
      // Every UNSTABLE_STEP-th file returns a freshly built OmKeyInfo with new
      // random node UUIDs. Fix 2 computes its deterministic pipeline ID from
      // those node UUIDs, so the result also changes each call -> cache miss.
      if (idx % UNSTABLE_STEP == 0) {
        return buildRandomKeyInfo(args.getKeyName(), repConfig);
      }
      return keyPool[idx];
    });

    RpcClient mockRpcClient = mock(RpcClient.class);
    when(mockRpcClient.getOzoneManagerClient()).thenReturn(mockOm);
    when(mockRpcClient.getXceiverClientManager()).thenReturn(xceiverFactory);

    OzoneVolume mockVolume = mock(OzoneVolume.class);
    when(mockVolume.getName()).thenReturn("vol");
    OzoneBucket mockBucket = mock(OzoneBucket.class);
    when(mockBucket.getName()).thenReturn("bucket");

    OzoneClientConfig.ChecksumCombineMode combineMode =
        OzoneClientConfig.ChecksumCombineMode.MD5MD5CRC;

    Runnable task = () -> {
      try {
        int idx = (int) (fileIdx.getAndIncrement() % KEY_POOL);
        String keyName = "file-" + idx;
        OmKeyInfo keyInfo = mockRpcClient.getOzoneManagerClient().lookupKey(
            new OmKeyArgs.Builder()
                .setVolumeName("vol")
                .setBucketName("bucket")
                .setKeyName(keyName)
                .setSortDatanodesInPipeline(true)
                .setLatestVersionLocation(true)
                .build());
        new ECFileChecksumHelper(
            mockVolume, mockBucket, keyName, FILE_SIZE, combineMode,
            mockRpcClient, keyInfo)
            .compute();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };

    // Warmup: establish connections, fill JIT caches, discard counts.
    runFor(WARMUP_SECS * 1000L, task);
    omCallCount.set(0);
    xceiverFactory.resetCounters();
    fileIdx.set(0);

    // Measurement window.
    long start = System.currentTimeMillis();
    long measuredFiles = runFor(MEASURE_SECS * 1000L, task);
    long wallMs = System.currentTimeMillis() - start;

    return new BenchmarkResult(measuredFiles, wallMs, omCallCount.get(),
        xceiverFactory.getNewConnectionCount(), xceiverFactory.getReuseCount(),
        latencyMs);
  }

  private static long runFor(long durationMs, Runnable task) throws Exception {
    AtomicBoolean running = new AtomicBoolean(true);
    AtomicLong count = new AtomicLong();
    ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; i++) {
      futures.add(executor.submit((Callable<Void>) () -> {
        while (running.get()) {
          task.run();
          count.incrementAndGet();
        }
        return null;
      }));
    }
    Thread.sleep(durationMs);
    running.set(false);
    for (Future<?> f : futures) {
      f.get();
    }
    executor.shutdown();
    return count.get();
  }

  // ---------------------------------------------------------------------------
  // JUnit entry point
  // ---------------------------------------------------------------------------

  @Test
  public void runBenchmark() throws Exception {
    System.out.println();
    System.out.println("=== ECFileChecksum Collection Benchmark ===");
    System.out.printf("Workload: %d threads, %ds warmup + %ds measurement per config%n%n",
        NUM_THREADS, WARMUP_SECS, MEASURE_SECS);

    String header = String.format(
        "%-10s  %-10s  %-10s  %-10s  %-12s  %-10s  %-14s  %-14s  %-9s",
        "Latency", "Wall(ms)", "Files", "Files/s", "OM calls", "OM/file",
        "XcNew(conn)", "XcReuse", "CacheHit%");
    String rule = new String(new char[105]).replace('\0', '-');

    System.out.println("--- RS-3-2 (3 data + 2 parity, 5 nodes) ---");
    System.out.println(header);
    System.out.println(rule);
    for (int latencyMs : LATENCIES_MS) {
      printRow(measure(latencyMs, EC32, KEY_INFOS, GET_BLOCK_RESPONSE));
    }

    System.out.println();
    System.out.println("--- RS-6-3 (6 data + 3 parity, 9 nodes) ---");
    System.out.println(header);
    System.out.println(rule);
    for (int latencyMs : LATENCIES_MS) {
      printRow(measure(latencyMs, EC63, EC63_KEY_INFOS, EC63_GET_BLOCK_RESPONSE));
    }

  }

  private static void printRow(BenchmarkResult r) {
    System.out.printf(
        "%-10s  %-10d  %-10d  %-10.1f  %-12d  %-10.2f  %-14d  %-14d  %d%%%n",
        r.getLatencyMs() + "ms",
        r.getWallMs(),
        r.getMeasuredFiles(),
        r.filesPerSec(),
        r.getOmCalls(),
        r.omCallsPerFile(),
        r.getXceiverNewCount(),
        r.getXceiverReuseCount(),
        r.cacheHitPercent());
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static List<DatanodeDetails> buildEcNodes(int count) {
    List<DatanodeDetails> nodes = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      nodes.add(DatanodeDetails.newBuilder()
          .setUuid(UUID.fromString("00000000-0000-0000-0000-00000000000" + i))
          .setHostName("dn" + i)
          .setIpAddress("10.0.0." + i)
          .build());
    }
    return nodes;
  }

  private static Pipeline buildEcPipeline(List<DatanodeDetails> nodes,
      ECReplicationConfig repConfig) {
    Map<DatanodeDetails, Integer> replicaIndexes = new HashMap<>();
    for (int i = 0; i < nodes.size(); i++) {
      replicaIndexes.put(nodes.get(i), i + 1);
    }
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(repConfig)
        .setState(Pipeline.PipelineState.CLOSED)
        .setNodes(nodes)
        .setReplicaIndexes(replicaIndexes)
        .build();
  }

  private static OmKeyInfo[] buildKeyInfos(Pipeline pipeline,
      ECReplicationConfig repConfig) {
    OmKeyInfo[] infos = new OmKeyInfo[KEY_POOL];
    for (int i = 0; i < KEY_POOL; i++) {
      OmKeyLocationInfo loc = new OmKeyLocationInfo.Builder()
          .setBlockID(new BlockID(CONTAINER_ID, i))
          .setPipeline(pipeline)
          .setLength(FILE_SIZE)
          .build();
      infos[i] = new OmKeyInfo.Builder()
          .setVolumeName("vol")
          .setBucketName("bucket")
          .setKeyName("file-" + i)
          .setOmKeyLocationInfos(Collections.singletonList(
              new OmKeyLocationInfoGroup(0,
                  Collections.singletonList(loc))))
          .setCreationTime(0L)
          .setModificationTime(0L)
          .setDataSize(FILE_SIZE)
          .setReplicationConfig(repConfig)
          .setFileChecksum(null)
          .setAcls(Collections.emptyList())
          .build();
    }
    return infos;
  }

  private static ContainerProtos.ContainerCommandResponseProto buildGetBlockResponse(
      int stripeChecksumBytes) {
    ByteString fourBytes = ByteString.copyFrom(new byte[4]);
    ByteString stripeChecksum = ByteString.copyFrom(new byte[stripeChecksumBytes]);

    ContainerProtos.ChecksumData checksumData =
        ContainerProtos.ChecksumData.newBuilder()
            .setType(ContainerProtos.ChecksumType.CRC32)
            .setBytesPerChecksum(512 * 1024)
            .addChecksums(fourBytes)
            .build();

    ContainerProtos.ChunkInfo chunk = ContainerProtos.ChunkInfo.newBuilder()
        .setChunkName("chunk0")
        .setOffset(0)
        .setLen(FILE_SIZE)
        .setChecksumData(checksumData)
        .setStripeChecksum(stripeChecksum)
        .build();

    ContainerProtos.DatanodeBlockID dnBlockId =
        ContainerProtos.DatanodeBlockID.newBuilder()
            .setContainerID(CONTAINER_ID)
            .setLocalID(1)
            .setBlockCommitSequenceId(1)
            .build();

    ContainerProtos.BlockData blockData =
        ContainerProtos.BlockData.newBuilder()
            .setBlockID(dnBlockId)
            .addChunks(chunk)
            .build();

    ContainerProtos.GetBlockResponseProto getBlockResponse =
        ContainerProtos.GetBlockResponseProto.newBuilder()
            .setBlockData(blockData)
            .build();

    return ContainerProtos.ContainerCommandResponseProto.newBuilder()
        .setCmdType(ContainerProtos.Type.GetBlock)
        .setResult(ContainerProtos.Result.SUCCESS)
        .setGetBlock(getBlockResponse)
        .build();
  }

  /**
   * Builds an OmKeyInfo whose EC pipeline has freshly generated random node
   * UUIDs. Called on every lookupKey invocation for unstable files so the
   * deterministic pipeline ID computed by Fix 2 is also effectively random per
   * call, guaranteeing a cache miss.
   */
  private static OmKeyInfo buildRandomKeyInfo(String keyName,
      ECReplicationConfig repConfig) {
    int nodeCount = repConfig.getData() + repConfig.getParity();
    List<DatanodeDetails> nodes = new ArrayList<>();
    Map<DatanodeDetails, Integer> replicaIndexes = new HashMap<>();
    for (int i = 0; i < nodeCount; i++) {
      DatanodeDetails dn = DatanodeDetails.newBuilder()
          .setUuid(UUID.randomUUID())
          .setHostName("rdn" + i)
          .setIpAddress("10.1.0." + i)
          .build();
      nodes.add(dn);
      replicaIndexes.put(dn, i + 1);
    }
    Pipeline pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(repConfig)
        .setState(Pipeline.PipelineState.CLOSED)
        .setNodes(nodes)
        .setReplicaIndexes(replicaIndexes)
        .build();
    OmKeyLocationInfo loc = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(CONTAINER_ID, 0))
        .setPipeline(pipeline)
        .setLength(FILE_SIZE)
        .build();
    return new OmKeyInfo.Builder()
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setKeyName(keyName)
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0,
                Collections.singletonList(loc))))
        .setCreationTime(0L)
        .setModificationTime(0L)
        .setDataSize(FILE_SIZE)
        .setReplicationConfig(repConfig)
        .setFileChecksum(null)
        .setAcls(Collections.emptyList())
        .build();
  }

  private static XceiverClientSpi createMockDnClient(Pipeline standalonePipeline,
      ContainerProtos.ContainerCommandResponseProto response) throws IOException {
    XceiverClientSpi mockDn = mock(XceiverClientSpi.class, CALLS_REAL_METHODS);
    XceiverClientReply reply = new XceiverClientReply(
        CompletableFuture.completedFuture(response));
    try {
      doReturn(reply).when(mockDn).sendCommandAsync(any());
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException(e);
    }
    when(mockDn.getPipeline()).thenReturn(standalonePipeline);
    return mockDn;
  }
}
