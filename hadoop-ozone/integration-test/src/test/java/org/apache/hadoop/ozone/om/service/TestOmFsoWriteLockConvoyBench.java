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

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_MANAGER_FAIR_LOCK;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.snapshot.diff.SnapshotDiffValueParser;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark isolating the FSO create/commit write-lock reader convoy on one hot bucket (HDDS-16289).
 *
 * <p>Every OM write is applied on a single serial state-machine apply thread. On the FSO create and commit path,
 * {@code OMFileCreateRequestWithFSO.validateAndUpdateCache} / {@code OMKeyCommitRequestWithFSO.validateAndUpdateCache}
 * acquire the bucket <em>write</em> lock and then hold it across mostly read-of-committed-state work — chiefly
 * {@code verifyDirectoryKeysInPath}, one RocksDB point lookup per path segment. Because the OM bucket lock is a
 * non-fair {@link java.util.concurrent.locks.ReentrantReadWriteLock} (ozone.om.lock.fair=false), once the lone apply
 * thread queues for the write lock every arriving reader on that bucket blocks behind it, even though in-flight
 * readers drain in microseconds. On a hot bucket this freezes the read RPCs that take the bucket read lock
 * (getBucketInfo, getFileStatus, lookupKey) for the whole write-lock hold.
 *
 * <p>Unlike {@link TestOmMixedWorkloadUnderDeletionBench}, whose under-load driver is a deletion backlog and whose
 * client threads cycle a 1:1 read/write mix, this benchmark targets the convoy directly:
 * <ul>
 *   <li><b>one</b> FSO bucket (the convoy is a single-bucket, single-stripe phenomenon);</li>
 *   <li>the background deletion services pushed out of the window, so the only bucket write-lock holders under
 *       measurement are the client ops in {@link #WRITE_OPS} (create/commit, mkdir, rename, delete);</li>
 *   <li>a <b>deep, tunable</b> path ({@code bench.pathDepth}) so each create's under-lock path walk is expensive —
 *       the flat trees the sibling benchmark stages do not reproduce this cost;</li>
 *   <li>a large <b>reader</b> pool against a small <b>writer</b> pool ({@code bench.readerThreads} /
 *       {@code bench.writerThreads}), matching the observed ~100:1 waiting-reader-to-apply-thread ratio rather than
 *       coupling reads and writes on the same thread.</li>
 * </ul>
 * It measures read-RPC p50/p99 in two conditions — <b>control</b> (readers alone) and <b>under-load</b> (readers while
 * writers drive create/commit on the same bucket) — and reports the under-load degradation per read op. The
 * client-visible read p99 degradation is the latency face of the same convoy the jstack analysis counts as readers
 * blocked in {@code OzoneManagerLock.acquireLock}; an async-profiler {@code lock} recording of the under-load window
 * (see below) is the thread-level face. The fix (narrowing the write lock to the cache-mutation tail) should shrink
 * the read degradation and the lock-contention time without changing writer throughput.
 *
 * <p>The {@code benchmark} tag is excluded from {@code mvn test} and CI by default; run on demand (rebuild the reactor
 * first to avoid stale-class errors):
 * <pre>
 *   mvn -pl :ozone-integration-test test -DskipShade -DskipRecon \
 *     -Dtest=TestOmFsoWriteLockConvoyBench -Dgroups=benchmark -Dexcluded-test-groups= \
 *     -Dsurefire.failIfNoSpecifiedTests=false -Djunit.jupiter.execution.timeout.default=20m
 * </pre>
 *
 * <p>Tunables: {@code bench.readerThreads} (default 8), {@code bench.writerThreads} (default 4) — both sized to
 * avoid oversubscribing the host the mini-cluster shares,
 * {@code bench.pathDepth} (default 16 — directory levels the create walk resolves under the write lock),
 * {@code bench.windowSec} (default 150 — duration of each of the control and under-load passes, so an arm spends
 * 5 minutes measuring),
 * {@code bench.warmupFilesPerDir} (default 200 — files pre-staged in the read directory so read ops hit live paths).
 *
 * <p>Adding {@code -Dbench.profile.event=lock} profiles only the under-load window with async-profiler, loaded
 * reflectively from a local install supplied via {@code -Dbench.profiler.jar} and {@code -Dbench.profiler.lib}; the
 * JFR is written under {@code -Dbench.profile.out} (default {@code /tmp}). The sampling interval is
 * {@code -Dbench.profile.interval} (default {@code 1ms}; raise it for {@code wall}, which samples every live thread)
 * and {@code -Dbench.profile.opts} appends further async-profiler options. {@code lock} shows who waits for the lock,
 * {@code wall} on the {@code OMStateMachineApplyTransactionThread} shows what consumes the hold time;
 * {@code cpu}/{@code wall}/{@code alloc} also work. For accurate leaf frames also pass
 * {@code -DargLine="-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints"}.
 */
@Tag("benchmark")
public class TestOmFsoWriteLockConvoyBench {

  private static final Logger LOG = LoggerFactory.getLogger(TestOmFsoWriteLockConvoyBench.class);

  private static final String OP_GETBUCKETINFO = "getbucketinfo";
  private static final String OP_GETFILESTATUS = "getfilestatus";
  private static final String OP_LOOKUPKEY = "lookupkey";
  // The read ops each take the bucket READ lock; these are the callers that dominated the observed convoy.
  private static final String[] READ_OPS = {OP_GETBUCKETINFO, OP_GETFILESTATUS, OP_LOOKUPKEY};

  // The write ops the writers cycle, one per narrowed FSO apply path: create covers both the file create and the key
  // commit transaction, mkdir the directory create, rename the key rename.
  private static final String[] WRITE_OPS = {"create", "mkdir", "rename", "delete"};

  // One hot bucket. The deep read/write subtrees live under it so create walks and read resolutions hit the same
  // bucket stripe the convoy forms on.
  private static final String READ_ROOT = "convoy/read";
  private static final String WRITE_ROOT = "convoy/write";

  /**
   * Removes test-harness-only overhead that would distort the measured lock/apply cost: the mini-cluster enables
   * {@link CodecBuffer} leak detection (a per-allocation finalizer) and runs the CodecBuffer/managed-RocksDB loggers
   * at DEBUG/TRACE (a stack trace per allocation), neither of which a production OM at INFO does. JVM-global, so the
   * returned action restores them once the benchmark is done.
   */
  private static Runnable stripTestOnlyOverhead() {
    org.apache.log4j.Logger codecBufferLogger =
        org.apache.log4j.Logger.getLogger("org.apache.hadoop.hdds.utils.db.CodecBuffer");
    org.apache.log4j.Logger managedRocksLogger =
        org.apache.log4j.Logger.getLogger("org.apache.hadoop.hdds.utils.db.managed");
    org.apache.log4j.Level codecBufferLevel = codecBufferLogger.getLevel();
    org.apache.log4j.Level managedRocksLevel = managedRocksLogger.getLevel();

    CodecBuffer.disableLeakDetection();
    codecBufferLogger.setLevel(org.apache.log4j.Level.INFO);
    managedRocksLogger.setLevel(org.apache.log4j.Level.INFO);

    return () -> {
      CodecBuffer.enableLeakDetection();
      codecBufferLogger.setLevel(codecBufferLevel);
      managedRocksLogger.setLevel(managedRocksLevel);
    };
  }

  @Test
  // Two windows plus cluster start and deep pre-staging run well past the 5m
  // junit.jupiter.execution.timeout.default that pom.xml pins in surefire's <configurationParameters>, which the
  // JUnit platform resolves ahead of any -D system property. Only a method-level @Timeout overrides it.
  @Timeout(value = 30, unit = TimeUnit.MINUTES)
  public void benchmarkFsoWriteLockConvoy() throws Exception {
    final String profileEvent = System.getProperty("bench.profile.event", "");
    // Defaults sized for a laptop-class host: 8 readers + 4 writers leaves cores for the in-JVM mini-cluster (OM
    // handlers, Ratis, datanodes) instead of oversubscribing it, which adds scheduler latency to every sample and
    // shows up as noise in both arms. Raise bench.readerThreads on a machine with cores to spare.
    final int readerThreads = Integer.getInteger("bench.readerThreads", 8);
    final int writerThreads = Integer.getInteger("bench.writerThreads", 4);
    final int pathDepth = Integer.getInteger("bench.pathDepth", 16);
    // Per window; an arm runs the control and under-load windows back to back, so 150s = 5 minutes measured per arm.
    final int windowSec = Integer.getInteger("bench.windowSec", 150);
    final int warmupFilesPerDir = Integer.getInteger("bench.warmupFilesPerDir", 200);
    // Convoy-amplification levers (config/workload route). A mini-cluster cannot reach the production write-lock hold
    // duration (its DB is tiny and cache-warm), so instead of inflating the hold we amplify how much a reader suffers
    // per collision and shrink the pool headroom that absorbs one:
    //  - fairLock=true makes the non-fair->fair switch: once the apply thread is QUEUED for the bucket write lock,
    //    every arriving reader blocks behind it, so a convoy forms at far lower hold/occupancy. Run both modes.
    //  - a small OM read pool / handler count reproduces the blast radius (HDDS-16596): with little headroom a modest
    //    convoy exhausts the pool and read p99 explodes at lower absolute contention.
    final boolean fairLock = Boolean.getBoolean("bench.fairLock");
    final int omReadThreads = Integer.getInteger("bench.omReadThreads", 0);
    final int omHandlers = Integer.getInteger("bench.omHandlers", 0);

    OzoneConfiguration conf = new OzoneConfiguration();
    // Push both deletion services far past the window: the only bucket write-lock holder under measurement must be
    // FSO create/commit, never a purge transaction.
    conf.setTimeDuration(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 1, TimeUnit.HOURS);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 1, TimeUnit.HOURS);
    conf.setBoolean(OZONE_MANAGER_FAIR_LOCK, fairLock);
    if (omReadThreads > 0) {
      conf.setInt(OMConfigKeys.OZONE_OM_READ_THREADPOOL_KEY, omReadThreads);
    }
    if (omHandlers > 0) {
      conf.setInt(OMConfigKeys.OZONE_OM_HANDLER_COUNT_KEY, omHandlers);
    }

    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    Runnable restoreTestOnlyOverhead = stripTestOnlyOverhead();
    try {
      cluster.waitForClusterToBeReady();
      try (OzoneClient client = cluster.newClient()) {
        OzoneBucket bucket = org.apache.hadoop.ozone.DataTestUtil.createVolumeAndBucket(client,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);
        OzoneVolume volume = client.getObjectStore().getVolume(bucket.getVolumeName());
        final String bucketName = bucket.getName();
        FileSystem fs = rootedFs(conf, volume.getName(), bucketName);
        try {
          // Pre-stage a deep read directory the readers resolve against, so getFileStatus/lookupKey hit live paths
          // at full depth (their own path walk under the read lock), and the create walk below hits the same depth.
          Path readDir = deepDir(READ_ROOT, pathDepth);
          fs.mkdirs(readDir);
          for (int i = 0; i < warmupFilesPerDir; i++) {
            fs.create(new Path(readDir, "f" + i), true).close();
          }
          String readKeyName = READ_ROOT + depthSuffix(pathDepth) + "/f0";
          fs.mkdirs(deepDir(WRITE_ROOT, pathDepth));

          // Control: readers alone, no writer holding the bucket write lock.
          Percentiles[] control;
          try (RunningReaders readers = startReaders(fs, volume, bucketName, readKeyName, readerThreads)) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(windowSec));
            readers.stop();
            control = toPercentiles(readers.await(), "control");
          }

          Profiler profiler = profileEvent.isEmpty() ? null : Profiler.load();
          String profileOut = null;
          if (profiler != null) {
            profileOut = System.getProperty("bench.profile.out", "/tmp") + "/prof-convoy-" + profileEvent + ".jfr";
            profiler.start(profileEvent, profileOut);
          }

          // Under load: readers while writers drive create/commit at depth on the same bucket, so the apply thread
          // repeatedly takes the bucket write lock across the path walk and the readers queue behind it.
          Percentiles[] underLoad;
          long[] writerOps;
          try (RunningReaders readers = startReaders(fs, volume, bucketName, readKeyName, readerThreads);
               RunningWriters writers = startWriters(conf, volume.getName(), bucketName, pathDepth, writerThreads)) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(windowSec));
            writers.stop();
            readers.stop();
            if (profiler != null) {
              profiler.stop();
            }
            writerOps = writers.await();
            underLoad = toPercentiles(readers.await(), "under-load");
          }

          long writerTotal = 0;
          for (long ops : writerOps) {
            writerTotal += ops;
          }
          String header = String.format(Locale.ROOT,
              "BENCH convoy fairLock=%b omReadThreads=%d omHandlers=%d readerThreads=%d writerThreads=%d "
                  + "pathDepth=%d windowSec=%d writerOps=%d writerCreate=%d writerMkdir=%d writerRename=%d "
                  + "writerDelete=%d",
              fairLock, omReadThreads, omHandlers, readerThreads, writerThreads, pathDepth, windowSec,
              writerTotal, writerOps[0], writerOps[1], writerOps[2], writerOps[3]);
          printBenchLine(control, underLoad, header);
          if (profileOut != null) {
            System.out.printf(Locale.ROOT, "BENCH profile event=%s out=%s%n", profileEvent, profileOut);
          }
        } finally {
          org.apache.hadoop.io.IOUtils.closeStream(fs);
        }
      }
    } finally {
      try {
        cluster.shutdown();
      } finally {
        restoreTestOnlyOverhead.run();
      }
    }
  }

  /**
   * Micro-benchmark of the FSO path walk itself (HDDS-16289), with no writers and no lock contention in play. It
   * measures what it costs to resolve the parent objectIDs of a depth-{@code bench.pathDepth} path on a live OM --
   * the work {@code OMFileRequest.getOMKeyInfoIfExists} / {@code verifyDirectoryKeysInPath} / {@code getParentID} do
   * once per path segment, on the apply thread for every FSO write and on a handler thread for every
   * {@code getFileStatus} and {@code lookupKey}.
   *
   * <p>Both arms run in the same JVM against the same OM RocksDB and the same table cache, interleaved round by round
   * so neither JIT state nor thermal drift can favour one of them:
   * <ul>
   *   <li><b>full</b> -- {@code dirTable.get(key).getObjectID()}, which decodes the whole {@code DirectoryInfo} and
   *       rebuilds an {@code OmDirectoryInfo}, including the ACL list, the metadata map and the timestamps the walk
   *       then discards. This is what every segment cost before the change, and is still exactly what
   *       {@link Table#getProjected} falls back to by default.</li>
   *   <li><b>projected</b> -- {@code dirTable.getProjected(key, OmDirectoryInfo::getObjectID,
   *       SnapshotDiffValueParser::parseDirectoryInfoObjectId)}: the same cache semantics, but a value read from the
   *       store is decoded for its objectID alone, straight out of the pooled {@code CodecBuffer}.</li>
   * </ul>
   * {@link #resolveParentId} holds the one walk body both arms share, so they differ only in that single call.
   *
   * <p>Single-threaded on purpose: this isolates the per-operation cost of the change. Its effect on read latency and
   * throughput under concurrency is what {@link #benchmarkFsoWriteLockConvoy} measures.
   *
   * <p>The projection only pays on a table-cache miss -- on a hit both arms return the cached {@code OmDirectoryInfo}
   * and do identical work -- so the staged tree is first left to flush out of the OM double buffer
   * ({@code bench.walkSettleSec}), which is also the steady state a production OM serves reads from.
   *
   * <p>Tunables: {@code bench.pathDepth} (default 16), {@code bench.walkRounds} (default 6),
   * {@code bench.walkIterations} (default 20000 walks per arm per round), {@code bench.walkWarmup} (default 5000),
   * {@code bench.walkSettleSec} (default 10).
   */
  @Test
  @Timeout(value = 20, unit = TimeUnit.MINUTES)
  public void benchmarkFsoWalkProjection() throws Exception {
    final int pathDepth = Integer.getInteger("bench.pathDepth", 16);
    final int rounds = Integer.getInteger("bench.walkRounds", 6);
    final int iterations = Integer.getInteger("bench.walkIterations", 20000);
    final int warmupIterations = Integer.getInteger("bench.walkWarmup", 5000);
    final int settleSec = Integer.getInteger("bench.walkSettleSec", 10);

    OzoneConfiguration conf = new OzoneConfiguration();
    // Keep the deletion services out of the measurement window, as the convoy benchmark does.
    conf.setTimeDuration(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 1, TimeUnit.HOURS);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 1, TimeUnit.HOURS);

    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    Runnable restoreTestOnlyOverhead = stripTestOnlyOverhead();
    try {
      cluster.waitForClusterToBeReady();
      try (OzoneClient client = cluster.newClient()) {
        OzoneBucket bucket = org.apache.hadoop.ozone.DataTestUtil.createVolumeAndBucket(client,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);
        final String volumeName = bucket.getVolumeName();
        final String bucketName = bucket.getName();
        FileSystem fs = rootedFs(conf, volumeName, bucketName);
        try {
          fs.mkdirs(deepDir(READ_ROOT, pathDepth));
        } finally {
          org.apache.hadoop.io.IOUtils.closeStream(fs);
        }

        OMMetadataManager metadataManager = cluster.getOzoneManager().getMetadataManager();
        final long volumeId = metadataManager.getVolumeId(volumeName);
        final long bucketId = metadataManager.getBucketId(volumeName, bucketName);
        final String[] pathElements = pathElements(READ_ROOT, pathDepth);

        Thread.sleep(TimeUnit.SECONDS.toMillis(settleSec));
        int cacheResidentSegments =
            countCacheResidentSegments(metadataManager, volumeId, bucketId, pathElements);

        List<Long> full = new ArrayList<>(rounds * iterations);
        List<Long> projected = new ArrayList<>(rounds * iterations);
        runWalks(metadataManager, volumeId, bucketId, pathElements, false, warmupIterations, null);
        runWalks(metadataManager, volumeId, bucketId, pathElements, true, warmupIterations, null);
        for (int round = 0; round < rounds; round++) {
          // Alternate which arm goes first so neither always gets the warmer or the cooler slot of a round.
          boolean projectedFirst = (round & 1) == 1;
          runWalks(metadataManager, volumeId, bucketId, pathElements, projectedFirst, iterations,
              projectedFirst ? projected : full);
          runWalks(metadataManager, volumeId, bucketId, pathElements, !projectedFirst, iterations,
              projectedFirst ? full : projected);
        }
        printWalkBenchLine(pathDepth, rounds, iterations, settleSec, cacheResidentSegments,
            full, projected);
      }
    } finally {
      try {
        cluster.shutdown();
      } finally {
        restoreTestOnlyOverhead.run();
      }
    }
  }

  /** Runs {@code iterations} full path walks, recording one nanosecond sample per walk when {@code sink} is given. */
  private static void runWalks(OMMetadataManager metadataManager, long volumeId, long bucketId, String[] pathElements,
      boolean projected, int iterations, List<Long> sink) throws IOException {
    for (int i = 0; i < iterations; i++) {
      long start = System.nanoTime();
      long leafObjectId = resolveParentId(metadataManager, volumeId, bucketId, pathElements, projected);
      long elapsedNs = System.nanoTime() - start;
      if (leafObjectId == 0) {
        throw new IllegalStateException("path walk did not resolve; the staged directory tree is missing");
      }
      if (sink != null) {
        sink.add(elapsedNs);
      }
    }
  }

  /**
   * Resolves the objectID of the last of {@code pathElements} the way the FSO walks do: one dirTable lookup per
   * segment, each keyed by the objectID the previous segment resolved to. The {@code projected} arm is the read the
   * production walks do after HDDS-16289; the other is the full read it replaced. Returns 0 if the path does not
   * resolve.
   */
  private static long resolveParentId(OMMetadataManager metadataManager, long volumeId, long bucketId,
      String[] pathElements, boolean projected) throws IOException {
    Table<String, OmDirectoryInfo> dirTable = metadataManager.getDirectoryTable();
    long lastKnownParentId = bucketId;
    for (String pathElement : pathElements) {
      String dbNodeName = metadataManager.getOzonePathKey(volumeId, bucketId, lastKnownParentId, pathElement);
      final Long objectId;
      if (projected) {
        objectId = dirTable.getProjected(dbNodeName, OmDirectoryInfo::getObjectID,
            SnapshotDiffValueParser::parseDirectoryInfoObjectId);
      } else {
        OmDirectoryInfo omDirInfo = dirTable.get(dbNodeName);
        objectId = omDirInfo == null ? null : omDirInfo.getObjectID();
      }
      if (objectId == null) {
        return 0;
      }
      lastKnownParentId = objectId;
    }
    return lastKnownParentId;
  }

  /**
   * Counts how many of the walked segments are still resident in the dirTable cache. The projection only pays on a
   * cache miss, so a non-zero count means that many segments are being compared on the identical cache-hit path --
   * reported so that an equal-arms result is read as "the tree had not flushed yet", not as "the change does nothing".
   */
  private static int countCacheResidentSegments(OMMetadataManager metadataManager, long volumeId, long bucketId,
      String[] pathElements) throws IOException {
    Table<String, OmDirectoryInfo> dirTable = metadataManager.getDirectoryTable();
    long lastKnownParentId = bucketId;
    int resident = 0;
    for (String pathElement : pathElements) {
      String dbNodeName = metadataManager.getOzonePathKey(volumeId, bucketId, lastKnownParentId, pathElement);
      CacheValue<OmDirectoryInfo> cached = dirTable.getCacheValue(new CacheKey<>(dbNodeName));
      if (cached != null && cached.getCacheValue() != null) {
        resident++;
      }
      OmDirectoryInfo omDirInfo = dirTable.get(dbNodeName);
      if (omDirInfo == null) {
        break;
      }
      lastKnownParentId = omDirInfo.getObjectID();
    }
    return resident;
  }

  /** The path elements of {@code root} followed by {@code depth} nested directories, in walk order. */
  private static String[] pathElements(String root, int depth) {
    List<String> elements = new ArrayList<>(Arrays.asList(root.split("/")));
    for (int d = 0; d < depth; d++) {
      elements.add("d" + d);
    }
    return elements.toArray(new String[0]);
  }

  /** Prints the {@code BENCH walk} lines: per-walk and per-segment cost of both arms, plus the projected/full ratio. */
  private static void printWalkBenchLine(int pathDepth, int rounds, int iterations, int settleSec,
      int cacheResidentSegments, List<Long> full, List<Long> projected) {
    final int segments = pathDepth + READ_ROOT.split("/").length;
    Percentiles fullPercentiles = Percentiles.of(full);
    Percentiles projectedPercentiles = Percentiles.of(projected);
    double fullMeanNs = meanNs(full);
    double projectedMeanNs = meanNs(projected);
    System.out.printf(Locale.ROOT,
        "BENCH walk pathDepth=%d segments=%d rounds=%d iterationsPerRound=%d settleSec=%d cacheResidentSegments=%d%n"
            + "BENCH walk full      n=%d meanUs=%.2f p50Us=%.2f p90Us=%.2f p99Us=%.2f perSegmentNs=%.0f%n"
            + "BENCH walk projected n=%d meanUs=%.2f p50Us=%.2f p90Us=%.2f p99Us=%.2f perSegmentNs=%.0f%n"
            + "BENCH walk delta meanPct=%+.1f%% p50Pct=%+.1f%% p99Pct=%+.1f%% speedup=%.2fx%n",
        pathDepth, segments, rounds, iterations, settleSec, cacheResidentSegments,
        fullPercentiles.count, fullMeanNs / 1000.0, fullPercentiles.p50 * 1000.0, fullPercentiles.p90 * 1000.0,
        fullPercentiles.p99 * 1000.0, fullMeanNs / segments,
        projectedPercentiles.count, projectedMeanNs / 1000.0, projectedPercentiles.p50 * 1000.0,
        projectedPercentiles.p90 * 1000.0, projectedPercentiles.p99 * 1000.0, projectedMeanNs / segments,
        100.0 * (projectedMeanNs - fullMeanNs) / fullMeanNs,
        100.0 * (projectedPercentiles.p50 - fullPercentiles.p50) / fullPercentiles.p50,
        100.0 * (projectedPercentiles.p99 - fullPercentiles.p99) / fullPercentiles.p99,
        safeRatio(fullMeanNs, projectedMeanNs));
  }

  private static double meanNs(List<Long> samplesNs) {
    if (samplesNs.isEmpty()) {
      return Double.NaN;
    }
    long total = 0;
    for (long sample : samplesNs) {
      total += sample;
    }
    return (double) total / samplesNs.size();
  }

  /** One {@code o3fs} FileSystem rooted at the given bucket. */
  private static FileSystem rootedFs(OzoneConfiguration conf, String volumeName, String bucketName)
      throws IOException {
    OzoneConfiguration bucketConf = new OzoneConfiguration(conf);
    bucketConf.set(FS_DEFAULT_NAME_KEY,
        String.format("%s://%s.%s/", OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName));
    return FileSystem.get(bucketConf);
  }

  private static Path deepDir(String root, int depth) {
    return new Path("/" + root + depthSuffix(depth));
  }

  private static String depthSuffix(int depth) {
    StringBuilder sb = new StringBuilder();
    for (int d = 0; d < depth; d++) {
      sb.append("/d").append(d);
    }
    return sb.toString();
  }

  /**
   * Starts {@code threads} reader threads that loop the {@link #READ_OPS} mix on the hot bucket until stopped,
   * recording per-op nanosecond samples. getBucketInfo and lookupKey go through the client (bucket read lock);
   * getFileStatus resolves the pre-staged deep path.
   */
  private RunningReaders startReaders(FileSystem fs, OzoneVolume volume, String bucketName, String readKeyName,
      int threads) {
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch startLatch = new CountDownLatch(1);
    AtomicBoolean running = new AtomicBoolean(true);
    AtomicBoolean failed = new AtomicBoolean(false);
    List<Future<List<long[]>>> futures = new ArrayList<>(threads);
    Path readFile = new Path("/" + readKeyName);
    for (int t = 0; t < threads; t++) {
      futures.add(pool.submit((Callable<List<long[]>>) () -> {
        List<long[]> samples = new ArrayList<>(1 << 16);
        startLatch.await();
        for (int i = 0; running.get(); i++) {
          int opIdx = i % READ_OPS.length;
          long t0 = System.nanoTime();
          try {
            switch (opIdx) {
            case 0:
              volume.getBucket(bucketName);
              break;
            case 1:
              fs.getFileStatus(readFile);
              break;
            default:
              volume.getBucket(bucketName).getKey(readKeyName);
              break;
            }
          } catch (IOException | RuntimeException e) {
            failed.set(true);
            throw e;
          }
          samples.add(new long[] {opIdx, System.nanoTime() - t0});
        }
        return samples;
      }));
    }
    startLatch.countDown();
    return new RunningReaders(pool, futures, running, failed);
  }

  /**
   * Starts {@code threads} writer threads that cycle the {@link #WRITE_OPS} mix at {@code pathDepth} into a per-thread
   * deep subtree until stopped, forcing the apply thread to hold the bucket write lock across a
   * depth-{@code pathDepth} path walk on every op. All five narrowed FSO apply paths are driven: create covers the
   * file create and the key commit, mkdir the directory create, rename the key rename, delete the key delete.
   * Returns the per-op counts
   * (index-aligned with {@link #WRITE_OPS}). Each writer uses its own FileSystem so the client side does not
   * serialize.
   */
  private RunningWriters startWriters(OzoneConfiguration conf, String volumeName, String bucketName, int pathDepth,
      int threads) throws IOException {
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch startLatch = new CountDownLatch(1);
    AtomicBoolean running = new AtomicBoolean(true);
    AtomicLong[] opCounts = new AtomicLong[WRITE_OPS.length];
    for (int op = 0; op < opCounts.length; op++) {
      opCounts[op] = new AtomicLong();
    }
    List<FileSystem> writerFs = new ArrayList<>(threads);
    List<Future<?>> futures = new ArrayList<>(threads);
    for (int t = 0; t < threads; t++) {
      final int writerId = t;
      FileSystem fs = rootedFs(conf, volumeName, bucketName);
      writerFs.add(fs);
      Path writerDir = new Path(deepDir(WRITE_ROOT, pathDepth), "w" + writerId);
      fs.mkdirs(writerDir);
      futures.add(pool.submit(() -> {
        long[] ops = new long[WRITE_OPS.length];
        Path lastCreated = null;
        Path lastDir = null;
        startLatch.await();
        for (int i = 0; running.get(); i++) {
          int opIdx = i % WRITE_OPS.length;
          if (opIdx == 1) {
            lastDir = new Path(writerDir, "d" + i);
            fs.mkdirs(lastDir);
            ops[1]++;
          } else if (opIdx == 2 && lastCreated != null) {
            // Rename the file this thread created on its previous create op.
            fs.rename(lastCreated, new Path(writerDir, "f" + i + "-r"));
            lastCreated = null;
            ops[2]++;
          } else if (opIdx == 3 && lastDir != null) {
            // Delete the (empty) directory this thread made on its previous mkdir op, non-recursively. On an FSO
            // bucket BasicRootedOzoneFileSystem sends that as a single DeleteKey with recursive=false, so the apply
            // path runs OMFileRequest.hasChildren -- a full scan of the dirTable and fileTable caches plus two
            // RocksDB seeks, the largest of the narrowed write-lock holds. Deleting a file instead would skip
            // hasChildren and add a createFakeParentDirectory round trip, so the directory case is both the
            // expensive one and the cleaner one to measure.
            fs.delete(lastDir, false);
            lastDir = null;
            ops[3]++;
          } else {
            // Create (and commit) a file; also the fallback when no file is pending, so the cycle never renames a
            // missing path.
            lastCreated = new Path(writerDir, "f" + i);
            fs.create(lastCreated, true).close();
            ops[0]++;
          }
        }
        for (int op = 0; op < ops.length; op++) {
          opCounts[op].addAndGet(ops[op]);
        }
        return null;
      }));
    }
    startLatch.countDown();
    return new RunningWriters(pool, futures, running, opCounts, writerFs);
  }

  /** Handle to the reader pool: stop, then join and collect per-op samples. */
  private static final class RunningReaders implements AutoCloseable {
    private final ExecutorService pool;
    private final List<Future<List<long[]>>> futures;
    private final AtomicBoolean running;
    private final AtomicBoolean failed;

    RunningReaders(ExecutorService pool, List<Future<List<long[]>>> futures, AtomicBoolean running,
        AtomicBoolean failed) {
      this.pool = pool;
      this.futures = futures;
      this.running = running;
      this.failed = failed;
    }

    void stop() {
      running.set(false);
    }

    @Override
    public void close() {
      stop();
      HadoopExecutors.shutdown(pool, LOG, 60, TimeUnit.SECONDS);
    }

    List<List<Long>> await() throws Exception {
      List<List<Long>> byOp = new ArrayList<>(READ_OPS.length);
      for (String ignored : READ_OPS) {
        byOp.add(new ArrayList<>());
      }
      for (Future<List<long[]>> future : futures) {
        for (long[] sample : future.get()) {
          byOp.get((int) sample[0]).add(sample[1]);
        }
      }
      if (failed.get()) {
        throw new IllegalStateException("reader thread failed");
      }
      return byOp;
    }
  }

  /** Handle to the writer pool: stop, then join and return the per-op counts. Closes the writer FileSystems. */
  private static final class RunningWriters implements AutoCloseable {
    private final ExecutorService pool;
    private final List<Future<?>> futures;
    private final AtomicBoolean running;
    private final AtomicLong[] opCounts;
    private final List<FileSystem> writerFs;

    RunningWriters(ExecutorService pool, List<Future<?>> futures, AtomicBoolean running, AtomicLong[] opCounts,
        List<FileSystem> writerFs) {
      this.pool = pool;
      this.futures = futures;
      this.running = running;
      this.opCounts = opCounts;
      this.writerFs = writerFs;
    }

    void stop() {
      running.set(false);
    }

    long[] await() throws Exception {
      for (Future<?> future : futures) {
        future.get();
      }
      long[] counts = new long[opCounts.length];
      for (int op = 0; op < counts.length; op++) {
        counts[op] = opCounts[op].get();
      }
      return counts;
    }

    @Override
    public void close() {
      stop();
      HadoopExecutors.shutdown(pool, LOG, 120, TimeUnit.SECONDS);
      for (FileSystem fs : writerFs) {
        org.apache.hadoop.io.IOUtils.closeStream(fs);
      }
    }
  }

  private static double safeRatio(double num, double den) {
    return den <= 0 ? Double.NaN : num / den;
  }

  /** Reduces per-op nanosecond samples to {@link Percentiles} (index-aligned with {@link #READ_OPS}). */
  private static Percentiles[] toPercentiles(List<List<Long>> byOp, String label) {
    Percentiles[] result = new Percentiles[READ_OPS.length];
    for (int op = 0; op < READ_OPS.length; op++) {
      result[op] = Percentiles.of(byOp.get(op));
      LOG.info("{} {}: ops={} p50Ms={} p99Ms={}", label, READ_OPS[op], byOp.get(op).size(),
          result[op].p50, result[op].p99);
    }
    return result;
  }

  /** Appends per-op control/under-load percentiles and degradation ratios and prints the {@code BENCH convoy} line. */
  private static void printBenchLine(Percentiles[] control, Percentiles[] underLoad, String header) {
    StringBuilder line = new StringBuilder(header);
    for (int op = 0; op < READ_OPS.length; op++) {
      line.append(String.format(Locale.ROOT, " %s[controlN=%d underLoadN=%d "
              + "control_p50=%.3f control_p90=%.3f control_p95=%.3f control_p99=%.3f "
              + "underLoad_p50=%.3f underLoad_p90=%.3f underLoad_p95=%.3f underLoad_p99=%.3f "
              + "deg50=%.2fx deg90=%.2fx deg95=%.2fx deg99=%.2fx]",
          READ_OPS[op], control[op].count, underLoad[op].count,
          control[op].p50, control[op].p90, control[op].p95, control[op].p99,
          underLoad[op].p50, underLoad[op].p90, underLoad[op].p95, underLoad[op].p99,
          safeRatio(underLoad[op].p50, control[op].p50), safeRatio(underLoad[op].p90, control[op].p90),
          safeRatio(underLoad[op].p95, control[op].p95), safeRatio(underLoad[op].p99, control[op].p99)));
    }
    System.out.println(line);
  }

  /**
   * Thin reflective wrapper over async-profiler's {@code one.profiler.AsyncProfiler}, loaded at runtime from the
   * configured jar so the benchmark carries no compile-time dependency on async-profiler.
   */
  private static final class Profiler {
    private final Object delegate;
    private final Method execute;

    private Profiler(Object delegate, Method execute) {
      this.delegate = delegate;
      this.execute = execute;
    }

    static Profiler load() throws Exception {
      String jar = requireProfilerPath("bench.profiler.jar");
      String lib = requireProfilerPath("bench.profiler.lib");
      try {
        return AccessController.doPrivileged((PrivilegedExceptionAction<Profiler>) () -> {
          URLClassLoader loader = new URLClassLoader(new URL[] {new File(jar).toURI().toURL()},
              Profiler.class.getClassLoader());
          Class<?> clazz = Class.forName("one.profiler.AsyncProfiler", true, loader);
          Object instance = clazz.getMethod("getInstance", String.class).invoke(null, lib);
          return new Profiler(instance, clazz.getMethod("execute", String.class));
        });
      } catch (PrivilegedActionException e) {
        throw (Exception) e.getCause();
      }
    }

    private static String requireProfilerPath(String property) {
      String value = System.getProperty(property);
      if (value == null || value.isEmpty()) {
        throw new IllegalStateException("bench.profile.event is set but " + property + " is not; point it at your "
            + "local async-profiler install (jar and native library) to enable profiling");
      }
      return value;
    }

    void start(String event, String jfrFile) throws Exception {
      String interval = System.getProperty("bench.profile.interval", "1ms");
      String extra = System.getProperty("bench.profile.opts", "");
      execute.invoke(delegate, String.format(Locale.ROOT, "start,event=%s,interval=%s%s,file=%s",
          event, interval, extra.isEmpty() ? "" : "," + extra, jfrFile));
    }

    void stop() throws Exception {
      execute.invoke(delegate, "stop");
    }
  }

  /**
   * p50/p90/p95/p99 in milliseconds over a set of nanosecond latency samples, plus the sample count. The write lock
   * is held for only a small fraction of the window, so a reader stall shows up in the upper percentiles while p50
   * stays flat; p90 and p95 are reported so that shape is visible rather than inferred from p99 alone.
   */
  private static final class Percentiles {
    private final int count;
    private final double p50;
    private final double p90;
    private final double p95;
    private final double p99;

    private Percentiles(int count, double p50, double p90, double p95, double p99) {
      this.count = count;
      this.p50 = p50;
      this.p90 = p90;
      this.p95 = p95;
      this.p99 = p99;
    }

    static Percentiles of(List<Long> samplesNs) {
      if (samplesNs.isEmpty()) {
        return new Percentiles(0, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
      }
      long[] sorted = samplesNs.stream().mapToLong(Long::longValue).sorted().toArray();
      return new Percentiles(sorted.length, quantile(sorted, 0.50), quantile(sorted, 0.90),
          quantile(sorted, 0.95), quantile(sorted, 0.99));
    }

    private static double quantile(long[] sorted, double q) {
      return sorted[Math.min(sorted.length - 1, (int) (sorted.length * q))] / 1_000_000.0;
    }
  }
}
