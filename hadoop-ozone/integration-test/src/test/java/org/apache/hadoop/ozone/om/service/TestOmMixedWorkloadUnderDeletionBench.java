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
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end benchmark reproducing the interactive-workload degradation seen when a large background deletion backlog
 * is being reclaimed on the same bucket that clients are actively writing to.
 *
 * <p>Every OM write is applied on a single serial state-machine apply thread under a per-bucket write lock. Reclaiming
 * a deletion backlog drives {@code PurgeDirectories} transactions through that thread; when the deleted directories are
 * densely populated, a single purge batch moves a large number of sub-files/sub-dirs under one write-lock hold, so the
 * apply thread — and the bucket write lock — is occupied for the whole batch. Concurrent user {@code create},
 * {@code mkdir} and {@code rename} contend for that same lock and thread, and reads contend for the bucket read lock,
 * so their latency degrades while the backlog drains.
 *
 * <p>This benchmark stages two sets under a single volume and FSO bucket: a densely-populated <em>backlog</em> subtree
 * that is recursively deleted and drained, and a separate stable <em>workload</em> set that is never deleted during the
 * run (a pre-staged dataset the read ops resolve against, plus a per-thread scratch area the write ops create into).
 * Sharing the bucket is intentional — deletion and the client workload contend on the same bucket lock, while the
 * workload always hits live paths. It then compares a mixed client workload on that bucket — create, mkdir, rename and
 * a data-plane file write (create + write a block), plus a data-plane file read and the metadata read RPCs that take
 * the bucket read lock (getFileStatus, listStatus, getBucketInfo, lookupKey) — in two conditions:
 * <ul>
 *   <li><b>control</b> — no deletion running, and</li>
 *   <li><b>under load</b> — the same workload while the backlog subtree is recursively deleted and fully purged from
 *       OM in the background (both FSO phases: moved into the deletedTable, then purged back out),</li>
 * </ul>
 * reporting per-operation p50/p99 latency and the under-load degradation, so the two code versions can be compared on
 * how much apply-thread purge work bleeds into interactive latency. It also reports the phase-1 drain time — the
 * {@code DirectoryDeletingService} move into the deletedTable via {@code OMDirectoriesPurgeRequestWithFSO}, the apply
 * path this change optimizes — separately from the full both-phase drain, so pure apply-thread throughput can be
 * compared alongside the interactive degradation.
 *
 * <p>Deletion is configured with production-representative per-task limits so batches are large. The {@code benchmark}
 * tag is excluded from {@code mvn test} and CI by default, so it must be re-enabled explicitly to run on demand
 * (rebuild the reactor first to avoid stale-class errors):
 * <pre>
 *   mvn -pl :ozone-integration-test test -DskipShade -DskipRecon \
 *     -Dtest=TestOmMixedWorkloadUnderDeletionBench -Dgroups=benchmark -Dexcluded-test-groups= \
 *     -Dsurefire.failIfNoSpecifiedTests=false
 * </pre>
 * Tunables: {@code bench.backlogDirs} (default 80), {@code bench.backlogFilesPerDir} (default 1000),
 * {@code bench.backlogNonEmptyEvery} (default 3 — every 3rd backlog file is written with a block, the rest are
 * empty so a large backlog stays cheap to stage), {@code bench.workloadDirs} (default 20) and
 * {@code bench.workloadFilesPerDir} (default 100) sizing the stable dataset the read ops resolve against,
 * {@code bench.fileBytes} (default 1 MiB) sizing the data-plane file write/read payload (and the block-bearing
 * staged files the reads pull), {@code bench.clientThreads} (default 4),
 * {@code bench.opsPerThread} (default 400), {@code bench.pathDeletingLimitPerTask} (default 2000) and
 * {@code bench.keyDeletingLimitPerTask} (default 40000). The last two size how much a single deletion round gathers;
 * with the Ratis appender byte limit non-binding at these entry sizes, a round's paths pack into one purge
 * transaction, so raising them makes each apply move far more entries under a single bucket write-lock hold — the
 * regime where the apply-thread per-entry cost dominates interactive latency.
 *
 * <p>Adding {@code -Dbench.profile.event=<cpu|lock|wall|alloc>} profiles only the under-load window with
 * async-profiler, loaded reflectively from a local install whose paths must be supplied via
 * {@code -Dbench.profiler.jar} (the async-profiler jar) and {@code -Dbench.profiler.lib} (its native library); the
 * JFR is written under {@code -Dbench.profile.out}, default {@code /tmp}. For accurate leaf frames also pass
 * {@code -DargLine="-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints"}.
 */
@Tag("benchmark")
public class TestOmMixedWorkloadUnderDeletionBench {

  private static final Logger LOG = LoggerFactory.getLogger(TestOmMixedWorkloadUnderDeletionBench.class);

  private static final String OP_CREATE = "create";
  private static final String OP_MKDIR = "mkdir";
  private static final String OP_RENAME = "rename";
  private static final String OP_FILEWRITE = "filewrite";
  private static final String OP_FILEREAD = "fileread";
  private static final String OP_GETFILESTATUS = "getfilestatus";
  private static final String OP_LISTSTATUS = "liststatus";
  private static final String OP_INFOBUCKET = "infobucket";
  private static final String OP_GETKEYINFO = "getkeyinfo";
  private static final String[] OPS =
      {OP_CREATE, OP_MKDIR, OP_RENAME, OP_FILEWRITE, OP_FILEREAD,
       OP_GETFILESTATUS, OP_LISTSTATUS, OP_INFOBUCKET, OP_GETKEYINFO};

  // The three sandboxes share the parent /workload/bucket but are separate subtrees; only the backlog is deleted.
  // The deletion set (built, recursively deleted, then drained through the apply thread).
  private static final String BACKLOG_ROOT = "workload/bucket/backlog";
  // The workload set — never deleted during the test: a stable pre-staged dataset the read ops resolve against, plus
  // a scratch area the write ops create into. Keeping this separate from the deletion set means the concurrent
  // operations always hit live paths while the backlog drains.
  private static final String WORKLOAD_DATA_ROOT = "workload/bucket/data";
  private static final String WORKLOAD_SCRATCH_ROOT = "workload/bucket/scratch";

  // Every bench.backlogNonEmptyEvery-th backlog file is written with a single block so its KeyInfo carries a
  // key-location list, exercising the block-metadata parse/serialize the purge apply and flush paths hit in
  // production. The rest are left empty because block-bearing files are far more expensive to stage (block
  // allocation + datanode write + commit), and a large backlog is what actually stresses the apply thread.
  private static final byte[] FILE_CONTENT = new byte[4];

  /**
   * Removes test-harness-only overhead that would otherwise distort the apply/flush cost under measurement: the
   * mini-cluster unconditionally enables {@link CodecBuffer} leak detection (a per-allocation finalizer), and the test
   * log config runs the {@code CodecBuffer}/managed-RocksDB loggers at DEBUG/TRACE, which capture a full stack trace on
   * every buffer allocation. Neither happens in a production OM running at INFO.
   */
  private static void stripTestOnlyOverhead() {
    CodecBuffer.disableLeakDetection();
    org.apache.log4j.Logger.getLogger("org.apache.hadoop.hdds.utils.db.CodecBuffer")
        .setLevel(org.apache.log4j.Level.INFO);
    org.apache.log4j.Logger.getLogger("org.apache.hadoop.hdds.utils.db.managed")
        .setLevel(org.apache.log4j.Level.INFO);
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.MINUTES)
  public void benchmarkMixedWorkloadUnderDeletionLoad() throws Exception {
    final String profileEvent = System.getProperty("bench.profile.event", "");

    // Number of FSO buckets (in one volume) the backlog and workload are spread across. With more than one bucket a
    // background purge round gathers deleted dirs from several buckets, so an ungrouped DirectoryDeletingService packs
    // multiple buckets into one purge transaction and the apply path holds all their write locks together; per-bucket
    // grouping keeps each transaction single-bucket. backlogDirs below is the TOTAL across buckets, split evenly.
    final int numBuckets = Integer.getInteger("bench.numBuckets", 4);
    final int backlogDirs = Integer.getInteger("bench.backlogDirs", 80);
    final int backlogFilesPerDir = Integer.getInteger("bench.backlogFilesPerDir", 1000);
    final int nonEmptyEvery = Integer.getInteger("bench.backlogNonEmptyEvery", 3);
    // Size of the stable workload dataset the read ops resolve against (never deleted during the test).
    final int workloadDirs = Integer.getInteger("bench.workloadDirs", 20);
    final int workloadFilesPerDir = Integer.getInteger("bench.workloadFilesPerDir", 100);
    // Payload for the data-plane write/read ops and the block-bearing staged workload files those reads hit.
    final int fileBytes = Integer.getInteger("bench.fileBytes", 1024 * 1024);
    final int clientThreads = Integer.getInteger("bench.clientThreads", 4);
    final int opsPerThread = Integer.getInteger("bench.opsPerThread", 400);
    final int pathDeletingLimit = Integer.getInteger("bench.pathDeletingLimitPerTask", 2000);
    final int keyDeletingLimit = Integer.getInteger("bench.keyDeletingLimitPerTask", 40000);
    // Phase-1 (DirectoryDeletingService) cadence. The interval is read in MILLISECONDS (KeyManagerImpl), so 1000
    // gives a genuine 1s gate between purge rounds: each round moves up to pathDeletingLimit paths under one bucket
    // write lock — the apply path this change optimizes — and gating spreads phase 1 into a long, sampled window.
    final int dirDeletingIntervalMs = Integer.getInteger("bench.dirDeletingIntervalMs", 1000);
    // Phase-2 (KeyDeletingService) is unchanged by this optimization; push its interval past the phase-1 window so it
    // does not run during measurement and only phase-1 contention is sampled.
    final int blockDeletingIntervalSec = Integer.getInteger("bench.blockDeletingIntervalSec", 600);
    final String ratisAppenderByteLimit = System.getProperty("bench.ratisAppenderByteLimit",
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT);
    final int perBucketBacklogDirs = Math.max(1, backlogDirs / numBuckets);
    final int backlogFiles = numBuckets * perBucketBacklogDirs * backlogFilesPerDir;

    OzoneConfiguration conf = new OzoneConfiguration();
    // Gate phase 1 (DirectoryDeletingService) at a genuine 1s interval so it moves pathDeletingLimit paths per round
    // under one apply-thread bucket write-lock hold — the path this change optimizes — spreading phase 1 into a long,
    // sampled window while the client workload runs. Phase 2 (KeyDeletingService) is unchanged by the optimization,
    // so its interval is pushed past the window to keep it out of measurement.
    conf.setInt(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, dirDeletingIntervalMs);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, blockDeletingIntervalSec, TimeUnit.SECONDS);
    conf.setInt(OMConfigKeys.OZONE_PATH_DELETING_LIMIT_PER_TASK, pathDeletingLimit);
    conf.setInt(OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK, keyDeletingLimit);
    conf.set(OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT, ratisAppenderByteLimit);
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 1000);

    stripTestOnlyOverhead();
    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    try {
      cluster.waitForClusterToBeReady();
      DirectoryDeletingService dds = cluster.getOzoneManager().getKeyManager().getDirDeletingService();
      OMMetadataManager mm = cluster.getOzoneManager().getMetadataManager();

      try (OzoneClient client = cluster.newClient()) {
        BucketFs env = createBuckets(client, conf, numBuckets);
        try {
          // Stage the deletion set (recursively deleted and drained below) and the stable workload set — a dataset the
          // read ops resolve against plus a per-thread scratch area the write ops create into — in every bucket, so the
          // workload always hits live paths in the same buckets the purge is draining. Backlog files carry only a tiny
          // block (one key-location for the purge to parse); workload files carry the full fileBytes payload.
          byte[] fileContent = new byte[fileBytes];
          for (int b = 0; b < numBuckets; b++) {
            buildDenseTree(env.fs[b], new Path("/" + BACKLOG_ROOT), perBucketBacklogDirs, backlogFilesPerDir,
                nonEmptyEvery, FILE_CONTENT);
            buildDenseTree(env.fs[b], new Path("/" + WORKLOAD_DATA_ROOT), workloadDirs, workloadFilesPerDir,
                nonEmptyEvery, fileContent);
            env.fs[b].mkdirs(new Path("/" + WORKLOAD_SCRATCH_ROOT));
          }
          WorkloadDataset dataset = new WorkloadDataset(workloadDirs, workloadFilesPerDir, nonEmptyEvery, fileContent);

          // Control: interactive latency on the hot buckets with no deletion running.
          Percentiles[] control = toPercentiles(
              startMixedWorkload(env.fs, env.volume, env.buckets, dataset, clientThreads, opsPerThread, "control")
                  .await(), "control");

          // Profile only the under-load window when -Dbench.profile.event=<cpu|lock|wall|alloc> is set. async-profiler
          // is loaded reflectively from -Dbench.profiler.jar / -Dbench.profiler.lib and a JFR recording is written to
          // -Dbench.profile.out (default /tmp). For accurate leaf frames add
          // -DargLine="-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints".
          Profiler profiler = profileEvent.isEmpty() ? null : Profiler.load();
          String profileOut = null;
          if (profiler != null) {
            profileOut = System.getProperty("bench.profile.out", "/tmp") + "/prof-mixed-" + profileEvent + ".jfr";
            profiler.start(profileEvent, profileOut);
          }

          // Run the interactive workload continuously in background threads, sampling throughout phase 1, and stop it
          // the moment phase 1 completes so the under-load percentiles reflect only the optimized, contended window.
          // Phase 1 (DirectoryDeletingService moving sub-files/sub-dirs into the deletedTable) takes the bucket write
          // lock on the apply thread — the path this change optimizes; every bucket's backlog is deleted here so purge
          // rounds span buckets. Phase 2 (KeyDeletingService draining the deletedTable) is unchanged by the change and
          // is kept out of the window by a long block-deleting interval, so it is neither sampled nor waited on.
          Table<String, ?> deletedDirTable = mm.getDeletedDirTable();
          long movedFilesBefore = dds.getMovedFilesCount();
          long start = System.nanoTime();
          long drainDeadline = start + TimeUnit.SECONDS.toNanos(900);
          RunningWorkload underLoadWl = startMixedWorkload(env.fs, env.volume, env.buckets, dataset, clientThreads, 0,
              "under-load");
          for (int b = 0; b < numBuckets; b++) {
            env.fs[b].delete(new Path("/" + BACKLOG_ROOT), true);
          }
          LOG.info("delete(recursive) issued on {} buckets; backlog draining in background", numBuckets);

          long phase1DrainNanos;
          try {
            phase1DrainNanos = awaitPhase1Drain(dds, mm, deletedDirTable, movedFilesBefore, backlogFiles,
                underLoadWl, start, drainDeadline);
          } finally {
            underLoadWl.stop();
            if (profiler != null) {
              profiler.stop();
            }
          }
          double phase1DrainMs = phase1DrainNanos / 1_000_000.0;
          List<List<Long>> underLoadSamples = underLoadWl.await();
          Percentiles[] underLoad = toPercentiles(underLoadSamples, "under-load");
          long underLoadOps = totalOps(underLoadSamples);
          String header = String.format(Locale.ROOT,
              "BENCH mixed numBuckets=%d backlogFiles=%d threads=%d opsPerThread=%d ratisByteLimit=%s underLoadOps=%d "
                  + "phase1DrainMs=%.1f",
              numBuckets, backlogFiles, clientThreads, opsPerThread, ratisAppenderByteLimit, underLoadOps,
              phase1DrainMs);
          printBenchLine(control, underLoad, header);
          if (profileOut != null) {
            System.out.printf(Locale.ROOT, "BENCH profile event=%s out=%s%n", profileEvent, profileOut);
          }
        } finally {
          for (FileSystem f : env.fs) {
            org.apache.hadoop.io.IOUtils.closeStream(f);
          }
        }
      }
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Blocks until phase 1 (DirectoryDeletingService) finishes: every backlog sub-file has been moved (cheap counter)
   * and the deletedDirTable has been fully drained of sub-dirs. countRowsInTable scans the table, so it is only probed
   * once the moved-files counter shows the sub-files are all moved. Phase 2 (deletedTable purge) is out of scope.
   * Returns the drain duration in nanos, or -1 if the workload failed before the drain completed.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private static long awaitPhase1Drain(DirectoryDeletingService dds, OMMetadataManager mm,
      Table<String, ?> deletedDirTable, long movedFilesBefore, long backlogFiles, RunningWorkload underLoadWl,
      long start, long drainDeadline) throws Exception {
    while (true) {
      long moved = dds.getMovedFilesCount() - movedFilesBefore;
      if (moved >= backlogFiles && mm.countRowsInTable(deletedDirTable) == 0) {
        return System.nanoTime() - start;
      }
      if (underLoadWl.failed()) {
        return -1;
      }
      if (System.nanoTime() > drainDeadline) {
        throw new IllegalStateException("phase 1 did not drain within 900s: moved=" + moved
            + " expected=" + backlogFiles
            + " deletedDirTableRows=" + mm.countRowsInTable(deletedDirTable));
      }
      Thread.sleep(200);
    }
  }

  private void buildDenseTree(FileSystem fs, Path root, int dirs, int filesPerDir, int nonEmptyEvery,
      byte[] blockContent) throws Exception {
    long buildStart = System.nanoTime();
    // Staging is client/RPC-round-trip bound, not apply-thread bound, so more concurrent creators speed it up
    // nearly linearly; a large backlog is otherwise the long pole of the run. Setup-only — does not affect the
    // measured control/under-load workload.
    ExecutorService pool = Executors.newFixedThreadPool(Integer.getInteger("bench.stagingThreads", 48));
    List<Future<?>> futures = new ArrayList<>(dirs);
    for (int d = 0; d < dirs; d++) {
      final Path dir = new Path(root, "dir" + d);
      futures.add(pool.submit(() -> {
        fs.mkdirs(dir);
        for (int f = 0; f < filesPerDir; f++) {
          Path file = new Path(dir, "f" + f);
          if (nonEmptyEvery > 0 && f % nonEmptyEvery == 0) {
            try (FSDataOutputStream os = fs.create(file, true)) {
              os.write(blockContent);
            }
          } else {
            fs.create(file, true).close();
          }
        }
        return null;
      }));
    }
    for (Future<?> future : futures) {
      future.get();
    }
    pool.shutdown();
    pool.awaitTermination(120, TimeUnit.SECONDS);
    LOG.info("Built dense backlog: {} files in {} dirs, every {}-th with a block ({} ms)",
        dirs * filesPerDir, dirs, nonEmptyEvery, (System.nanoTime() - buildStart) / 1_000_000);
  }

  /** Creates one volume with {@code numBuckets} FSO buckets and one {@code o3fs} FileSystem rooted at each bucket, so
   * the backlog and workload can be spread across buckets and a purge round can gather deleted dirs from several of
   * them. Returned FileSystems must be closed by the caller. */
  private BucketFs createBuckets(OzoneClient client, OzoneConfiguration conf, int numBuckets) throws IOException {
    OzoneBucket first = org.apache.hadoop.ozone.DataTestUtil.createVolumeAndBucket(client,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    OzoneVolume volume = client.getObjectStore().getVolume(first.getVolumeName());
    OzoneBucket[] buckets = new OzoneBucket[numBuckets];
    FileSystem[] fs = new FileSystem[numBuckets];
    buckets[0] = first;
    BucketArgs args = BucketArgs.newBuilder().setStorageType(StorageType.DISK)
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED).build();
    for (int b = 1; b < numBuckets; b++) {
      String name = first.getName() + "-" + b;
      volume.createBucket(name, args);
      buckets[b] = volume.getBucket(name);
    }
    for (int b = 0; b < numBuckets; b++) {
      OzoneConfiguration bucketConf = new OzoneConfiguration(conf);
      bucketConf.set(FS_DEFAULT_NAME_KEY, String.format("%s://%s.%s/", OzoneConsts.OZONE_URI_SCHEME,
          buckets[b].getName(), volume.getName()));
      fs[b] = FileSystem.get(bucketConf);
    }
    return new BucketFs(volume, buckets, fs);
  }

  /** One volume, its FSO buckets, and the per-bucket {@code o3fs} FileSystems (index-aligned with {@code buckets}). */
  private static final class BucketFs {
    private final OzoneVolume volume;
    private final OzoneBucket[] buckets;
    private final FileSystem[] fs;

    BucketFs(OzoneVolume volume, OzoneBucket[] buckets, FileSystem[] fs) {
      this.volume = volume;
      this.buckets = buckets;
      this.fs = fs;
    }
  }

  /**
   * Starts {@code threads} client threads spread round-robin across the hot buckets (thread {@code t} is pinned to
   * bucket {@code t % buckets.length} and its FileSystem), each cycling through the {@link #OPS} mix (create, mkdir,
   * rename and a data-plane file write, plus a data-plane file read and the metadata read RPCs that contend for the
   * bucket read lock: getFileStatus, listStatus, getBucketInfo and lookupKey) and recording per-operation nanosecond
   * samples. When {@code opsPerThread > 0} each thread runs that fixed number of ops (control pass); when
   * {@code opsPerThread <= 0} each thread runs continuously until {@link RunningWorkload#stop()} is called (the
   * under-load drain window). Call {@link RunningWorkload#await()} to join the threads and collect the samples
   * grouped by operation index (aligned with {@link #OPS}).
   */
  private RunningWorkload startMixedWorkload(FileSystem[] fs, OzoneVolume volume, OzoneBucket[] buckets,
      WorkloadDataset dataset, int threads, int opsPerThread, String label) {
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch startLatch = new CountDownLatch(1);
    AtomicBoolean failed = new AtomicBoolean(false);
    AtomicBoolean running = new AtomicBoolean(true);
    List<Future<List<long[]>>> futures = new ArrayList<>(threads);
    for (int t = 0; t < threads; t++) {
      final int threadId = t;
      final int bucketId = t % buckets.length;
      final FileSystem tfs = fs[bucketId];
      final OzoneBucket bucket = buckets[bucketId];
      final String bucketName = bucket.getName();
      futures.add(pool.submit((Callable<List<long[]>>) () -> {
        List<long[]> latencies = new ArrayList<>(Math.max(16, opsPerThread));
        // Write ops create into this thread's private scratch subtree; read ops resolve against the pre-staged
        // dataset. Both live under the workload set (same volume and bucket as the backlog), which is never deleted.
        Path scratch = new Path("/" + WORKLOAD_SCRATCH_ROOT, label + "-" + threadId);
        tfs.mkdirs(scratch);
        Path lastCreated = null;
        byte[] readBuf = new byte[64 * 1024];
        startLatch.await();
        // Fixed batch when opsPerThread > 0 (control); otherwise loop until stop() clears running (under-load).
        for (int i = 0; running.get() && (opsPerThread <= 0 || i < opsPerThread); i++) {
          int opIdx = i % OPS.length;
          // rename operates on the file created in this thread's previous create op; fall back to a create when none
          // is pending so the cycle never renames a missing path.
          if (opIdx == 2 && lastCreated == null) {
            opIdx = 0;
          }
          // Read ops target an existing dataset entry; the index spread keeps threads off a single dir.
          int dataDir = (threadId + i) % dataset.dirs;
          int dataFile = i % dataset.filesPerDir;
          // The data-plane read must hit a staged file that carries a block (every nonEmptyEvery-th file).
          int blockFile = (i % Math.max(1, dataset.filesPerDir / dataset.nonEmptyEvery)) * dataset.nonEmptyEvery;
          long t0 = System.nanoTime();
          try {
            switch (opIdx) {
            case 0:
              Path created = new Path(scratch, "f" + i);
              tfs.create(created, true).close();
              lastCreated = created;
              break;
            case 1:
              tfs.mkdirs(new Path(scratch, "d" + i));
              break;
            case 2:
              tfs.rename(lastCreated, new Path(scratch, "f" + i + "-r"));
              lastCreated = null;
              break;
            case 3:
              // File write: create + write a real block, exercising the data-plane write path (block allocation,
              // datanode write, commit) on top of the OM key create/commit.
              try (FSDataOutputStream os = tfs.create(new Path(scratch, "w" + i), true)) {
                os.write(dataset.fileContent);
              }
              break;
            case 4:
              // File read: open a staged block-bearing file and read it fully, exercising the data-plane read path
              // (block-location lookup + datanode read).
              readFully(tfs, new Path("/" + WORKLOAD_DATA_ROOT + "/dir" + dataDir, "f" + blockFile), readBuf);
              break;
            case 5:
              tfs.getFileStatus(new Path("/" + WORKLOAD_DATA_ROOT + "/dir" + dataDir, "f" + dataFile));
              break;
            case 6:
              tfs.listStatus(new Path("/" + WORKLOAD_DATA_ROOT, "dir" + dataDir));
              break;
            case 7:
              volume.getBucket(bucketName);
              break;
            default:
              bucket.getKey(WORKLOAD_DATA_ROOT + "/dir" + dataDir + "/f" + dataFile);
              break;
            }
          } catch (IOException | RuntimeException e) {
            failed.set(true);
            throw e;
          }
          latencies.add(new long[] {opIdx, System.nanoTime() - t0});
        }
        return latencies;
      }));
    }
    startLatch.countDown();
    return new RunningWorkload(pool, futures, running, failed, label);
  }

  /** Handle to a running {@link #startMixedWorkload} run: stop the threads, then await and collect their samples. */
  private static final class RunningWorkload {
    private final ExecutorService pool;
    private final List<Future<List<long[]>>> futures;
    private final AtomicBoolean running;
    private final AtomicBoolean failed;
    private final String label;

    RunningWorkload(ExecutorService pool, List<Future<List<long[]>>> futures, AtomicBoolean running,
        AtomicBoolean failed, String label) {
      this.pool = pool;
      this.futures = futures;
      this.running = running;
      this.failed = failed;
      this.label = label;
    }

    void stop() {
      running.set(false);
    }

    boolean failed() {
      return failed.get();
    }

    /** Joins the client threads and returns per-operation nanosecond samples grouped by operation index. */
    List<List<Long>> await() throws Exception {
      List<List<Long>> byOp = new ArrayList<>(OPS.length);
      for (String ignored : OPS) {
        byOp.add(new ArrayList<>());
      }
      for (Future<List<long[]>> future : futures) {
        for (long[] sample : future.get()) {
          byOp.get((int) sample[0]).add(sample[1]);
        }
      }
      pool.shutdown();
      pool.awaitTermination(180, TimeUnit.SECONDS);
      if (failed.get()) {
        throw new IllegalStateException("client thread failed during " + label);
      }
      for (int op = 0; op < OPS.length; op++) {
        LOG.info("{} {}: ops={}", label, OPS[op], byOp.get(op).size());
      }
      return byOp;
    }
  }

  /** Total number of samples across all operations (how many ops backed the reported percentiles). */
  private static long totalOps(List<List<Long>> byOp) {
    long total = 0;
    for (List<Long> opSamples : byOp) {
      total += opSamples.size();
    }
    return total;
  }

  /** Reduces per-operation nanosecond samples to {@link Percentiles} (index-aligned with {@link #OPS}). */
  private static Percentiles[] toPercentiles(List<List<Long>> byOp, String label) {
    Percentiles[] result = new Percentiles[OPS.length];
    for (int op = 0; op < OPS.length; op++) {
      result[op] = Percentiles.of(byOp.get(op));
      LOG.info("{} {}: ops={} p50Ms={} p99Ms={}", label, OPS[op], byOp.get(op).size(),
          result[op].p50, result[op].p99);
    }
    return result;
  }

  private static double safeRatio(double num, double den) {
    return den <= 0 ? Double.NaN : num / den;
  }

  /** Appends the per-operation control/under-load percentiles and degradation ratios to {@code header} and prints
   * the single machine-readable {@code BENCH mixed ...} line the A/B driver greps for. */
  private static void printBenchLine(Percentiles[] control, Percentiles[] underLoad, String header) {
    StringBuilder line = new StringBuilder(header);
    for (int op = 0; op < OPS.length; op++) {
      line.append(String.format(Locale.ROOT, " %s[control_p50=%.2f control_p99=%.2f "
              + "underLoad_p50=%.2f underLoad_p99=%.2f deg50=%.2fx deg99=%.2fx]",
          OPS[op], control[op].p50, control[op].p99, underLoad[op].p50, underLoad[op].p99,
          safeRatio(underLoad[op].p50, control[op].p50), safeRatio(underLoad[op].p99, control[op].p99)));
    }
    System.out.println(line);
  }

  /** Reads {@code p} to end into the reusable {@code buf}, discarding the bytes — the benchmark measures the read
   * path (block-location lookup + datanode read), not the payload. */
  private static void readFully(FileSystem fs, Path p, byte[] buf) throws IOException {
    try (FSDataInputStream in = fs.open(p)) {
      int n;
      do {
        n = in.read(buf);
      } while (n != -1);
    }
  }

  /** Shape of the stable workload dataset the read ops resolve against ({@code dirs} directories, each holding
   * {@code filesPerDir} files under {@link #WORKLOAD_DATA_ROOT}, every {@code nonEmptyEvery}-th carrying a
   * {@code fileContent} block), plus the payload the data-plane file write op writes. */
  private static final class WorkloadDataset {
    private final int dirs;
    private final int filesPerDir;
    private final int nonEmptyEvery;
    private final byte[] fileContent;

    WorkloadDataset(int dirs, int filesPerDir, int nonEmptyEvery, byte[] fileContent) {
      this.dirs = dirs;
      this.filesPerDir = filesPerDir;
      this.nonEmptyEvery = nonEmptyEvery;
      this.fileContent = fileContent;
    }
  }

  /**
   * Thin reflective wrapper over async-profiler's {@code one.profiler.AsyncProfiler} API, loaded at runtime from the
   * configured jar so the benchmark carries no compile-time or Maven dependency on async-profiler.
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
      // JFR output (inferred from the .jfr extension) records cpu/wall sample intervals and every contended lock/alloc
      // event, so a single recording converts to both a flamegraph and collapsed stacks afterwards.
      execute.invoke(delegate, String.format(Locale.ROOT, "start,event=%s,interval=1ms,file=%s", event, jfrFile));
    }

    void stop() throws Exception {
      execute.invoke(delegate, "stop");
    }
  }

  /** p50/p99 in milliseconds over a set of nanosecond latency samples. */
  private static final class Percentiles {
    private final double p50;
    private final double p99;

    private Percentiles(double p50, double p99) {
      this.p50 = p50;
      this.p99 = p99;
    }

    static Percentiles of(List<Long> samplesNs) {
      if (samplesNs.isEmpty()) {
        return new Percentiles(Double.NaN, Double.NaN);
      }
      long[] sorted = samplesNs.stream().mapToLong(Long::longValue).sorted().toArray();
      double p50 = sorted[(int) (sorted.length * 0.50)] / 1_000_000.0;
      double p99 = sorted[Math.min(sorted.length - 1, (int) (sorted.length * 0.99))] / 1_000_000.0;
      return new Percentiles(p50, p99);
    }
  }
}
