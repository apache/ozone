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

package org.apache.hadoop.ozone.om.snapshot;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.ROCKSDB_SST_SUFFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_DEFRAG_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DEFRAG_LIMIT_PER_TASK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.ManagedRawSSTFileReader;
import org.apache.hadoop.ozone.DataTestUtil;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshotInternalMetrics;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * HDDS-13218: integration tests that snapshot defrag reduces checkpoint disk footprint.
 *
 * <p>Uses inode-aware sizing (matching {@link OMSnapshotDirectoryMetrics}) so hardlinked SST files
 * are not double-counted across snapshot checkpoint directories. Version-0 checkpoints hardlink to
 * AOS SST files, so their on-disk byte totals are not comparable to materialized post-defrag
 * checkpoints. Savings are validated by cross-snapshot SST reference reduction in the chain.
 *
 * <p>Covers a three-snapshot chain with AOS compactions and insert/overwrite/delete churn on OBS
 * and FSO buckets, full-then-incremental defrag paths, footprint checks after deleting the
 * middle snapshot and running a follow-up defrag on the remaining youngest snapshot, isolated
 * full defrag on a single snapshot, and idempotent repeated defrag on an already-defragged chain.
 *
 * <p>Uses one mini-cluster for the whole class (1 datanode, replication factor one) because
 * assertions inspect OM checkpoint directories only. Shared snapshot-defrag helpers with
 * {@link TestOmSnapshotCheckpointDbContent} may be consolidated in a follow-up under HDDS-13003.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOmSnapshotDefragSpaceSavings {

  private static final byte[] TEST_KEY_CONTENT = new byte[] {0x61, 0x62, 0x63};
  private static final byte[] OVERWRITE_KEY_CONTENT = new byte[] {0x64, 0x65, 0x66};
  private static final int INITIAL_KEY_COUNT = 100;
  private static final int OVERWRITE_KEY_COUNT = 50;
  private static final int DELETE_KEY_COUNT = 25;
  private static final int NEW_KEYS_PER_SNAPSHOT = 10;
  private static final int CHECKPOINT_WAIT_MS = 120_000;
  private static final int PURGE_WAIT_MS = 180_000;
  private static final int DEFRAG_WAIT_MS = 600_000;
  private static final int KEY_DELETE_WAIT_MS = 60_000;
  private static final long FOOTPRINT_TOLERANCE_BYTES = 8192;

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private OzoneClient client;
  private ObjectStore store;

  @BeforeAll
  void initCluster() throws Exception {
    startCluster();
  }

  private void startCluster() throws Exception {
    assumeTrue(ManagedRawSSTFileReader.tryLoadLibrary(),
        "Snapshot defrag requires rocks-tools native library");

    conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);
    // Keep background defrag idle during the test; manual triggerSnapshotDefrag() still requires
    // the service to be initialized (interval must be > 0).
    conf.setTimeDuration(OZONE_SNAPSHOT_DEFRAG_SERVICE_INTERVAL, 2, TimeUnit.HOURS);
    conf.setInt(SNAPSHOT_DEFRAG_LIMIT_PER_TASK, 10);
    conf.setTimeDuration(OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setInt(OZONE_REPLICATION, 1);

    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1).build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(ONE, 60_000);
    client = cluster.newClient();
    store = client.getObjectStore();
    resumeBackgroundServices();
  }

  @AfterAll
  void shutdownCluster() {
    IOUtils.closeQuietly(client, cluster);
  }

  private void resumeBackgroundServices() {
    OzoneManager om = cluster.getOzoneManager();
    om.getKeyManager().getDeletingService().resume();
    om.getKeyManager().getDirDeletingService().resume();
    om.getKeyManager().getSnapshotDeletingService().resume();
  }

  /**
   * Three-snapshot chain with churn on OBS and FSO: defrag should reduce aggregate checkpoint
   * footprint on both layouts. OBS pass also verifies one full defrag and two incremental defrags.
   */
  @ParameterizedTest(name = "layout={0}")
  @EnumSource(value = BucketLayout.class, names = {"OBJECT_STORE", "FILE_SYSTEM_OPTIMIZED"})
  public void testSnapshotDefragReducesCheckpointFootprintWithChurn(BucketLayout layout)
      throws Exception {
    runChurnFootprintScenario(layout);
  }

  /**
   * After an initial defrag pass, deleting the middle snapshot and defragging again should not
   * increase the youngest snapshot footprint and should shrink the remaining chain footprint.
   */
  @Test
  public void testObsSnapshotDefragReducesFootprintAfterMiddleSnapshotPurge() throws Exception {
    SnapshotChainSetup setup = createSnapshotChainWithChurn(BucketLayout.OBJECT_STORE);
    triggerDefragUntilDone(setup.snapshots);

    SnapshotInfo s2 = setup.snapshots.get(1);
    SnapshotInfo s3 = setup.snapshots.get(2);
    int s3VersionAfterFirstDefrag = readSnapshotVersion(s3);
    CheckpointFootprint s3FootprintAfterFirstDefrag = measureActiveAggregateCheckpointFootprint(
        Arrays.asList(s3));
    CheckpointFootprint aggregateAfterFirstDefrag =
        measureActiveAggregateCheckpointFootprint(setup.snapshots);

    store.deleteSnapshot(setup.volumeName, setup.bucketName, s2.getName());
    waitForSnapshotPurged(s2);
    s3 = loadSnapshotInfo(setup.volumeName, setup.bucketName, s3.getName());

    triggerDefragUntilVersionIncreases(s3, s3VersionAfterFirstDefrag);

    CheckpointFootprint s3FootprintAfterSecondDefrag = measureActiveAggregateCheckpointFootprint(
        Arrays.asList(s3));
    assertTrue(
        s3FootprintAfterSecondDefrag.getTotalBytes()
            <= s3FootprintAfterFirstDefrag.getTotalBytes() + FOOTPRINT_TOLERANCE_BYTES,
        () -> String.format(
            "Expected S3 footprint not to grow materially after purge re-defrag: first=%d bytes, "
                + "second=%d bytes",
            s3FootprintAfterFirstDefrag.getTotalBytes(), s3FootprintAfterSecondDefrag.getTotalBytes()));

    SnapshotInfo s1 = setup.snapshots.get(0);
    CheckpointFootprint aggregateAfterSecondDefrag = measureActiveAggregateCheckpointFootprint(
        Arrays.asList(s1, s3));
    assertTrue(aggregateAfterSecondDefrag.getTotalBytes() < aggregateAfterFirstDefrag.getTotalBytes(),
        () -> String.format(
            "Expected remaining chain footprint to shrink after S2 purge: before=%d bytes, after=%d bytes",
            aggregateAfterFirstDefrag.getTotalBytes(), aggregateAfterSecondDefrag.getTotalBytes()));
  }

  /**
   * A lone OBS snapshot should run through the full defrag path, materialize a defragged checkpoint,
   * and remove the version-0 directory. Byte savings for a single snapshot are validated on a chain
   * in {@link #testSnapshotDefragReducesCheckpointFootprintWithChurn()}.
   */
  @Test
  public void testObsSingleSnapshotFullDefragReducesCheckpointFootprint() throws Exception {
    SnapshotInfo snapshotInfo = createSingleSnapshotWithChurn(BucketLayout.OBJECT_STORE);
    List<SnapshotInfo> snapshots = Arrays.asList(snapshotInfo);

    OmSnapshotInternalMetrics metrics = cluster.getOzoneManager().getOmSnapshotIntMetrics();
    long fullDefragBefore = metrics.getNumSnapshotFullDefrag();

    triggerDefragUntilDone(snapshots);

    assertEquals(1, readSnapshotVersion(snapshotInfo),
        "Single snapshot should be at defrag version 1");
    assertNull(snapshotInfo.getPathPreviousSnapshotId(),
        "Single snapshot should use the full defrag path");
    assertTrue(metrics.getNumSnapshotFullDefrag() >= fullDefragBefore + 1,
        "Expected a full defrag for the lone snapshot");
    assertVersionZeroCheckpointRemoved(snapshotInfo);
    assertTrue(isSnapshotDefragComplete(snapshotInfo),
        "Single snapshot should be defrag-complete after defrag");
  }

  /**
   * Running defrag again on an already-defragged three-snapshot chain should not increase
   * checkpoint footprint or snapshot local-data versions.
   */
  @Test
  public void testObsRepeatedDefragDoesNotIncreaseCheckpointFootprint() throws Exception {
    List<SnapshotInfo> snapshots = createSnapshotChainWithChurn(BucketLayout.OBJECT_STORE).snapshots;
    triggerDefragUntilDone(snapshots);

    CheckpointFootprint footprintAfterFirstDefrag =
        measureActiveAggregateCheckpointFootprint(snapshots);
    int s1Version = readSnapshotVersion(snapshots.get(0));
    int s2Version = readSnapshotVersion(snapshots.get(1));
    int s3Version = readSnapshotVersion(snapshots.get(2));

    cluster.getOzoneManager().triggerSnapshotDefrag(false);

    CheckpointFootprint footprintAfterSecondDefrag =
        measureActiveAggregateCheckpointFootprint(snapshots);
    assertEquals(footprintAfterFirstDefrag.getTotalBytes(),
        footprintAfterSecondDefrag.getTotalBytes(),
        "Repeated defrag should not increase checkpoint bytes");
    assertEquals(footprintAfterFirstDefrag.getSstFileCount(),
        footprintAfterSecondDefrag.getSstFileCount(),
        "Repeated defrag should not increase SST file count");
    assertEquals(s1Version, readSnapshotVersion(snapshots.get(0)),
        "Repeated defrag should not bump S1 version");
    assertEquals(s2Version, readSnapshotVersion(snapshots.get(1)),
        "Repeated defrag should not bump S2 version");
    assertEquals(s3Version, readSnapshotVersion(snapshots.get(2)),
        "Repeated defrag should not bump S3 version");
  }

  private void runChurnFootprintScenario(BucketLayout layout) throws Exception {
    List<SnapshotInfo> snapshots = createSnapshotChainWithChurn(layout).snapshots;
    CheckpointFootprint duplicateInclusiveBefore =
        measureDuplicateInclusiveAggregateFootprint(snapshots, 0);
    CheckpointFootprint dedupedBefore = measureAggregateCheckpointFootprint(snapshots, 0);

    OmSnapshotInternalMetrics metrics = cluster.getOzoneManager().getOmSnapshotIntMetrics();
    long fullDefragBefore = metrics.getNumSnapshotFullDefrag();
    long incDefragBefore = metrics.getNumSnapshotIncDefrag();

    triggerDefragUntilDone(snapshots);

    assertDefragReducedChainFootprint(layout, snapshots, duplicateInclusiveBefore, dedupedBefore);
    if (layout == BucketLayout.OBJECT_STORE) {
      assertTrue(metrics.getNumSnapshotFullDefrag() >= fullDefragBefore + 1,
          "Expected at least one full defrag for the chain head snapshot");
      assertTrue(metrics.getNumSnapshotIncDefrag() >= incDefragBefore + 2,
          "Expected incremental defrag for the second and third snapshots");
      assertNull(snapshots.get(0).getPathPreviousSnapshotId(),
          "Chain head snapshot should use the full defrag path");
      assertNotNull(snapshots.get(1).getPathPreviousSnapshotId(),
          "Second snapshot should use the incremental defrag path");
    }
  }

  private void assertDefragReducedChainFootprint(BucketLayout layout, List<SnapshotInfo> snapshots,
      CheckpointFootprint duplicateInclusiveBefore, CheckpointFootprint dedupedBefore)
      throws IOException {
    CheckpointFootprint dedupedAfter = measureActiveAggregateCheckpointFootprint(snapshots);
    CheckpointFootprint duplicateInclusiveAfter = measureDuplicateInclusiveActiveFootprint(snapshots);

    assertTrue(duplicateInclusiveBefore.getSstFileCount() > dedupedBefore.getSstFileCount(),
        () -> String.format(
            "[%s] Expected pre-defrag chain to carry redundant SST references: "
                + "duplicate-inclusive=%d, deduped=%d",
            layout, duplicateInclusiveBefore.getSstFileCount(), dedupedBefore.getSstFileCount()));
    assertTrue(dedupedAfter.getSstFileCount() < duplicateInclusiveBefore.getSstFileCount(),
        () -> String.format(
            "[%s] Expected defragged chain to drop SST references vs duplicate-inclusive pre-defrag "
                + "baseline: before=%d, after=%d",
            layout, duplicateInclusiveBefore.getSstFileCount(), dedupedAfter.getSstFileCount()));

    long sstRedundancyBefore = duplicateInclusiveBefore.getSstFileCount()
        - dedupedBefore.getSstFileCount();
    long sstRedundancyAfter = duplicateInclusiveAfter.getSstFileCount()
        - dedupedAfter.getSstFileCount();
    assertTrue(sstRedundancyAfter < sstRedundancyBefore,
        () -> String.format(
            "[%s] Expected defrag to reduce cross-snapshot SST redundancy: before=%d, after=%d",
            layout, sstRedundancyBefore, sstRedundancyAfter));
  }

  /**
   * Sums each snapshot checkpoint independently, counting every file path without inode dedup, so
   * hardlinked SST paths in version-0 checkpoints are charged once per snapshot directory.
   */
  private CheckpointFootprint measureDuplicateInclusiveActiveFootprint(
      List<SnapshotInfo> snapshots) throws IOException {
    long totalBytes = 0;
    long sstFileCount = 0;
    OMMetadataManager metadataManager = cluster.getOzoneManager().getMetadataManager();
    for (SnapshotInfo snapshotInfo : snapshots) {
      Path checkpointDir = OmSnapshotManager.getSnapshotPath(metadataManager,
          snapshotInfo.getSnapshotId(), readSnapshotVersion(snapshotInfo));
      CheckpointFootprint footprint = calculateDirectoryFootprintWithoutDedup(checkpointDir);
      totalBytes += footprint.getTotalBytes();
      sstFileCount += footprint.getSstFileCount();
    }
    return new CheckpointFootprint(totalBytes, sstFileCount);
  }

  private CheckpointFootprint measureDuplicateInclusiveAggregateFootprint(
      List<SnapshotInfo> snapshots, int version) throws IOException {
    long totalBytes = 0;
    long sstFileCount = 0;
    OMMetadataManager metadataManager = cluster.getOzoneManager().getMetadataManager();
    for (SnapshotInfo snapshotInfo : snapshots) {
      Path checkpointDir = OmSnapshotManager.getSnapshotPath(metadataManager,
          snapshotInfo.getSnapshotId(), version);
      CheckpointFootprint footprint = calculateDirectoryFootprintWithoutDedup(checkpointDir);
      totalBytes += footprint.getTotalBytes();
      sstFileCount += footprint.getSstFileCount();
    }
    return new CheckpointFootprint(totalBytes, sstFileCount);
  }

  private SnapshotChainSetup createSnapshotChainWithChurn(BucketLayout layout)
      throws IOException, InterruptedException, TimeoutException {
    OzoneBucket bucket = DataTestUtil.createVolumeAndBucket(client, layout);
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();
    DBStore activeDbStore = cluster.getOzoneManager().getMetadataManager().getStore();

    List<String> phaseOneKeys = createKeys(bucket, layout, "key-", 0, INITIAL_KEY_COUNT);
    store.createSnapshot(volumeName, bucketName, "snap-s1");
    activeDbStore.compactDB();

    for (int i = 0; i < OVERWRITE_KEY_COUNT; i++) {
      DataTestUtil.createKey(bucket, phaseOneKeys.get(i), OVERWRITE_KEY_CONTENT);
    }
    createKeys(bucket, layout, "key-s2-", 0, NEW_KEYS_PER_SNAPSHOT);
    store.createSnapshot(volumeName, bucketName, "snap-s2");
    activeDbStore.compactDB();

    for (int i = OVERWRITE_KEY_COUNT; i < OVERWRITE_KEY_COUNT + DELETE_KEY_COUNT; i++) {
      bucket.deleteKey(phaseOneKeys.get(i));
      waitForKeyDeleted(bucket, phaseOneKeys.get(i));
    }
    createKeys(bucket, layout, "key-s3-", 0, NEW_KEYS_PER_SNAPSHOT);
    store.createSnapshot(volumeName, bucketName, "snap-s3");
    activeDbStore.compactDB();

    List<SnapshotInfo> snapshots = Arrays.asList(
        loadSnapshotInfo(volumeName, bucketName, "snap-s1"),
        loadSnapshotInfo(volumeName, bucketName, "snap-s2"),
        loadSnapshotInfo(volumeName, bucketName, "snap-s3"));
    for (SnapshotInfo snapshotInfo : snapshots) {
      waitForCheckpointReady(snapshotInfo);
    }
    return new SnapshotChainSetup(volumeName, bucketName, snapshots);
  }

  private SnapshotInfo createSingleSnapshotWithChurn(BucketLayout layout)
      throws IOException, InterruptedException, TimeoutException {
    OzoneBucket bucket = DataTestUtil.createVolumeAndBucket(client, layout);
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();
    DBStore activeDbStore = cluster.getOzoneManager().getMetadataManager().getStore();

    List<String> keys = createKeys(bucket, layout, "key-", 0, INITIAL_KEY_COUNT);
    activeDbStore.compactDB();
    for (int i = 0; i < OVERWRITE_KEY_COUNT; i++) {
      DataTestUtil.createKey(bucket, keys.get(i), OVERWRITE_KEY_CONTENT);
    }
    store.createSnapshot(volumeName, bucketName, "snap-s1");
    activeDbStore.compactDB();

    SnapshotInfo snapshotInfo = loadSnapshotInfo(volumeName, bucketName, "snap-s1");
    waitForCheckpointReady(snapshotInfo);
    return snapshotInfo;
  }

  private void assertVersionZeroCheckpointRemoved(SnapshotInfo snapshotInfo) throws IOException {
    OMMetadataManager metadataManager = cluster.getOzoneManager().getMetadataManager();
    Path versionZeroDir = OmSnapshotManager.getSnapshotPath(metadataManager,
        snapshotInfo.getSnapshotId(), 0);
    assertTrue(!Files.isDirectory(versionZeroDir),
        "Version-0 checkpoint directory should be removed after defrag: " + versionZeroDir);
  }

  private static List<String> createKeys(OzoneBucket bucket, BucketLayout layout, String prefix,
      int start, int count) throws IOException {
    List<String> keyNames = new ArrayList<>(count);
    for (int i = start; i < start + count; i++) {
      String keyName = objectKey(layout, prefix + String.format("%05d", i));
      DataTestUtil.createKey(bucket, keyName, TEST_KEY_CONTENT);
      keyNames.add(keyName);
    }
    return keyNames;
  }

  private static String objectKey(BucketLayout layout, String name) {
    return layout.isFileSystemOptimized() ? "dir/" + name : name;
  }

  private SnapshotInfo loadSnapshotInfo(String volumeName, String bucketName,
      String snapshotName) throws IOException {
    OzoneManager om = cluster.getOzoneManager();
    SnapshotInfo snapshotInfo = om.getMetadataManager().getSnapshotInfoTable().get(
        SnapshotInfo.getTableKey(volumeName, bucketName, snapshotName));
    assertNotNull(snapshotInfo, "Snapshot row should exist for " + snapshotName);
    assertEquals(snapshotName, snapshotInfo.getName());
    return snapshotInfo;
  }

  private void waitForCheckpointReady(SnapshotInfo snapshotInfo)
      throws TimeoutException, InterruptedException {
    String currentPath = OmSnapshotManager.getSnapshotPath(conf, snapshotInfo, 0)
        + OM_KEY_PREFIX + "CURRENT";
    GenericTestUtils.waitFor(() -> new File(currentPath).exists(), 1000, CHECKPOINT_WAIT_MS);
  }

  private void waitForSnapshotPurged(SnapshotInfo snapshotInfo)
      throws TimeoutException, InterruptedException {
    OzoneManager om = cluster.getOzoneManager();
    resumeBackgroundServices();
    GenericTestUtils.waitFor(() -> {
      try {
        return om.getMetadataManager().getSnapshotInfoTable()
            .get(snapshotInfo.getTableKey()) == null;
      } catch (IOException e) {
        return false;
      }
    }, 1000, PURGE_WAIT_MS);
  }

  private void waitForKeyDeleted(OzoneBucket bucket, String keyName)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      try {
        bucket.getKey(keyName);
        return false;
      } catch (IOException e) {
        return true;
      }
    }, 1000, KEY_DELETE_WAIT_MS);
  }

  /**
   * Wait for a follow-up defrag pass after snapshot-chain rewiring (e.g. middle snapshot purge).
   */
  private void triggerDefragUntilVersionIncreases(SnapshotInfo snapshotInfo,
      int baselineVersion) throws TimeoutException, InterruptedException {
    OzoneManager om = cluster.getOzoneManager();
    String volumeName = snapshotInfo.getVolumeName();
    String bucketName = snapshotInfo.getBucketName();
    String snapshotName = snapshotInfo.getName();
    waitForDefragCondition("snapshot " + snapshotName + " defrag version > " + baselineVersion,
        () -> {
          SnapshotInfo currentSnapshot = loadSnapshotInfo(volumeName, bucketName, snapshotName);
          if (readSnapshotVersion(currentSnapshot) > baselineVersion
              && isSnapshotDefragComplete(currentSnapshot)) {
            return true;
          }
          om.triggerSnapshotDefrag(false);
          currentSnapshot = loadSnapshotInfo(volumeName, bucketName, snapshotName);
          return readSnapshotVersion(currentSnapshot) > baselineVersion
              && isSnapshotDefragComplete(currentSnapshot);
        });
  }

  private void triggerDefragUntilDone(List<SnapshotInfo> snapshots)
      throws TimeoutException, InterruptedException {
    waitForDefragCondition("all snapshots defrag-complete", () -> {
      if (areAllSnapshotsDefragComplete(snapshots)) {
        return true;
      }
      cluster.getOzoneManager().triggerSnapshotDefrag(false);
      return areAllSnapshotsDefragComplete(snapshots);
    });
  }

  private void waitForDefragCondition(String description, DefragWaitCondition condition)
      throws TimeoutException, InterruptedException {
    IOException[] lastFailure = new IOException[1];
    try {
      GenericTestUtils.waitFor(() -> {
        try {
          if (condition.check()) {
            lastFailure[0] = null;
            return true;
          }
          return false;
        } catch (IOException e) {
          lastFailure[0] = e;
          return false;
        }
      }, 2000, DEFRAG_WAIT_MS);
    } catch (TimeoutException e) {
      if (lastFailure[0] != null) {
        TimeoutException timeout = new TimeoutException(
            "Timed out waiting for " + description + ". Last triggerSnapshotDefrag failure: "
                + lastFailure[0].getMessage());
        timeout.initCause(lastFailure[0]);
        throw timeout;
      }
      throw e;
    }
  }

  @FunctionalInterface
  private interface DefragWaitCondition {
    boolean check() throws IOException;
  }

  private boolean areAllSnapshotsDefragComplete(List<SnapshotInfo> snapshots) {
    for (SnapshotInfo snapshotInfo : snapshots) {
      if (!isSnapshotDefragComplete(snapshotInfo)) {
        return false;
      }
    }
    return true;
  }

  private boolean isSnapshotDefragComplete(SnapshotInfo snapshotInfo) {
    try {
      OmSnapshotLocalDataManager localDataManager =
          cluster.getOzoneManager().getOmSnapshotManager().getSnapshotLocalDataManager();
      try (OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataProvider provider =
               localDataManager.getOmSnapshotLocalData(snapshotInfo)) {
        return provider.getVersion() > 0 && !provider.needsDefrag();
      }
    } catch (IOException e) {
      return false;
    }
  }

  private int readSnapshotVersion(SnapshotInfo snapshotInfo) throws IOException {
    OmSnapshotLocalDataManager localDataManager =
        cluster.getOzoneManager().getOmSnapshotManager().getSnapshotLocalDataManager();
    try (OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataProvider provider =
             localDataManager.getOmSnapshotLocalData(snapshotInfo)) {
      return (int) provider.getVersion();
    }
  }

  private CheckpointFootprint measureActiveAggregateCheckpointFootprint(
      List<SnapshotInfo> snapshots) throws IOException {
    Set<Object> visitedInodes = new HashSet<>();
    long totalBytes = 0;
    long sstFileCount = 0;
    OMMetadataManager metadataManager = cluster.getOzoneManager().getMetadataManager();
    for (SnapshotInfo snapshotInfo : snapshots) {
      CheckpointFootprint footprint = measureCheckpointDirectoryFootprint(
          metadataManager, snapshotInfo.getSnapshotId(), readSnapshotVersion(snapshotInfo),
          visitedInodes);
      totalBytes += footprint.getTotalBytes();
      sstFileCount += footprint.getSstFileCount();
    }
    return new CheckpointFootprint(totalBytes, sstFileCount);
  }

  private CheckpointFootprint measureAggregateCheckpointFootprint(
      List<SnapshotInfo> snapshots, int version) throws IOException {
    Set<Object> visitedInodes = new HashSet<>();
    long totalBytes = 0;
    long sstFileCount = 0;
    OMMetadataManager metadataManager = cluster.getOzoneManager().getMetadataManager();
    for (SnapshotInfo snapshotInfo : snapshots) {
      CheckpointFootprint footprint = measureCheckpointDirectoryFootprint(
          metadataManager, snapshotInfo.getSnapshotId(), version, visitedInodes);
      totalBytes += footprint.getTotalBytes();
      sstFileCount += footprint.getSstFileCount();
    }
    return new CheckpointFootprint(totalBytes, sstFileCount);
  }

  private static CheckpointFootprint measureCheckpointDirectoryFootprint(
      OMMetadataManager metadataManager, UUID snapshotId, int version,
      Set<Object> visitedInodes) throws IOException {
    Path checkpointDir = OmSnapshotManager.getSnapshotPath(metadataManager, snapshotId, version);
    assertTrue(Files.isDirectory(checkpointDir),
        "Expected checkpoint directory for snapshot " + snapshotId + " version " + version
            + " at " + checkpointDir);
    return calculateDirectoryFootprint(checkpointDir, visitedInodes);
  }

  private static CheckpointFootprint calculateDirectoryFootprintWithoutDedup(Path directory)
      throws IOException {
    assertTrue(Files.isDirectory(directory),
        "Expected checkpoint directory at " + directory);
    long totalBytes = 0;
    long sstFileCount = 0;
    try (Stream<Path> files = Files.list(directory)) {
      for (Path path : files.collect(Collectors.toList())) {
        if (!Files.isRegularFile(path)) {
          continue;
        }
        totalBytes += Files.size(path);
        if (path.getFileName().toString().endsWith(ROCKSDB_SST_SUFFIX)) {
          sstFileCount++;
        }
      }
    }
    return new CheckpointFootprint(totalBytes, sstFileCount);
  }

  /**
   * Measures checkpoint directory size using inode deduplication, matching
   * {@link OMSnapshotDirectoryMetrics}.
   */
  private static CheckpointFootprint calculateDirectoryFootprint(
      Path directory, Set<Object> visitedInodes) throws IOException {
    long totalBytes = 0;
    long sstFileCount = 0;
    try (Stream<Path> files = Files.list(directory)) {
      for (Path path : files.collect(Collectors.toList())) {
        if (!Files.isRegularFile(path)) {
          continue;
        }
        Object inodeKey = IOUtils.getINode(path);
        if (inodeKey == null) {
          inodeKey = path.toAbsolutePath() + ":" + Files.size(path);
        }
        if (visitedInodes.add(inodeKey)) {
          totalBytes += Files.size(path);
          if (path.getFileName().toString().endsWith(ROCKSDB_SST_SUFFIX)) {
            sstFileCount++;
          }
        }
      }
    }
    return new CheckpointFootprint(totalBytes, sstFileCount);
  }

  private static final class CheckpointFootprint {
    private final long totalBytes;
    private final long sstFileCount;

    private CheckpointFootprint(long totalBytes, long sstFileCount) {
      this.totalBytes = totalBytes;
      this.sstFileCount = sstFileCount;
    }

    private long getTotalBytes() {
      return totalBytes;
    }

    private long getSstFileCount() {
      return sstFileCount;
    }
  }

  private static final class SnapshotChainSetup {
    private final String volumeName;
    private final String bucketName;
    private final List<SnapshotInfo> snapshots;

    private SnapshotChainSetup(String volumeName, String bucketName,
        List<SnapshotInfo> snapshots) {
      this.volumeName = volumeName;
      this.bucketName = bucketName;
      this.snapshots = snapshots;
    }
  }
}
