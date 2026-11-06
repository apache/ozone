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

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.ozone.test.OzoneTestBase.uniqueObjectName;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.DataTestUtil;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerDoubleBuffer;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests hsync and a rejected overwrite commit in OM HA setup.
 *
 * <p>HDDS-16765: a rejected overwrite commit used to leave OVERWRITTEN_HSYNC_KEY on another writer's
 * open entry in the open file table cache until the double buffer flushed and evicted that epoch.
 *
 * <p>Client sequence (FileSystem API only), bucket namespace quota = 2:
 * <pre>
 *   A: create f, write, hsync          (used namespace 1)
 *   B: create f (overwrite), write     (create passes: 1 + 1 &lt;= 2)
 *   C: create g, close                 (used namespace 2)
 *   B: close                           -&gt; QUOTA_EXCEEDED, the overwrite never takes effect
 *   A: write, hsync, close             -&gt; must still succeed
 * </pre>
 *
 * <p>The same sequence is run in four situations, and each asserts the same outcome: writer A keeps
 * its own stream writable and every OM ends up holding what A committed. What they showed before
 * the fix:
 * <pre>
 *   no flush lag, repeated rounds -&gt; the window is not hit on timing alone
 *   every OM flushed beforehand   -&gt; passes cleanly
 *   one follower's flush held     -&gt; A sees success, but that follower's RocksDB diverges
 *   the leader's flush held       -&gt; A's close is rejected, although the followers applied it
 * </pre>
 */
public class TestHSyncOverwriteWithOMHA {
  private static final String OM_SERVICE_ID = "omservice-hsync-overwrite";
  /** Rounds of the sequence run without any double buffer synchronization. */
  private static final int ROUNDS_WITHOUT_FLUSH_LAG = 40;
  /** Bytes writer A has committed once it closes its stream. */
  private static final int EXPECTED_SIZE = 150;

  private static MiniOzoneHAClusterImpl cluster;
  private static OzoneClient client;
  private static FileSystem fs;
  /** Directory of a bucket with no namespace quota, used only for the Ratis ordering barrier. */
  private static Path markerDir;

  @BeforeAll
  public static void staticInit() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED, true);
    conf.setBoolean("ozone.client.hbase.enhancements.allowed", true);
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);

    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId(OM_SERVICE_ID)
        .setNumOfOzoneManagers(3)
        .build();

    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    OzoneConfiguration fsConf = new OzoneConfiguration(cluster.getConf());
    fsConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        String.format("%s://%s/", OZONE_OFS_URI_SCHEME, OM_SERVICE_ID));
    fs = FileSystem.get(fsConf);

    OzoneBucket markerBucket = DataTestUtil.createVolumeAndBucket(client, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    markerDir = new Path("/" + markerBucket.getVolumeName() + "/" + markerBucket.getName());
  }

  @AfterAll
  public static void cleanUp() {
    IOUtils.closeQuietly(fs, client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * No hook and no waiting: writer A continues right behind the rejected close of writer B, while
   * every OM is still flushing at its own pace. Repeated, since the window is only hit by timing.
   * The window is not expected to be hit on its own, so every round must come out clean.
   */
  @Test
  public void testRejectedOverwriteWithoutFlushLag() throws Exception {
    for (int round = 0; round < ROUNDS_WITHOUT_FLUSH_LAG; round++) {
      runRejectedOverwrite(null, false);
    }
  }

  /**
   * Control: every OM has flushed and evicted the open file table cache entry the rejected commit
   * flagged before the fix, so even then no OM could serve writer A from a stale cached entry.
   */
  @Test
  public void testRejectedOverwriteAfterAllOMsFlushed() throws Exception {
    runRejectedOverwrite(null, true);
  }

  /**
   * One follower's double buffer flush is held, so before the fix that follower was the only OM left
   * holding the cached open entry the rejected commit flagged. Writer A talks to the leader, which
   * has already evicted the entry, so A saw its writes succeed while the lagging follower rejected
   * them and its RocksDB diverged from the other two.
   */
  @Test
  public void testRejectedOverwriteWhileFollowerFlushLags() throws Exception {
    runRejectedOverwrite(follower(), true);
  }

  /**
   * The leader's double buffer flush is held instead. Writer A's own commit now goes to the OM that
   * before the fix still held the flagged cached entry, so A's close was rejected as overwritten
   * even though the followers had applied its earlier writes.
   */
  @Test
  public void testRejectedOverwriteWhileLeaderFlushLags() throws Exception {
    runRejectedOverwrite(cluster.getOMLeader(), true);
  }

  /**
   * Runs the HDDS-16765 client sequence on a fresh bucket and asserts that writer A keeps its own
   * stream writable and that every OM ends up with the same committed file.
   *
   * @param laggingOm OM whose double buffer flush is held across the rejected commit, null for none
   * @param syncFlushes true to let every other OM's double buffer catch up between the steps
   */
  private void runRejectedOverwrite(OzoneManager laggingOm, boolean syncFlushes) throws Exception {
    OzoneBucket bucket = DataTestUtil.createVolumeAndBucket(client, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    bucket.setQuota(OzoneQuota.parseQuota("10GB", "2"));

    Path dir = new Path("/" + bucket.getVolumeName() + "/" + bucket.getName());
    String fileName = uniqueObjectName("file-");
    Path file = new Path(dir, fileName);

    // A: create, write and hsync. The hsync puts the file in the file table, used namespace is 1.
    FSDataOutputStream streamA = fs.create(file, false);
    streamA.write(new byte[100]);
    streamA.hsync();

    // B: overwrite the same file and write to it. The create still passes the quota check, 1 + 1 <= 2.
    FSDataOutputStream streamB = fs.create(file, true);
    streamB.write(new byte[7]);

    // C: another file consumes the last namespace unit, so B's commit can no longer fit the quota.
    String anotherFileName = uniqueObjectName("file-");
    FSDataOutputStream streamC = fs.create(new Path(dir, anotherFileName), false);
    streamC.close();

    if (syncFlushes) {
      for (OzoneManager om : cluster.getOzoneManagersList()) {
        awaitOpenFileCacheFlush(om, fileName);
      }
    }

    OzoneManagerDoubleBuffer laggingBuffer = laggingOm == null ? null
        : laggingOm.getOmRatisServer().getOmStateMachine().getOzoneManagerDoubleBuffer();
    if (laggingBuffer != null) {
      laggingBuffer.pause();
    }
    try {
      // B: close. The overwrite commit is rejected by the namespace quota, so it must leave A alone.
      OMException e = assertThrows(OMException.class, streamB::close);
      assertEquals(OMException.ResultCodes.QUOTA_EXCEEDED, e.getResult());

      if (syncFlushes) {
        for (OzoneManager om : cluster.getOzoneManagersList()) {
          if (om != laggingOm) {
            awaitOpenFileCacheFlush(om, fileName);
          }
        }
      }

      // A: the rejected overwrite must not have taken A's own open entry away.
      assertDoesNotThrow(() -> {
        streamA.write(new byte[50]);
        streamA.hsync();
        streamA.close();
      }, "writer B's rejected overwrite commit took writer A's own open entry away");

      if (laggingOm != null) {
        awaitLaggingOmCaughtUp(laggingOm);
      }
    } finally {
      if (laggingBuffer != null) {
        laggingBuffer.unpause();
      }
    }

    // Every OM must end up agreeing on what A committed.
    for (OzoneManager om : cluster.getOzoneManagersList()) {
      awaitCommittedSize(om, bucket, fileName);
    }
  }

  private OzoneManager follower() {
    OzoneManager leader = cluster.getOMLeader();
    for (OzoneManager om : cluster.getOzoneManagersList()) {
      if (om != leader) {
        return om;
      }
    }
    throw new IllegalStateException("No follower OM");
  }

  /**
   * Waits until the double buffer of {@code om} has flushed far enough to evict every open file
   * table cache entry of the file.
   */
  private static void awaitOpenFileCacheFlush(OzoneManager om, String fileName) throws Exception {
    GenericTestUtils.waitFor(() -> {
      try {
        om.awaitDoubleBufferFlush();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return !hasCachedOpenFileEntry(om, fileName);
    }, 250, 10000);
  }

  /**
   * Does the OM's open file table cache still hold an entry for this file.
   */
  private static boolean hasCachedOpenFileEntry(OzoneManager om, String fileName) {
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>>> iterator =
        om.getMetadataManager().getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).cacheIterator();
    while (iterator.hasNext()) {
      if (iterator.next().getKey().getCacheKey().contains("/" + fileName + "/")) {
        return true;
      }
    }
    return false;
  }

  /**
   * Blocks until {@code laggingOm} has processed writer A's commits, while its flush is still held.
   * Ratis applies in order, so one more write transaction showing up on that OM means A's commits
   * were processed there already, and not merely still on their way.
   */
  private static void awaitLaggingOmCaughtUp(OzoneManager laggingOm) throws Exception {
    String markerName = uniqueObjectName("marker-");
    fs.create(new Path(markerDir, markerName), true).close();
    GenericTestUtils.waitFor(() -> hasCachedOpenFileEntry(laggingOm, markerName), 250, 10000);
  }

  /**
   * Waits until RocksDB on {@code om} holds the file as writer A committed it, so that an OM merely
   * lagging behind on apply or flush is not read as a diverged replica.
   */
  private static void awaitCommittedSize(OzoneManager om, OzoneBucket bucket, String fileName) throws Exception {
    try {
      GenericTestUtils.waitFor(() -> {
        try {
          return committedSize(om, bucket, fileName) == EXPECTED_SIZE;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }, 100, 30000);
    } catch (TimeoutException e) {
      assertEquals(EXPECTED_SIZE, committedSize(om, bucket, fileName),
          "OM " + om.getOMNodeId() + " never caught up on " + fileName);
    }
  }

  /** Size of the file as RocksDB holds it on one OM, -1 if absent. getSkipCache bypasses the table cache. */
  private static long committedSize(OzoneManager om, OzoneBucket bucket, String fileName) throws IOException {
    OMMetadataManager metadataManager = om.getMetadataManager();
    long volumeId = metadataManager.getVolumeId(bucket.getVolumeName());
    long bucketId = metadataManager.getBucketId(bucket.getVolumeName(), bucket.getName());
    // The file is created directly under the bucket, so the bucket is its parent.
    String dbKey = metadataManager.getOzonePathKey(volumeId, bucketId, bucketId, fileName);
    OmKeyInfo keyInfo = metadataManager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).getSkipCache(dbKey);
    return keyInfo == null ? -1 : keyInfo.getDataSize();
  }
}
