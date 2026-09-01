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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.BucketVersioningStatus;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmLCNoncurrentVersionExpiration;
import org.apache.hadoop.ozone.om.helpers.OmLCRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests which noncurrent versions the NoncurrentVersionExpiration rules pick.
 *
 * <p>Reclaiming what was picked is OMObjectVersionsReclaimRequest's job and is
 * tested there; this covers the choice itself.
 */
public class TestNoncurrentVersionSelector {

  private static final String VOLUME = "vol1";
  private static final String BUCKET = "bucket1";
  private static final int NO_LIMIT = Integer.MAX_VALUE;

  private OMMetadataManager omMetadataManager;
  private NoncurrentVersionSelector selector;

  @TempDir
  private File folder;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_OM_DB_DIRS, folder.getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(conf, null);
    selector = new NoncurrentVersionSelector(omMetadataManager);
  }

  private static OmBucketInfo versionedBucket() {
    return OmBucketInfo.newBuilder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setBucketLayout(BucketLayout.OBJECT_STORE)
        .setVersioningStatus(BucketVersioningStatus.ENABLED)
        .build();
  }

  private static List<OmLCRule> rules(Integer noncurrentDays,
      Integer newerNoncurrentVersions) throws Exception {
    OmLCNoncurrentVersionExpiration.Builder action =
        new OmLCNoncurrentVersionExpiration.Builder();
    if (noncurrentDays != null) {
      action.setNoncurrentDays(noncurrentDays);
    }
    if (newerNoncurrentVersions != null) {
      action.setNewerNoncurrentVersions(newerNoncurrentVersions);
    }
    OmLCNoncurrentVersionExpiration expiration = action.build();
    expiration.valid(0L);

    return Collections.singletonList(new OmLCRule.Builder()
        .setId("rule")
        .setEnabled(true)
        .setPrefix("")
        .addAction(expiration)
        .build());
  }

  private static long daysAgo(int days) {
    return System.currentTimeMillis() - TimeUnit.DAYS.toMillis(days);
  }

  private OmKeyInfo version(String keyName, long versionId, long modTime) {
    return new OmKeyInfo.Builder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(keyName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ONE))
        .setVersionId(versionId)
        .setModificationTime(modTime)
        .setCreationTime(modTime)
        .build();
  }

  /** A noncurrent version of a key in a bucket whose name extends BUCKET. */
  private void addNoncurrentInSiblingBucket(String bucketName, String keyName,
      long versionId, long noncurrentAt) throws Exception {
    OmKeyInfo info = new OmKeyInfo.Builder()
        .setVolumeName(VOLUME)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ONE))
        .setVersionId(versionId)
        .setModificationTime(noncurrentAt)
        .setCreationTime(noncurrentAt)
        .setNoncurrentTime(noncurrentAt)
        .build();
    omMetadataManager.getVersionedKeyTable().put(
        omMetadataManager.getVersionedOzoneKey(VOLUME, bucketName, keyName,
            versionId),
        info);
  }

  /** A noncurrent version stamped as having gone noncurrent at that time. */
  private void addNoncurrent(String keyName, long versionId,
      long noncurrentAt) throws Exception {
    addNoncurrent(keyName, versionId, noncurrentAt, noncurrentAt);
  }

  private void addNoncurrent(String keyName, long versionId, long modTime,
      long noncurrentAt) throws Exception {
    addNoncurrent(keyName, versionId, modTime, noncurrentAt, 0L);
  }

  private void addNoncurrent(String keyName, long versionId, long modTime,
      long noncurrentAt, long updateId) throws Exception {
    omMetadataManager.getVersionedKeyTable().put(
        omMetadataManager.getVersionedOzoneKey(VOLUME, BUCKET, keyName,
            versionId),
        version(keyName, versionId, modTime).toBuilder()
            .setNoncurrentTime(noncurrentAt)
            .setUpdateID(updateId)
            .build());
  }

  /** A version with no stamp, as a broken invariant would leave it. */
  private void addUnstampedNoncurrent(String keyName, long versionId,
      long modTime) throws Exception {
    omMetadataManager.getVersionedKeyTable().put(
        omMetadataManager.getVersionedOzoneKey(VOLUME, BUCKET, keyName,
            versionId),
        version(keyName, versionId, modTime));
  }

  private void addCurrent(String keyName, long versionId, long modTime)
      throws Exception {
    omMetadataManager.getKeyTable(BucketLayout.OBJECT_STORE).put(
        omMetadataManager.getOzoneKey(VOLUME, BUCKET, keyName),
        version(keyName, versionId, modTime));
  }

  /** The dbKeys of what a pass selected, in the order it met them. */
  private static List<String> keysOf(
      List<NoncurrentVersionSelector.ExpiredRecord> records) {
    return records.stream()
        .map(NoncurrentVersionSelector.ExpiredRecord::getDbKey)
        .collect(Collectors.toList());
  }

  private String versionKey(String keyName, long versionId) {
    return omMetadataManager.getVersionedOzoneKey(VOLUME, BUCKET, keyName,
        versionId);
  }

  // ---- NewerNoncurrentVersions ----

  /**
   * A dbKey names a position rather than a record, so what is selected carries
   * the updateID of the record the rules matched. The reclaim runs after
   * replication, by which time the key may hold something else.
   */
  @Test
  public void testWhatIsSelectedCarriesTheRecordsUpdateId() throws Exception {
    addCurrent("key", 500L, daysAgo(0));
    addNoncurrent("key", 300L, daysAgo(1), daysAgo(1), 4711L);
    addNoncurrent("key", 200L, daysAgo(2), daysAgo(2), 4712L);

    NoncurrentVersionSelector.Selection selection =
        selector.select(versionedBucket(), rules(null, 1), null, NO_LIMIT);

    assertEquals(Collections.singletonList(new
        NoncurrentVersionSelector.ExpiredRecord(versionKey("key", 200L),
            4712L)), selection.getExpiredVersions());
  }

  /** Versions are visited newest first, so the ones over the limit are oldest. */
  @Test
  public void testTheOldestVersionsOverTheLimitAreSelected() throws Exception {
    addCurrent("key", 500L, daysAgo(0));
    addNoncurrent("key", 400L, daysAgo(1));
    addNoncurrent("key", 300L, daysAgo(2));
    addNoncurrent("key", 200L, daysAgo(3));
    addNoncurrent("key", 100L, daysAgo(4));

    NoncurrentVersionSelector.Selection selection =
        selector.select(versionedBucket(), rules(null, 2), null, NO_LIMIT);

    assertEquals(Arrays.asList(versionKey("key", 200L),
        versionKey("key", 100L)), keysOf(selection.getExpiredVersions()));
    assertTrue(selection.isFinished());
  }

  /** The count restarts at every key, so one key cannot spend another's. */
  @Test
  public void testEachKeyIsCountedSeparately() throws Exception {
    for (String keyName : Arrays.asList("a", "b", "c")) {
      addCurrent(keyName, 500L, daysAgo(0));
      addNoncurrent(keyName, 300L, daysAgo(1));
      addNoncurrent(keyName, 200L, daysAgo(2));
    }

    NoncurrentVersionSelector.Selection selection =
        selector.select(versionedBucket(), rules(null, 1), null, NO_LIMIT);

    // one over the limit per key, never more
    assertEquals(Arrays.asList(versionKey("a", 200L), versionKey("b", 200L),
        versionKey("c", 200L)), keysOf(selection.getExpiredVersions()));
  }

  @Test
  public void testAKeyWithinTheLimitIsUntouched() throws Exception {
    addCurrent("key", 500L, daysAgo(0));
    addNoncurrent("key", 300L, daysAgo(1));

    NoncurrentVersionSelector.Selection selection =
        selector.select(versionedBucket(), rules(null, 5), null, NO_LIMIT);

    assertTrue(selection.getExpiredVersions().isEmpty());
  }

  // ---- NoncurrentDays ----

  /**
   * A version's age is counted from when it stopped being current, which the
   * record carries, and not from its own modification time.
   */
  @Test
  public void testAgeIsCountedFromWhenTheVersionWasSuperseded()
      throws Exception {
    addCurrent("key", 500L, daysAgo(1));
    // written long ago, but only superseded yesterday
    addNoncurrent("key", 400L, daysAgo(90), daysAgo(1));
    // written at the same time, and noncurrent ever since
    addNoncurrent("key", 300L, daysAgo(90), daysAgo(90));

    NoncurrentVersionSelector.Selection selection =
        selector.select(versionedBucket(), rules(30, null), null, NO_LIMIT);

    assertEquals(Collections.singletonList(versionKey("key", 300L)),
        keysOf(selection.getExpiredVersions()),
        "a version superseded yesterday is not 30 days noncurrent");
  }

  /**
   * The moment a version went noncurrent is fixed when it happens, so
   * removing the version that superseded it does not restart its clock. A
   * derived age would: with the version above gone, the current version is
   * the only thing left to date it against.
   */
  @Test
  public void testRemovingTheSupersedingVersionKeepsTheClock()
      throws Exception {
    addCurrent("key", 500L, daysAgo(1));
    // 400 superseded it 90 days ago and has since been reclaimed itself
    addNoncurrent("key", 300L, daysAgo(120), daysAgo(90));

    NoncurrentVersionSelector.Selection selection =
        selector.select(versionedBucket(), rules(30, null), null, NO_LIMIT);

    assertEquals(Collections.singletonList(versionKey("key", 300L)),
        keysOf(selection.getExpiredVersions()));
  }

  /**
   * Every record is stamped as it is moved into the versionedKeyTable, so an
   * unstamped one means that invariant is broken, and expiring on a broken
   * invariant would destroy data.
   */
  @Test
  public void testAnUnstampedVersionIsNotExpiredByAge() throws Exception {
    addCurrent("key", 500L, daysAgo(1));
    addUnstampedNoncurrent("key", 400L, daysAgo(90));
    addUnstampedNoncurrent("key", 300L, daysAgo(120));

    NoncurrentVersionSelector.Selection selection =
        selector.select(versionedBucket(), rules(30, null), null, NO_LIMIT);

    assertTrue(selection.getExpiredVersions().isEmpty());

    // the count rule does not depend on the stamp, so it still applies
    selection = selector.select(versionedBucket(), rules(null, 1), null,
        NO_LIMIT);

    assertEquals(Collections.singletonList(versionKey("key", 300L)),
        keysOf(selection.getExpiredVersions()));
  }

  /** Either limit on its own is enough, so each catches what the other misses. */
  @Test
  public void testCountAndAgeCombine() throws Exception {
    // 400 went noncurrent a day ago, 300 went noncurrent 100 days ago
    addCurrent("key", 500L, daysAgo(1));
    addNoncurrent("key", 400L, daysAgo(1));
    addNoncurrent("key", 300L, daysAgo(100));

    // age catches 300 even though the count limit is nowhere near
    NoncurrentVersionSelector.Selection byAge =
        selector.select(versionedBucket(), rules(30, 5), null, NO_LIMIT);
    assertEquals(Collections.singletonList(versionKey("key", 300L)),
        keysOf(byAge.getExpiredVersions()));

    // count catches 300 even though it is nowhere near the age limit
    NoncurrentVersionSelector.Selection byCount =
        selector.select(versionedBucket(), rules(3650, 1), null, NO_LIMIT);
    assertEquals(Collections.singletonList(versionKey("key", 300L)),
        keysOf(byCount.getExpiredVersions()));
  }

  // ---- resume ----

  /**
   * A pass stops at a key boundary and reports where to pick up, so the next
   * pass counts that key's versions from the start rather than mid-key.
   */
  @Test
  public void testAPassResumesAtAKeyBoundary() throws Exception {
    for (String keyName : Arrays.asList("a", "b", "c")) {
      addCurrent(keyName, 500L, daysAgo(0));
      addNoncurrent(keyName, 300L, daysAgo(1));
      addNoncurrent(keyName, 200L, daysAgo(2));
    }

    NoncurrentVersionSelector.Selection first =
        selector.select(versionedBucket(), rules(null, 1), null, 1);

    assertEquals(Collections.singletonList(versionKey("a", 200L)),
        keysOf(first.getExpiredVersions()));
    assertFalse(first.isFinished());
    // the boundary is b's newest version, never the middle of a key
    assertEquals(versionKey("b", 300L), first.getResumeFrom());

    NoncurrentVersionSelector.Selection second = selector.select(
        versionedBucket(), rules(null, 1), first.getResumeFrom(), NO_LIMIT);

    assertEquals(Arrays.asList(versionKey("b", 200L), versionKey("c", 200L)),
        keysOf(second.getExpiredVersions()));
    assertTrue(second.isFinished());
    assertNull(second.getResumeFrom());
  }

  @Test
  public void testAPassStopsAfterWalkingTheLimitEvenIfNothingExpires()
      throws Exception {
    // Nothing here expires: every key keeps two noncurrent versions and the
    // rule keeps five. Bounding the pass by what it selected would walk the
    // whole bucket in one go, never yielding and never saving progress.
    for (String keyName : Arrays.asList("a", "b", "c")) {
      addCurrent(keyName, 500L, daysAgo(0));
      addNoncurrent(keyName, 300L, daysAgo(1));
      addNoncurrent(keyName, 200L, daysAgo(2));
    }

    NoncurrentVersionSelector.Selection first =
        selector.select(versionedBucket(), rules(null, 5), null, 2);

    assertTrue(first.getExpiredVersions().isEmpty());
    assertFalse(first.isFinished());
    assertEquals(2, first.getVersionsScanned());
    assertEquals(versionKey("b", 300L), first.getResumeFrom());

    NoncurrentVersionSelector.Selection second = selector.select(
        versionedBucket(), rules(null, 5), first.getResumeFrom(), 2);

    assertEquals(2, second.getVersionsScanned());
    assertEquals(versionKey("c", 300L), second.getResumeFrom());

    NoncurrentVersionSelector.Selection third = selector.select(
        versionedBucket(), rules(null, 5), second.getResumeFrom(), 2);

    assertEquals(2, third.getVersionsScanned());
    assertTrue(third.isFinished());
  }

  @Test
  public void testTheScannedCountCoversSurvivorsAndExpiredAlike()
      throws Exception {
    addCurrent("a", 500L, daysAgo(0));
    addNoncurrent("a", 300L, daysAgo(1));
    addNoncurrent("a", 200L, daysAgo(2));
    addNoncurrent("a", 100L, daysAgo(3));

    NoncurrentVersionSelector.Selection selection =
        selector.select(versionedBucket(), rules(null, 1), null, NO_LIMIT);

    assertEquals(2, selection.getExpiredVersions().size());
    assertEquals(3, selection.getVersionsScanned());
  }

  @Test
  public void testAnEmptyBucketFinishesImmediately() throws Exception {
    NoncurrentVersionSelector.Selection selection =
        selector.select(versionedBucket(), rules(null, 1), null, NO_LIMIT);

    assertTrue(selection.getExpiredVersions().isEmpty());
    assertTrue(selection.isFinished());
    assertEquals(0, selection.getVersionsScanned());
  }

  /**
   * The scan is bounded by the bucket. Without the separator its prefix is
   * also a prefix of every bucket whose name extends it, and this bucket's
   * rules would expire another bucket's versions.
   */
  @Test
  public void testABucketWhoseNameExtendsThisOneIsNotScanned()
      throws Exception {
    addNoncurrent("key", 300L, daysAgo(1));
    addNoncurrent("key", 200L, daysAgo(2));
    addNoncurrentInSiblingBucket(BUCKET + "0", "key", 300L, daysAgo(1));
    addNoncurrentInSiblingBucket(BUCKET + "0", "key", 200L, daysAgo(2));

    NoncurrentVersionSelector.Selection selection =
        selector.select(versionedBucket(), rules(null, 1), null, NO_LIMIT);

    assertEquals(2, selection.getVersionsScanned());
    assertEquals(Collections.singletonList(versionKey("key", 200L)),
        keysOf(selection.getExpiredVersions()));
  }
}
