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

import static org.apache.hadoop.hdds.StringUtils.bytes2String;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_SEPARATOR;
import static org.apache.hadoop.ozone.om.OmSnapshotLocalDataYaml.YAML_FILE_EXTENSION;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.compress.utils.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshotLocalData;
import org.apache.hadoop.ozone.om.OmSnapshotLocalDataYaml;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.FlatResource;
import org.apache.hadoop.ozone.om.lock.HierarchicalResourceLockManager;
import org.apache.hadoop.ozone.om.lock.HierarchicalResourceLockManager.HierarchicalResourceLock;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataProvider;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager.WritableOmSnapshotLocalDataProvider;
import org.apache.hadoop.ozone.util.YamlSerializer;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.rocksdb.LiveFileMetaData;
import org.yaml.snakeyaml.Yaml;

/**
 * Test class for OmSnapshotLocalDataManager.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
public class TestOmSnapshotLocalDataManager {

  private static YamlSerializer<OmSnapshotLocalData> snapshotLocalDataYamlSerializer;
  private static List<String> lockCapturor;

  @Mock
  private OMMetadataManager omMetadataManager;

  @Mock
  private HierarchicalResourceLockManager lockManager;

  @Mock
  private RDBStore rdbStore;

  @Mock
  private RDBStore snapshotStore;

  @TempDir
  private Path tempDir;

  private OmSnapshotLocalDataManager localDataManager;
  private AutoCloseable mocks;

  private File snapshotsDir;

  private static final String READ_LOCK_MESSAGE_ACQUIRE = "readLock acquire";
  private static final String READ_LOCK_MESSAGE_UNLOCK = "readLock unlock";
  private static final String WRITE_LOCK_MESSAGE_ACQUIRE = "writeLock acquire";
  private static final String WRITE_LOCK_MESSAGE_UNLOCK = "writeLock unlock";

  @BeforeAll
  public static void setupClass() {
    snapshotLocalDataYamlSerializer = new YamlSerializer<OmSnapshotLocalData>(
        new OmSnapshotLocalDataYaml.YamlFactory()) {

      @Override
      public void computeAndSetChecksum(Yaml yaml, OmSnapshotLocalData data) throws IOException {
        data.computeAndSetChecksum(yaml);
      }
    };
    lockCapturor = new ArrayList<>();
  }

  @AfterAll
  public static void teardownClass() {
    snapshotLocalDataYamlSerializer.close();
    snapshotLocalDataYamlSerializer = null;
  }

  @BeforeEach
  public void setUp() throws IOException {
    mocks = MockitoAnnotations.openMocks(this);
    
    // Setup mock behavior
    when(omMetadataManager.getStore()).thenReturn(rdbStore);
    when(omMetadataManager.getHierarchicalLockManager()).thenReturn(lockManager);
    this.snapshotsDir = tempDir.resolve("snapshots").toFile();
    FileUtils.deleteDirectory(snapshotsDir);
    assertTrue(snapshotsDir.exists() || snapshotsDir.mkdirs());
    File dbLocation = tempDir.resolve("db").toFile();
    FileUtils.deleteDirectory(dbLocation);
    assertTrue(dbLocation.exists() || dbLocation.mkdirs());
    mockLockManager();

    when(rdbStore.getSnapshotsParentDir()).thenReturn(snapshotsDir.getAbsolutePath());
    when(rdbStore.getDbLocation()).thenReturn(dbLocation);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (localDataManager != null) {
      localDataManager.close();
    }
    if (mocks != null) {
      mocks.close();
    }
  }

  private String getReadLockMessageAcquire(UUID snapshotId) {
    return READ_LOCK_MESSAGE_ACQUIRE + " " + FlatResource.SNAPSHOT_LOCAL_DATA_LOCK + " " + snapshotId;
  }

  private String getReadLockMessageRelease(UUID snapshotId) {
    return READ_LOCK_MESSAGE_UNLOCK + " " + FlatResource.SNAPSHOT_LOCAL_DATA_LOCK + " " + snapshotId;
  }

  private String getWriteLockMessageAcquire(UUID snapshotId) {
    return WRITE_LOCK_MESSAGE_ACQUIRE + " " + FlatResource.SNAPSHOT_LOCAL_DATA_LOCK + " " + snapshotId;
  }

  private String getWriteLockMessageRelease(UUID snapshotId) {
    return WRITE_LOCK_MESSAGE_UNLOCK + " " + FlatResource.SNAPSHOT_LOCAL_DATA_LOCK + " " + snapshotId;
  }

  private HierarchicalResourceLock getHierarchicalResourceLock(FlatResource resource, String key, boolean isWriteLock) {
    return new HierarchicalResourceLock() {
      @Override
      public boolean isLockAcquired() {
        return true;
      }

      @Override
      public void close() {
        if (isWriteLock) {
          lockCapturor.add(WRITE_LOCK_MESSAGE_UNLOCK + " " + resource + " " + key);
        } else {
          lockCapturor.add(READ_LOCK_MESSAGE_UNLOCK + " " + resource + " " + key);
        }
      }
    };
  }

  private void mockLockManager() throws IOException {
    lockCapturor.clear();
    reset(lockManager);
    when(lockManager.acquireReadLock(any(FlatResource.class), anyString()))
        .thenAnswer(i -> {
          lockCapturor.add(READ_LOCK_MESSAGE_ACQUIRE + " " + i.getArgument(0) + " " + i.getArgument(1));
          return getHierarchicalResourceLock(i.getArgument(0), i.getArgument(1), false);
        });
    when(lockManager.acquireWriteLock(any(FlatResource.class), anyString()))
        .thenAnswer(i -> {
          lockCapturor.add(WRITE_LOCK_MESSAGE_ACQUIRE + " " + i.getArgument(0) + " " + i.getArgument(1));
          return getHierarchicalResourceLock(i.getArgument(0), i.getArgument(1), true);
        });
  }

  private List<UUID> createSnapshotLocalData(OmSnapshotLocalDataManager snapshotLocalDataManager,
      int numberOfSnapshots) throws IOException {
    SnapshotInfo previousSnapshotInfo = null;
    int counter = 0;
    Map<String, List<LiveFileMetaData>> liveFileMetaDataMap = new HashMap<>();
    liveFileMetaDataMap.put(KEY_TABLE,
        Lists.newArrayList(createMockLiveFileMetaData("file1.sst", KEY_TABLE, "key1", "key2")));
    liveFileMetaDataMap.put(FILE_TABLE, Lists.newArrayList(createMockLiveFileMetaData("file2.sst", FILE_TABLE, "key1",
        "key2")));
    liveFileMetaDataMap.put(DIRECTORY_TABLE, Lists.newArrayList(createMockLiveFileMetaData("file2.sst",
        DIRECTORY_TABLE, "key1", "key2")));
    liveFileMetaDataMap.put("col1", Lists.newArrayList(createMockLiveFileMetaData("file2.sst", "col1", "key1",
        "key2")));
    List<UUID> snapshotIds = new ArrayList<>();
    for (int i = 0; i < numberOfSnapshots; i++) {
      UUID snapshotId = UUID.randomUUID();
      SnapshotInfo snapshotInfo = createMockSnapshotInfo(snapshotId, previousSnapshotInfo == null ? null
          : previousSnapshotInfo.getSnapshotId());
      mockSnapshotStore(snapshotId, liveFileMetaDataMap.values().stream()
          .flatMap(Collection::stream).collect(Collectors.toList()));
      snapshotLocalDataManager.createNewOmSnapshotLocalDataFile(snapshotStore, snapshotInfo);
      previousSnapshotInfo = snapshotInfo;
      for (Map.Entry<String, List<LiveFileMetaData>> tableEntry : liveFileMetaDataMap.entrySet()) {
        String table = tableEntry.getKey();
        tableEntry.getValue().add(createMockLiveFileMetaData("file" + counter++ + ".sst", table, "key1", "key4"));
      }
      snapshotIds.add(snapshotId);
    }
    return snapshotIds;
  }

  private void mockSnapshotStore(UUID snapshotId, List<LiveFileMetaData> sstFiles) throws RocksDatabaseException {
    // Setup snapshot store mock
    File snapshotDbLocation = OmSnapshotManager.getSnapshotPath(omMetadataManager, snapshotId, 0).toFile();
    assertTrue(snapshotDbLocation.exists() || snapshotDbLocation.mkdirs());

    when(snapshotStore.getDbLocation()).thenReturn(snapshotDbLocation);
    RocksDatabase rocksDatabase = mock(RocksDatabase.class);
    when(snapshotStore.getDb()).thenReturn(rocksDatabase);
    when(rocksDatabase.getLiveFilesMetaData()).thenReturn(sstFiles);
  }

  /**
   * Checks lock orders taken i.e. while reading a snapshot against the previous snapshot.
   * Depending on read or write locks are acquired on the snapshotId and read lock is acquired on the previous
   * snapshot. Once the instance is closed the read lock on previous snapshot is released followed by releasing the
   * lock on the snapshotId.
   * @param read
   * @throws IOException
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testLockOrderingAgainstAnotherSnapshot(boolean read) throws IOException {
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    List<UUID> snapshotIds = new ArrayList<>();
    snapshotIds.add(null);
    snapshotIds.addAll(createSnapshotLocalData(localDataManager, 20));
    for (int start = 0; start < snapshotIds.size(); start++) {
      for (int end = start + 1; end < snapshotIds.size(); end++) {
        UUID startSnapshotId = snapshotIds.get(start);
        UUID endSnapshotId = snapshotIds.get(end);
        lockCapturor.clear();
        int logCaptorIdx = 0;
        try (ReadableOmSnapshotLocalDataProvider omSnapshotLocalDataProvider =
                 read ? localDataManager.getOmSnapshotLocalData(endSnapshotId, startSnapshotId) :
                     localDataManager.getWritableOmSnapshotLocalData(endSnapshotId, startSnapshotId)) {
          OmSnapshotLocalData snapshotLocalData = omSnapshotLocalDataProvider.getSnapshotLocalData();
          OmSnapshotLocalData previousSnapshot = omSnapshotLocalDataProvider.getPreviousSnapshotLocalData();
          assertEquals(endSnapshotId, snapshotLocalData.getSnapshotId());
          if (startSnapshotId == null) {
            assertNull(previousSnapshot);
            assertNull(snapshotLocalData.getPreviousSnapshotId());
            continue;
          }
          assertEquals(startSnapshotId, previousSnapshot.getSnapshotId());
          assertEquals(startSnapshotId, snapshotLocalData.getPreviousSnapshotId());
          if (read) {
            assertEquals(getReadLockMessageAcquire(endSnapshotId), lockCapturor.get(logCaptorIdx++));
          } else {
            assertEquals(getWriteLockMessageAcquire(endSnapshotId), lockCapturor.get(logCaptorIdx++));
          }
          int idx = end - 1;
          UUID previousSnapId = snapshotIds.get(idx--);
          assertEquals(getReadLockMessageAcquire(previousSnapId), lockCapturor.get(logCaptorIdx++));
          while (idx >= start) {
            UUID prevPrevSnapId = snapshotIds.get(idx--);
            assertEquals(getReadLockMessageAcquire(prevPrevSnapId), lockCapturor.get(logCaptorIdx++));
            assertEquals(getReadLockMessageRelease(previousSnapId), lockCapturor.get(logCaptorIdx++));
            previousSnapId = prevPrevSnapId;
          }
        }
        assertEquals(getReadLockMessageRelease(startSnapshotId), lockCapturor.get(logCaptorIdx++));
        if (read) {
          assertEquals(getReadLockMessageRelease(endSnapshotId), lockCapturor.get(logCaptorIdx++));
        } else {
          assertEquals(getWriteLockMessageRelease(endSnapshotId), lockCapturor.get(logCaptorIdx++));
        }
        assertEquals(lockCapturor.size(), logCaptorIdx);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testVersionLockResolution(boolean read) throws IOException {
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    List<UUID> snapshotIds = createSnapshotLocalData(localDataManager, 5);
    for (int snapIdx = 0; snapIdx < snapshotIds.size(); snapIdx++) {
      UUID snapId = snapshotIds.get(snapIdx);
      UUID expectedPreviousSnapId = snapIdx - 1 >= 0 ? snapshotIds.get(snapIdx - 1) : null;
      lockCapturor.clear();
      int logCaptorIdx = 0;
      try (ReadableOmSnapshotLocalDataProvider omSnapshotLocalDataProvider =
               read ? localDataManager.getOmSnapshotLocalData(snapId) :
                   localDataManager.getWritableOmSnapshotLocalData(snapId)) {
        OmSnapshotLocalData snapshotLocalData = omSnapshotLocalDataProvider.getSnapshotLocalData();
        OmSnapshotLocalData previousSnapshot = omSnapshotLocalDataProvider.getPreviousSnapshotLocalData();
        assertEquals(snapId, snapshotLocalData.getSnapshotId());
        assertEquals(expectedPreviousSnapId, previousSnapshot == null ? null :
            previousSnapshot.getSnapshotId());
        if (read) {
          assertEquals(getReadLockMessageAcquire(snapId), lockCapturor.get(logCaptorIdx++));
        } else {
          assertEquals(getWriteLockMessageAcquire(snapId), lockCapturor.get(logCaptorIdx++));
        }
        if (expectedPreviousSnapId != null) {
          assertEquals(getReadLockMessageAcquire(expectedPreviousSnapId), lockCapturor.get(logCaptorIdx++));
        }
      }
      if (expectedPreviousSnapId != null) {
        assertEquals(getReadLockMessageRelease(expectedPreviousSnapId), lockCapturor.get(logCaptorIdx++));
      }
      if (read) {
        assertEquals(getReadLockMessageRelease(snapId), lockCapturor.get(logCaptorIdx++));
      } else {
        assertEquals(getWriteLockMessageRelease(snapId), lockCapturor.get(logCaptorIdx++));
      }
      assertEquals(lockCapturor.size(), logCaptorIdx);
    }
  }

  @Test
  public void testWriteVersionAdditionValidationWithoutPreviousSnapshotVersionExisting() throws IOException {
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    List<UUID> snapshotIds = createSnapshotLocalData(localDataManager, 2);
    UUID snapId = snapshotIds.get(1);
    try (WritableOmSnapshotLocalDataProvider omSnapshotLocalDataProvider =
             localDataManager.getWritableOmSnapshotLocalData(snapId)) {
      OmSnapshotLocalData snapshotLocalData = omSnapshotLocalDataProvider.getSnapshotLocalData();
      snapshotLocalData.addVersionSSTFileInfos(Lists.newArrayList(createMockLiveFileMetaData("file1.sst", KEY_TABLE,
          "key1", "key2")), 3);

      IOException ex = assertThrows(IOException.class, omSnapshotLocalDataProvider::commit);
      assertTrue(ex.getMessage().contains("since previous snapshot with version hasn't been loaded"));
    }
  }

  @Test
  public void testUpdateTransactionInfo() throws IOException {
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    TransactionInfo transactionInfo = TransactionInfo.valueOf(ThreadLocalRandom.current().nextLong(),
        ThreadLocalRandom.current().nextLong());
    UUID snapshotId = createSnapshotLocalData(localDataManager, 1).get(0);
    try (WritableOmSnapshotLocalDataProvider snap = localDataManager.getWritableOmSnapshotLocalData(snapshotId)) {
      OmSnapshotLocalData snapshotLocalData = snap.getSnapshotLocalData();
      assertNull(snapshotLocalData.getTransactionInfo());
      snap.setTransactionInfo(transactionInfo);
      snap.commit();
    }

    try (ReadableOmSnapshotLocalDataProvider snap = localDataManager.getOmSnapshotLocalData(snapshotId)) {
      OmSnapshotLocalData snapshotLocalData = snap.getSnapshotLocalData();
      assertEquals(transactionInfo, snapshotLocalData.getTransactionInfo());
    }
  }

  @Test
  public void testAddVersionFromRDB() throws IOException {
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    List<UUID> snapshotIds = createSnapshotLocalData(localDataManager, 2);
    addVersionsToLocalData(localDataManager, snapshotIds.get(0), ImmutableMap.of(4, 5, 6, 8));
    UUID snapId = snapshotIds.get(1);
    List<LiveFileMetaData> newVersionSstFiles =
        Lists.newArrayList(createMockLiveFileMetaData("file5.sst", KEY_TABLE, "key1", "key2"),
        createMockLiveFileMetaData("file6.sst", FILE_TABLE, "key1", "key2"),
        createMockLiveFileMetaData("file7.sst", KEY_TABLE, "key1", "key2"),
        createMockLiveFileMetaData("file1.sst", "col1", "key1", "key2"));
    try (WritableOmSnapshotLocalDataProvider snap =
             localDataManager.getWritableOmSnapshotLocalData(snapId)) {
      mockSnapshotStore(snapId, newVersionSstFiles);
      snap.addSnapshotVersion(snapshotStore);
      snap.commit();
    }
    validateVersions(localDataManager, snapId, 1, Sets.newHashSet(0, 1));
    try (ReadableOmSnapshotLocalDataProvider snap = localDataManager.getOmSnapshotLocalData(snapId)) {
      OmSnapshotLocalData snapshotLocalData = snap.getSnapshotLocalData();
      OmSnapshotLocalData.VersionMeta versionMeta = snapshotLocalData.getVersionSstFileInfos().get(1);
      assertEquals(6, versionMeta.getPreviousSnapshotVersion());
      List<SstFileInfo> expectedLiveFileMetaData =
          newVersionSstFiles.subList(0, 3).stream().map(SstFileInfo::new).collect(Collectors.toList());
      assertEquals(expectedLiveFileMetaData, versionMeta.getSstFiles());
    }
  }

  private void validateVersions(OmSnapshotLocalDataManager snapshotLocalDataManager, UUID snapId, int expectedVersion,
      Set<Integer> expectedVersions) throws IOException {
    try (ReadableOmSnapshotLocalDataProvider snap = snapshotLocalDataManager.getOmSnapshotLocalData(snapId)) {
      assertEquals(expectedVersion, snap.getSnapshotLocalData().getVersion());
      assertEquals(expectedVersions, snap.getSnapshotLocalData().getVersionSstFileInfos().keySet());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWriteWithChainUpdate(boolean previousSnapshotExisting) throws IOException {
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    List<UUID> snapshotIds = createSnapshotLocalData(localDataManager, 3 + (previousSnapshotExisting ? 1 : 0));
    int snapshotIdx = 1 + (previousSnapshotExisting ? 1 : 0);
    for (UUID snapshotId : snapshotIds) {
      addVersionsToLocalData(localDataManager, snapshotId, ImmutableMap.of(1, 1));
    }

    UUID snapshotId = snapshotIds.get(snapshotIdx);
    UUID toUpdatePreviousSnapshotId = snapshotIdx - 2 >= 0 ? snapshotIds.get(snapshotIdx - 2) : null;

    try (WritableOmSnapshotLocalDataProvider snap =
             localDataManager.getWritableOmSnapshotLocalData(snapshotId, toUpdatePreviousSnapshotId)) {
      assertFalse(snap.needsDefrag());
      snap.commit();
      assertTrue(snap.needsDefrag());
    }
    try (ReadableOmSnapshotLocalDataProvider snap =
             localDataManager.getOmSnapshotLocalData(snapshotId)) {
      assertEquals(toUpdatePreviousSnapshotId, snap.getSnapshotLocalData().getPreviousSnapshotId());
      assertTrue(snap.needsDefrag());
    }
  }

  /**
   * Validates write-time version propagation and removal rules when the previous
   * snapshot already has a concrete version recorded.
   *
   * Test flow:
   * 1) Create two snapshots in a chain: {@code prevSnapId -> snapId}.
   * 2) For {@code prevSnapId}: set {@code version=3} and add SST metadata for version {@code 0}; commit.
   * 3) For {@code snapId}: set {@code version=4} and add SST metadata for version {@code 4}; commit.
   *    After commit, versions resolve to {@code prev.version=4} and {@code snap.version=5}, and their
   *    version maps are {@code {0,4}} and {@code {0,5}} respectively (base version 0 plus the current one).
   * 4) If {@code nextVersionExisting} is {@code true}:
   *    - Attempt to remove version {@code 4} from {@code prevSnapId}; expect {@link IOException} because
   *      the successor snapshot still exists at version {@code 5} and depends on {@code prevSnapId}.
   *    - Validate that versions and version maps remain unchanged.
   *    Else ({@code false}):
   *    - Remove version {@code 5} from {@code snapId} and commit, then remove version {@code 4} from
   *      {@code prevSnapId} and commit.
   *    - Validate that both snapshots now only contain the base version {@code 0}.
   *
   * This ensures a snapshot cannot drop a version that still has a dependent successor, and that removals
   * are allowed only after dependents are cleared.
   *
   * @param nextVersionExisting whether the successor snapshot's version still exists ({@code true}) or is
   *                            removed first ({@code false})
   * @throws IOException if commit validation fails as expected in the protected case
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWriteVersionValidation(boolean nextVersionExisting) throws IOException {
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    List<UUID> snapshotIds = createSnapshotLocalData(localDataManager, 3);
    UUID prevSnapId = snapshotIds.get(0);
    UUID snapId = snapshotIds.get(1);
    UUID nextSnapId = snapshotIds.get(2);
    addVersionsToLocalData(localDataManager, prevSnapId, ImmutableMap.of(4, 1));
    addVersionsToLocalData(localDataManager, snapId, ImmutableMap.of(5, 4));
    addVersionsToLocalData(localDataManager, nextSnapId, ImmutableMap.of(6, 0));

    validateVersions(localDataManager, snapId, 5, Sets.newHashSet(0, 5));
    validateVersions(localDataManager, prevSnapId, 4, Sets.newHashSet(0, 4));

    if (nextVersionExisting) {
      try (WritableOmSnapshotLocalDataProvider prevSnap = localDataManager.getWritableOmSnapshotLocalData(prevSnapId)) {
        prevSnap.removeVersion(4);
        IOException ex = assertThrows(IOException.class, prevSnap::commit);
        assertTrue(ex.getMessage().contains("Cannot remove Snapshot " + prevSnapId + " with version : 4 since it " +
            "still has predecessors"));
      }
      validateVersions(localDataManager, snapId, 5, Sets.newHashSet(0, 5));
      validateVersions(localDataManager, prevSnapId, 4, Sets.newHashSet(0, 4));
    } else {
      try (WritableOmSnapshotLocalDataProvider snap = localDataManager.getWritableOmSnapshotLocalData(snapId)) {
        snap.removeVersion(5);
        snap.commit();
      }

      try (WritableOmSnapshotLocalDataProvider prevSnap = localDataManager.getWritableOmSnapshotLocalData(prevSnapId)) {
        prevSnap.removeVersion(4);
        prevSnap.commit();
      }
      validateVersions(localDataManager, snapId, 5, Sets.newHashSet(0));
      validateVersions(localDataManager, prevSnapId, 4, Sets.newHashSet(0));
      // Check next snapshot is able to resolve to previous snapshot.
      try (ReadableOmSnapshotLocalDataProvider nextSnap = localDataManager.getOmSnapshotLocalData(nextSnapId,
          prevSnapId)) {
        OmSnapshotLocalData snapshotLocalData = nextSnap.getSnapshotLocalData();
        assertEquals(prevSnapId, snapshotLocalData.getPreviousSnapshotId());
        snapshotLocalData.getVersionSstFileInfos()
            .forEach((version, versionMeta) -> {
              assertEquals(0, versionMeta.getPreviousSnapshotVersion());
            });
      }
    }
  }

  private void addVersionsToLocalData(OmSnapshotLocalDataManager snapshotLocalDataManager,
      UUID snapId, Map<Integer, Integer> versionMap) throws IOException {
    try (WritableOmSnapshotLocalDataProvider snap = snapshotLocalDataManager.getWritableOmSnapshotLocalData(snapId)) {
      OmSnapshotLocalData snapshotLocalData = snap.getSnapshotLocalData();
      for (Map.Entry<Integer, Integer> version : versionMap.entrySet().stream()
          .sorted(Map.Entry.comparingByKey()).collect(Collectors.toList())) {
        snapshotLocalData.setVersion(version.getKey() - 1);
        snapshotLocalData.addVersionSSTFileInfos(ImmutableList.of(createMockLiveFileMetaData("file" + version +
            ".sst", KEY_TABLE, "key1", "key2")), version.getValue());
      }
      mockSnapshotStore(snapId, ImmutableList.of(createMockLiveFileMetaData("file"
          + snapshotLocalData.getVersion() + 1 + ".sst", KEY_TABLE, "key1", "key2")));
      snap.addSnapshotVersion(snapshotStore);
      snap.removeVersion(snapshotLocalData.getVersion());
      snapshotLocalData.setVersion(snapshotLocalData.getVersion() - 1);
      snap.commit();
    }
    try (ReadableOmSnapshotLocalDataProvider snap = snapshotLocalDataManager.getOmSnapshotLocalData(snapId)) {
      OmSnapshotLocalData snapshotLocalData = snap.getSnapshotLocalData();
      for (int version : versionMap.keySet()) {
        assertTrue(snapshotLocalData.getVersionSstFileInfos().containsKey(version));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  public void testNeedsDefrag(int previousVersion) throws IOException {
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    List<UUID> snapshotIds = createSnapshotLocalData(localDataManager, 2);
    for (UUID snapshotId : snapshotIds) {
      try (ReadableOmSnapshotLocalDataProvider snap = localDataManager.getOmSnapshotLocalData(snapshotId)) {
        assertTrue(snap.needsDefrag());
      }
    }
    addVersionsToLocalData(localDataManager, snapshotIds.get(0), ImmutableMap.of(1, 1, 2, 2, 3, 3));
    try (ReadableOmSnapshotLocalDataProvider snap = localDataManager.getOmSnapshotLocalData(snapshotIds.get(0))) {
      assertFalse(snap.needsDefrag());
    }
    addVersionsToLocalData(localDataManager, snapshotIds.get(1), ImmutableMap.of(1, 3, 2, previousVersion));
    try (ReadableOmSnapshotLocalDataProvider snap = localDataManager.getOmSnapshotLocalData(snapshotIds.get(1))) {
      assertEquals(previousVersion < snap.getPreviousSnapshotLocalData().getVersion(), snap.needsDefrag());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testVersionResolution(boolean read) throws IOException {
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    List<UUID> snapshotIds = createSnapshotLocalData(localDataManager, 5);
    List<Map<Integer, Integer>> versionMaps = Arrays.asList(
        ImmutableMap.of(4, 1, 6, 3, 8, 9, 11, 15),
        ImmutableMap.of(5, 4, 6, 8, 10, 11),
        ImmutableMap.of(1, 5, 3, 5, 8, 10),
        ImmutableMap.of(1, 1, 2, 3, 5, 8),
        ImmutableMap.of(1, 1, 11, 2, 20, 5, 30, 2)
    );
    for (int i = 0; i < snapshotIds.size(); i++) {
      addVersionsToLocalData(localDataManager, snapshotIds.get(i), versionMaps.get(i));
    }
    for (int start = 0; start < snapshotIds.size(); start++) {
      for (int end = start + 1; end < snapshotIds.size(); end++) {
        UUID prevSnapId = snapshotIds.get(start);
        UUID snapId = snapshotIds.get(end);
        Map<Integer, Integer> versionMap = new HashMap<>(versionMaps.get(end));
        versionMap.put(0, 0);
        for (int idx = end - 1; idx > start; idx--) {
          for (Map.Entry<Integer, Integer> version : versionMap.entrySet()) {
            version.setValue(versionMaps.get(idx).getOrDefault(version.getValue(), 0));
          }
        }
        try (ReadableOmSnapshotLocalDataProvider snap = read ?
            localDataManager.getOmSnapshotLocalData(snapId, prevSnapId) :
            localDataManager.getWritableOmSnapshotLocalData(snapId, prevSnapId)) {
          OmSnapshotLocalData snapshotLocalData = snap.getSnapshotLocalData();
          OmSnapshotLocalData prevSnapshotLocalData = snap.getPreviousSnapshotLocalData();
          assertEquals(prevSnapshotLocalData.getSnapshotId(), snapshotLocalData.getPreviousSnapshotId());
          assertEquals(prevSnapId, snapshotLocalData.getPreviousSnapshotId());
          assertEquals(snapId, snapshotLocalData.getSnapshotId());
          assertTrue(snapshotLocalData.getVersionSstFileInfos().size() > 1);
          snapshotLocalData.getVersionSstFileInfos()
              .forEach((version, versionMeta) -> {
                assertEquals(versionMap.get(version), versionMeta.getPreviousSnapshotVersion());
              });
        }
      }
    }
  }

  @Test
  public void testConstructor() throws IOException {
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    assertNotNull(localDataManager);
  }

  @Test
  public void testGetSnapshotLocalPropertyYamlPathWithSnapshotInfo() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    SnapshotInfo snapshotInfo = createMockSnapshotInfo(snapshotId, null);
    
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);

    File yamlPath = new File(localDataManager.getSnapshotLocalPropertyYamlPath(snapshotInfo));
    assertNotNull(yamlPath);
    Path expectedYamlPath = Paths.get(snapshotsDir.getAbsolutePath(), "db" + OM_SNAPSHOT_SEPARATOR + snapshotId
        + YAML_FILE_EXTENSION);
    assertEquals(expectedYamlPath.toAbsolutePath().toString(), yamlPath.getAbsolutePath());
  }

  @Test
  public void testCreateNewSnapshotLocalYaml() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    SnapshotInfo snapshotInfo = createMockSnapshotInfo(snapshotId, null);

    Map<String, List<String>> expNotDefraggedSSTFileList = new TreeMap<>();
    OmSnapshotLocalData.VersionMeta notDefraggedVersionMeta = new OmSnapshotLocalData.VersionMeta(0,
        ImmutableList.of(new SstFileInfo("dt1", "k1", "k2", DIRECTORY_TABLE),
            new SstFileInfo("dt2", "k1", "k2", DIRECTORY_TABLE),
            new SstFileInfo("ft1", "k1", "k2", FILE_TABLE),
            new SstFileInfo("ft2", "k1", "k2", FILE_TABLE),
            new SstFileInfo("kt1", "k1", "k2", KEY_TABLE),
            new SstFileInfo("kt2", "k1", "k2", KEY_TABLE)));
    expNotDefraggedSSTFileList.put(KEY_TABLE, Stream.of("kt1", "kt2").collect(Collectors.toList()));
    expNotDefraggedSSTFileList.put(FILE_TABLE, Stream.of("ft1", "ft2").collect(Collectors.toList()));
    expNotDefraggedSSTFileList.put(DIRECTORY_TABLE, Stream.of("dt1", "dt2").collect(Collectors.toList()));

    List<LiveFileMetaData> mockedLiveFiles = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : expNotDefraggedSSTFileList.entrySet()) {
      String cfname = entry.getKey();
      for (String fname : entry.getValue()) {
        mockedLiveFiles.add(createMockLiveFileMetaData("/" + fname + ".sst", cfname, "k1", "k2"));
      }
    }
    // Add some other column families and files that should be ignored
    mockedLiveFiles.add(createMockLiveFileMetaData("ot1.sst", "otherTable", "k1", "k2"));
    mockedLiveFiles.add(createMockLiveFileMetaData("ot2.sst", "otherTable", "k1", "k2"));

    mockSnapshotStore(snapshotId, mockedLiveFiles);
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    Path snapshotYaml = Paths.get(localDataManager.getSnapshotLocalPropertyYamlPath(snapshotInfo));
    // Create an existing YAML file for the snapshot
    assertTrue(snapshotYaml.toFile().createNewFile());
    assertEquals(0, Files.size(snapshotYaml));
    // Create a new YAML file for the snapshot
    localDataManager.createNewOmSnapshotLocalDataFile(snapshotStore, snapshotInfo);
    // Verify that previous file was overwritten
    assertTrue(Files.exists(snapshotYaml));
    assertTrue(Files.size(snapshotYaml) > 0);
    // Verify the contents of the YAML file
    OmSnapshotLocalData localData = localDataManager.getOmSnapshotLocalData(snapshotYaml.toFile());
    assertNotNull(localData);
    assertEquals(0, localData.getVersion());
    assertEquals(notDefraggedVersionMeta, localData.getVersionSstFileInfos().get(0));
    assertFalse(localData.getSstFiltered());
    assertEquals(0L, localData.getLastDefragTime());
    assertTrue(localData.getNeedsDefrag());
    assertEquals(1, localData.getVersionSstFileInfos().size());
  }

  @Test
  public void testCreateNewOmSnapshotLocalDataFile() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    SnapshotInfo snapshotInfo = createMockSnapshotInfo(snapshotId, null);
    // Setup snapshot store mock

    List<LiveFileMetaData> sstFiles = new ArrayList<>();
    sstFiles.add(createMockLiveFileMetaData("file1.sst", KEY_TABLE, "key1", "key7"));
    sstFiles.add(createMockLiveFileMetaData("file2.sst", KEY_TABLE, "key3", "key9"));
    sstFiles.add(createMockLiveFileMetaData("file3.sst", FILE_TABLE, "key1", "key7"));
    sstFiles.add(createMockLiveFileMetaData("file4.sst", FILE_TABLE, "key1", "key7"));
    sstFiles.add(createMockLiveFileMetaData("file5.sst", DIRECTORY_TABLE, "key1", "key7"));
    sstFiles.add(createMockLiveFileMetaData("file6.sst", "colFamily1", "key1", "key7"));
    List<SstFileInfo> sstFileInfos = IntStream.range(0, sstFiles.size() - 1)
        .mapToObj(sstFiles::get).map(lfm ->
            new SstFileInfo(lfm.fileName().replace(".sst", ""),
            bytes2String(lfm.smallestKey()),
            bytes2String(lfm.largestKey()), bytes2String(lfm.columnFamilyName()))).collect(Collectors.toList());
    mockSnapshotStore(snapshotId, sstFiles);

    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);

    localDataManager.createNewOmSnapshotLocalDataFile(snapshotStore, snapshotInfo);
    
    // Verify file was created
    OmSnapshotLocalData.VersionMeta versionMeta;
    try (ReadableOmSnapshotLocalDataProvider snapshotLocalData = localDataManager.getOmSnapshotLocalData(snapshotId)) {
      assertEquals(1, snapshotLocalData.getSnapshotLocalData().getVersionSstFileInfos().size());
      versionMeta = snapshotLocalData.getSnapshotLocalData().getVersionSstFileInfos().get(0);
      OmSnapshotLocalData.VersionMeta expectedVersionMeta =
          new OmSnapshotLocalData.VersionMeta(0, sstFileInfos);
      assertEquals(expectedVersionMeta, versionMeta);
      // New Snapshot create needs to be defragged always.
      assertTrue(snapshotLocalData.needsDefrag());
    }
  }

  @Test
  public void testGetOmSnapshotLocalDataWithSnapshotInfo() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    SnapshotInfo snapshotInfo = createMockSnapshotInfo(snapshotId, null);
    
    // Create and write snapshot local data file
    OmSnapshotLocalData localData = createMockLocalData(snapshotId, null);
    
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);

    // Write the file manually for testing
    Path yamlPath = Paths.get(localDataManager.getSnapshotLocalPropertyYamlPath(snapshotInfo.getSnapshotId()));
    writeLocalDataToFile(localData, yamlPath);
    
    // Test retrieval
    try (ReadableOmSnapshotLocalDataProvider retrieved = localDataManager.getOmSnapshotLocalData(snapshotInfo)) {
      assertNotNull(retrieved.getSnapshotLocalData());
      assertEquals(snapshotId, retrieved.getSnapshotLocalData().getSnapshotId());
    }
  }

  @Test
  public void testGetOmSnapshotLocalDataWithMismatchedSnapshotId() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    UUID wrongSnapshotId = UUID.randomUUID();
    
    // Create local data with wrong snapshot ID
    OmSnapshotLocalData localData = createMockLocalData(wrongSnapshotId, null);
    
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);

    Path yamlPath = Paths.get(localDataManager.getSnapshotLocalPropertyYamlPath(snapshotId));
    writeLocalDataToFile(localData, yamlPath);
    // Should throw IOException due to mismatched IDs
    assertThrows(IOException.class, () -> {
      localDataManager.getOmSnapshotLocalData(snapshotId);
    });
  }

  @Test
  public void testGetOmSnapshotLocalDataWithFile() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    
    OmSnapshotLocalData localData = createMockLocalData(snapshotId, null);
    
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);

    Path yamlPath = tempDir.resolve("test-snapshot.yaml");
    writeLocalDataToFile(localData, yamlPath);
    
    OmSnapshotLocalData retrieved = localDataManager
        .getOmSnapshotLocalData(yamlPath.toFile());
    
    assertNotNull(retrieved);
    assertEquals(snapshotId, retrieved.getSnapshotId());
  }

  @Test
  public void testAddVersionNodeWithDependents() throws IOException {
    List<UUID> versionIds = Stream.of(UUID.randomUUID(), UUID.randomUUID())
        .sorted(Comparator.comparing(String::valueOf)).collect(Collectors.toList());
    UUID snapshotId = versionIds.get(0);
    UUID previousSnapshotId = versionIds.get(1);
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);
    // Create snapshot directory structure and files
    createSnapshotLocalDataFile(snapshotId, previousSnapshotId);
    createSnapshotLocalDataFile(previousSnapshotId, null);
    OmSnapshotLocalData localData = createMockLocalData(snapshotId, previousSnapshotId);
    
    // Should not throw exception
    localDataManager.addVersionNodeWithDependents(localData);
  }

  @Test
  public void testAddVersionNodeWithDependentsAlreadyExists() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    
    createSnapshotLocalDataFile(snapshotId, null);
    
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);

    OmSnapshotLocalData localData = createMockLocalData(snapshotId, null);
    
    // First addition
    localDataManager.addVersionNodeWithDependents(localData);
    
    // Second addition - should handle gracefully
    localDataManager.addVersionNodeWithDependents(localData);
  }

  @Test
  public void testInitWithExistingYamlFiles() throws IOException {
    List<UUID> versionIds = Stream.of(UUID.randomUUID(), UUID.randomUUID())
        .sorted(Comparator.comparing(String::valueOf)).collect(Collectors.toList());
    UUID snapshotId = versionIds.get(0);
    UUID previousSnapshotId = versionIds.get(1);
    
    createSnapshotLocalDataFile(previousSnapshotId, null);
    createSnapshotLocalDataFile(snapshotId, previousSnapshotId);
    
    // Initialize - should load existing files
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);

    assertNotNull(localDataManager);
    Map<UUID, OmSnapshotLocalDataManager.SnapshotVersionsMeta> versionMap =
        localDataManager.getVersionNodeMap();
    assertEquals(2, versionMap.size());
    assertEquals(versionMap.keySet(), new HashSet<>(versionIds));
  }

  @Test
  public void testInitWithInvalidPathThrowsException() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    
    // Create a file with wrong location
    OmSnapshotLocalData localData = createMockLocalData(snapshotId, null);
    Path wrongPath = Paths.get(snapshotsDir.getAbsolutePath(), "db-wrong-name.yaml");
    writeLocalDataToFile(localData, wrongPath);
    
    // Should throw IOException during init
    assertThrows(IOException.class, () -> {
      new OmSnapshotLocalDataManager(omMetadataManager);
    });
  }

  @Test
  public void testClose() throws IOException {
    localDataManager = new OmSnapshotLocalDataManager(omMetadataManager);

    // Should not throw exception
    localDataManager.close();
  }

  // Helper methods

  private SnapshotInfo createMockSnapshotInfo(UUID snapshotId, UUID previousSnapshotId) {
    SnapshotInfo.Builder builder = SnapshotInfo.newBuilder()
        .setSnapshotId(snapshotId)
        .setName("snapshot-" + snapshotId);
    
    if (previousSnapshotId != null) {
      builder.setPathPreviousSnapshotId(previousSnapshotId);
    }
    
    return builder.build();
  }

  private LiveFileMetaData createMockLiveFileMetaData(String fileName, String columnFamilyName, String smallestKey,
      String largestKey) {
    LiveFileMetaData liveFileMetaData = mock(LiveFileMetaData.class);
    when(liveFileMetaData.columnFamilyName()).thenReturn(StringUtils.string2Bytes(columnFamilyName));
    when(liveFileMetaData.fileName()).thenReturn(fileName);
    when(liveFileMetaData.smallestKey()).thenReturn(StringUtils.string2Bytes(smallestKey));
    when(liveFileMetaData.largestKey()).thenReturn(StringUtils.string2Bytes(largestKey));
    return liveFileMetaData;
  }

  private OmSnapshotLocalData createMockLocalData(UUID snapshotId, UUID previousSnapshotId) {
    List<LiveFileMetaData> sstFiles = new ArrayList<>();
    sstFiles.add(createMockLiveFileMetaData("file1.sst", "columnFamily1", "key1", "key7"));
    sstFiles.add(createMockLiveFileMetaData("file2.sst", "columnFamily1", "key3", "key10"));
    sstFiles.add(createMockLiveFileMetaData("file3.sst", "columnFamily2", "key1", "key8"));
    sstFiles.add(createMockLiveFileMetaData("file4.sst", "columnFamily2", "key0", "key10"));
    return new OmSnapshotLocalData(snapshotId, sstFiles, previousSnapshotId, null);
  }

  private void createSnapshotLocalDataFile(UUID snapshotId, UUID previousSnapshotId)
      throws IOException {
    OmSnapshotLocalData localData = createMockLocalData(snapshotId, previousSnapshotId);
    
    String fileName = "db" + OM_SNAPSHOT_SEPARATOR + snapshotId.toString() + YAML_FILE_EXTENSION;
    Path yamlPath = Paths.get(snapshotsDir.getAbsolutePath(), fileName);
    
    writeLocalDataToFile(localData, yamlPath);
  }

  private void writeLocalDataToFile(OmSnapshotLocalData localData, Path filePath)
      throws IOException {
    // This is a simplified version - in real implementation, 
    // you would use the YamlSerializer
    snapshotLocalDataYamlSerializer.save(filePath.toFile(), localData);
  }
}
