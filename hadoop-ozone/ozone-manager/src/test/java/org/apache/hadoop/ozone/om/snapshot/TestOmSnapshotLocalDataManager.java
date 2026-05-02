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
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_LOCAL_DATA_MANAGER_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OmSnapshotLocalDataYaml.YAML_FILE_EXTENSION;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE;
import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.StringInMemoryTestTable;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotLocalData;
import org.apache.hadoop.ozone.om.OmSnapshotLocalDataYaml;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.DAGLeveledResource;
import org.apache.hadoop.ozone.om.lock.HierarchicalResourceLockManager;
import org.apache.hadoop.ozone.om.lock.HierarchicalResourceLockManager.HierarchicalResourceLock;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataProvider;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager.WritableOmSnapshotLocalDataProvider;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.hadoop.ozone.util.YamlSerializer;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.apache.ratis.util.function.CheckedFunction;
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
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.RocksDB;
import org.yaml.snakeyaml.Yaml;

/**
 * Test class for OmSnapshotLocalDataManager.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
public class TestOmSnapshotLocalDataManager {

  private static YamlSerializer<OmSnapshotLocalData> snapshotLocalDataYamlSerializer;
  private static List<String> lockCapturor;
  private static OzoneConfiguration conf;
  private static Map<UUID, Boolean> purgedSnapshotIdMap;

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

  @Mock
  private OMLayoutVersionManager layoutVersionManager;

  private OmSnapshotLocalDataManager localDataManager;
  private AutoCloseable mocks;

  private File snapshotsDir;
  private MockedStatic<OmSnapshotManager> snapshotUtilMock;

  private static final String READ_LOCK_MESSAGE_ACQUIRE = "readLock acquire";
  private static final String READ_LOCK_MESSAGE_UNLOCK = "readLock unlock";
  private static final String WRITE_LOCK_MESSAGE_ACQUIRE = "writeLock acquire";
  private static final String WRITE_LOCK_MESSAGE_UNLOCK = "writeLock unlock";

  @BeforeAll
  public static void setupClass() {
    conf = new OzoneConfiguration();
    snapshotLocalDataYamlSerializer = new YamlSerializer<OmSnapshotLocalData>(
        new OmSnapshotLocalDataYaml.YamlFactory()) {

      @Override
      public void computeAndSetChecksum(Yaml yaml, OmSnapshotLocalData data) throws IOException {
        data.computeAndSetChecksum(yaml);
      }
    };
    lockCapturor = new ArrayList<>();
    purgedSnapshotIdMap = new HashMap<>();
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
    this.snapshotUtilMock = mockStatic(OmSnapshotManager.class, CALLS_REAL_METHODS);
    purgedSnapshotIdMap.clear();
    snapshotUtilMock.when(() -> OmSnapshotManager.isSnapshotPurged(any(), any(), any(), any()))
        .thenAnswer(i -> purgedSnapshotIdMap.getOrDefault(i.getArgument(2), false));
    when(layoutVersionManager.isAllowed(any(LayoutFeature.class))).thenReturn(true);
    conf.setInt(OZONE_OM_SNAPSHOT_LOCAL_DATA_MANAGER_SERVICE_INTERVAL, -1);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (localDataManager != null) {
      localDataManager.close();
    }
    if (mocks != null) {
      mocks.close();
    }
    if (snapshotUtilMock != null) {
      snapshotUtilMock.close();
    }
  }

  private String getReadLockMessageAcquire(UUID snapshotId) {
    return READ_LOCK_MESSAGE_ACQUIRE + " " + DAGLeveledResource.SNAPSHOT_LOCAL_DATA_LOCK + " " + snapshotId;
  }

  private String getReadLockMessageRelease(UUID snapshotId) {
    return READ_LOCK_MESSAGE_UNLOCK + " " + DAGLeveledResource.SNAPSHOT_LOCAL_DATA_LOCK + " " + snapshotId;
  }

  private String getWriteLockMessageAcquire(UUID snapshotId) {
    return WRITE_LOCK_MESSAGE_ACQUIRE + " " + DAGLeveledResource.SNAPSHOT_LOCAL_DATA_LOCK + " " + snapshotId;
  }

  private String getWriteLockMessageRelease(UUID snapshotId) {
    return WRITE_LOCK_MESSAGE_UNLOCK + " " + DAGLeveledResource.SNAPSHOT_LOCAL_DATA_LOCK + " " + snapshotId;
  }

  private HierarchicalResourceLock getHierarchicalResourceLock(DAGLeveledResource resource, String key,
      boolean isWriteLock) {
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
    when(lockManager.acquireReadLock(any(DAGLeveledResource.class), anyString()))
        .thenAnswer(i -> {
          lockCapturor.add(READ_LOCK_MESSAGE_ACQUIRE + " " + i.getArgument(0) + " " + i.getArgument(1));
          return getHierarchicalResourceLock(i.getArgument(0), i.getArgument(1), false);
        });
    when(lockManager.acquireWriteLock(any(DAGLeveledResource.class), anyString()))
        .thenAnswer(i -> {
          lockCapturor.add(WRITE_LOCK_MESSAGE_ACQUIRE + " " + i.getArgument(0) + " " + i.getArgument(1));
          return getHierarchicalResourceLock(i.getArgument(0), i.getArgument(1), true);
        });
  }

  private OmSnapshotLocalDataManager getNewOmSnapshotLocalDataManager(
      CheckedFunction<SnapshotInfo, OmMetadataManagerImpl, IOException> provider) throws IOException {
    return new OmSnapshotLocalDataManager(omMetadataManager, null, layoutVersionManager, provider, conf);
  }

  private OmSnapshotLocalDataManager getNewOmSnapshotLocalDataManager() throws IOException {
    return getNewOmSnapshotLocalDataManager(null);
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
    ManagedRocksDB db = mock(ManagedRocksDB.class);
    when(rocksDatabase.getManagedRocksDb()).thenReturn(db);
    RocksDB rdb = mock(RocksDB.class);
    when(db.get()).thenReturn(rdb);
    when(rdb.getLiveFilesMetaData()).thenReturn(sstFiles);
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
    localDataManager = getNewOmSnapshotLocalDataManager();
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
          OmSnapshotLocalData previousSnapshot = omSnapshotLocalDataProvider.getPreviousSnapshotLocalData()
              .orElse(null);
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
    localDataManager = getNewOmSnapshotLocalDataManager();
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
        OmSnapshotLocalData previousSnapshot = omSnapshotLocalDataProvider.getPreviousSnapshotLocalData().orElse(null);
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
    localDataManager = getNewOmSnapshotLocalDataManager();
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
    localDataManager = getNewOmSnapshotLocalDataManager();
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
    localDataManager = getNewOmSnapshotLocalDataManager();
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
  @ValueSource(booleans =  {true, false})
  public void testOrphanVersionDeletionWithVersionDeletion(boolean purgeSnapshot) throws IOException {
    localDataManager = getNewOmSnapshotLocalDataManager();
    List<UUID> snapshotIds = createSnapshotLocalData(localDataManager, 3);
    UUID firstSnapId = snapshotIds.get(0);
    UUID secondSnapId = snapshotIds.get(1);
    UUID thirdSnapId = snapshotIds.get(2);

    addVersionsToLocalData(localDataManager, firstSnapId, ImmutableMap.of(1, 1, 2, 2, 3, 3));
    addVersionsToLocalData(localDataManager, secondSnapId, ImmutableMap.of(4, 2, 8, 1, 10, 3, 11, 3));
    addVersionsToLocalData(localDataManager, thirdSnapId, ImmutableMap.of(5, 8, 13, 10));
    assertEquals(new HashSet<>(snapshotIds), localDataManager.getSnapshotToBeCheckedForOrphans().keySet());
    localDataManager.getSnapshotToBeCheckedForOrphans().clear();
    purgedSnapshotIdMap.put(secondSnapId, purgeSnapshot);
    localDataManager.checkOrphanSnapshotVersions(omMetadataManager, null, thirdSnapId);
    try (ReadableOmSnapshotLocalDataProvider snap = localDataManager.getOmSnapshotLocalData(thirdSnapId)) {
      OmSnapshotLocalData snapshotLocalData = snap.getSnapshotLocalData();
      assertEquals(Sets.newHashSet(0, 13), snapshotLocalData.getVersionSstFileInfos().keySet());
    }
    assertTrue(localDataManager.getSnapshotToBeCheckedForOrphans().containsKey(secondSnapId));
    localDataManager.checkOrphanSnapshotVersions(omMetadataManager, null, secondSnapId);
    try (ReadableOmSnapshotLocalDataProvider snap = localDataManager.getOmSnapshotLocalData(secondSnapId)) {
      OmSnapshotLocalData snapshotLocalData = snap.getSnapshotLocalData();
      if (purgeSnapshot) {
        assertEquals(Sets.newHashSet(0, 10), snapshotLocalData.getVersionSstFileInfos().keySet());
      } else {
        assertEquals(Sets.newHashSet(0, 10, 11), snapshotLocalData.getVersionSstFileInfos().keySet());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans =  {true, false})
  public void testOrphanVersionDeletionWithChainUpdate(boolean purgeSnapshot) throws IOException {
    localDataManager = getNewOmSnapshotLocalDataManager();
    List<UUID> snapshotIds = createSnapshotLocalData(localDataManager, 3);
    UUID firstSnapId = snapshotIds.get(0);
    UUID secondSnapId = snapshotIds.get(1);
    UUID thirdSnapId = snapshotIds.get(2);

    addVersionsToLocalData(localDataManager, firstSnapId, ImmutableMap.of(1, 1, 2, 2, 3, 3));
    addVersionsToLocalData(localDataManager, secondSnapId, ImmutableMap.of(4, 2, 8, 1, 10, 3, 11, 3));
    addVersionsToLocalData(localDataManager, thirdSnapId, ImmutableMap.of(5, 8, 13, 10));
    purgedSnapshotIdMap.put(secondSnapId, purgeSnapshot);
    try (WritableOmSnapshotLocalDataProvider snapshotLocalDataProvider =
             localDataManager.getWritableOmSnapshotLocalData(thirdSnapId, firstSnapId)) {
      snapshotLocalDataProvider.commit();
    }
    try (ReadableOmSnapshotLocalDataProvider snap = localDataManager.getOmSnapshotLocalData(thirdSnapId)) {
      OmSnapshotLocalData snapshotLocalData = snap.getSnapshotLocalData();
      assertEquals(Sets.newHashSet(0, 5, 13), snapshotLocalData.getVersionSstFileInfos().keySet());
      assertEquals(firstSnapId, snapshotLocalData.getPreviousSnapshotId());
    }

    assertTrue(localDataManager.getSnapshotToBeCheckedForOrphans().containsKey(secondSnapId));
    localDataManager.checkOrphanSnapshotVersions(omMetadataManager, null, secondSnapId);
    if (purgeSnapshot) {
      assertThrows(NoSuchFileException.class,
          () -> localDataManager.getOmSnapshotLocalData(secondSnapId));
      assertFalse(localDataManager.getVersionNodeMapUnmodifiable().containsKey(secondSnapId));
    } else {
      try (ReadableOmSnapshotLocalDataProvider snap = localDataManager.getOmSnapshotLocalData(secondSnapId)) {
        OmSnapshotLocalData snapshotLocalData = snap.getSnapshotLocalData();
        assertEquals(Sets.newHashSet(0, 11), snapshotLocalData.getVersionSstFileInfos().keySet());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWriteWithChainUpdate(boolean previousSnapshotExisting) throws IOException {
    localDataManager = getNewOmSnapshotLocalDataManager();
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
    localDataManager = getNewOmSnapshotLocalDataManager();
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
    localDataManager = getNewOmSnapshotLocalDataManager();
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
      assertEquals(previousVersion < snap.getPreviousSnapshotLocalData().orElse(null).getVersion(), snap.needsDefrag());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testVersionResolution(boolean read) throws IOException {
    localDataManager = getNewOmSnapshotLocalDataManager();
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
      for (int end = 0; end < snapshotIds.size(); end++) {
        UUID prevSnapId = snapshotIds.get(start);
        UUID snapId = snapshotIds.get(end);
        Map<Integer, Integer> versionMap = new HashMap<>(versionMaps.get(end));
        versionMap.put(0, 0);
        for (int idx = end - 1; idx > start; idx--) {
          for (Map.Entry<Integer, Integer> version : versionMap.entrySet()) {
            version.setValue(versionMaps.get(idx).getOrDefault(version.getValue(), 0));
          }
        }
        if (start >= end) {
          assertThrows(IOException.class, () -> {
            if (read) {
              localDataManager.getOmSnapshotLocalData(snapId, prevSnapId);
            } else {
              localDataManager.getWritableOmSnapshotLocalData(snapId, prevSnapId);
            }
          });
        } else {
          try (ReadableOmSnapshotLocalDataProvider snap = read ?
              localDataManager.getOmSnapshotLocalData(snapId, prevSnapId) :
              localDataManager.getWritableOmSnapshotLocalData(snapId, prevSnapId)) {
            OmSnapshotLocalData snapshotLocalData = snap.getSnapshotLocalData();
            OmSnapshotLocalData prevSnapshotLocalData = snap.getPreviousSnapshotLocalData().orElse(null);
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
  }

  @Test
  public void testConstructor() throws IOException {
    localDataManager = getNewOmSnapshotLocalDataManager();
    assertNotNull(localDataManager);
  }

  @Test
  public void testGetSnapshotLocalPropertyYamlPathWithSnapshotInfo() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    SnapshotInfo snapshotInfo = createMockSnapshotInfo(snapshotId, null);
    
    localDataManager = getNewOmSnapshotLocalDataManager();

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
    int seqNumber = 0;
    for (Map.Entry<String, List<String>> entry : expNotDefraggedSSTFileList.entrySet()) {
      String cfname = entry.getKey();
      for (String fname : entry.getValue()) {
        mockedLiveFiles.add(createMockLiveFileMetaData("/" + fname + ".sst", cfname, "k1", "k2", seqNumber++));
      }
    }
    int expectedDbTxSequenceNumber = seqNumber - 1;
    // Add some other column families and files that should be ignored
    mockedLiveFiles.add(createMockLiveFileMetaData("ot1.sst", "otherTable", "k1", "k2", seqNumber++));
    mockedLiveFiles.add(createMockLiveFileMetaData("ot2.sst", "otherTable", "k1", "k2", seqNumber));

    mockSnapshotStore(snapshotId, mockedLiveFiles);
    localDataManager = getNewOmSnapshotLocalDataManager();
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
    assertEquals(expectedDbTxSequenceNumber, localData.getDbTxSequenceNumber());
  }

  @Test
  public void testCreateNewOmSnapshotLocalDataFile() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    SnapshotInfo snapshotInfo = createMockSnapshotInfo(snapshotId, null);
    // Setup snapshot store mock

    List<LiveFileMetaData> sstFiles = new ArrayList<>();
    sstFiles.add(createMockLiveFileMetaData("file1.sst", KEY_TABLE, "key1", "key7", 10));
    sstFiles.add(createMockLiveFileMetaData("file2.sst", KEY_TABLE, "key3", "key9", 20));
    sstFiles.add(createMockLiveFileMetaData("file3.sst", FILE_TABLE, "key1", "key7", 30));
    sstFiles.add(createMockLiveFileMetaData("file4.sst", FILE_TABLE, "key1", "key7", 100));
    sstFiles.add(createMockLiveFileMetaData("file5.sst", DIRECTORY_TABLE, "key1", "key7", 5000));
    sstFiles.add(createMockLiveFileMetaData("file6.sst", "colFamily1", "key1", "key7", 6000));
    List<SstFileInfo> sstFileInfos = IntStream.range(0, sstFiles.size() - 1)
        .mapToObj(sstFiles::get).map(lfm ->
            new SstFileInfo(lfm.fileName().replace(".sst", ""),
            bytes2String(lfm.smallestKey()),
            bytes2String(lfm.largestKey()), bytes2String(lfm.columnFamilyName()))).collect(Collectors.toList());
    mockSnapshotStore(snapshotId, sstFiles);

    localDataManager = getNewOmSnapshotLocalDataManager();

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
      assertEquals(5000, snapshotLocalData.getSnapshotLocalData().getDbTxSequenceNumber());
    }
  }

  @Test
  public void testGetOmSnapshotLocalDataWithSnapshotInfo() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    SnapshotInfo snapshotInfo = createMockSnapshotInfo(snapshotId, null);
    
    // Create and write snapshot local data file
    OmSnapshotLocalData localData = createMockLocalData(snapshotId, null);
    
    localDataManager = getNewOmSnapshotLocalDataManager();

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
    
    localDataManager = getNewOmSnapshotLocalDataManager();

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
    
    localDataManager = getNewOmSnapshotLocalDataManager();

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
    localDataManager = getNewOmSnapshotLocalDataManager();
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
    
    localDataManager = getNewOmSnapshotLocalDataManager();

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
    localDataManager = getNewOmSnapshotLocalDataManager();

    assertNotNull(localDataManager);
    Map<UUID, OmSnapshotLocalDataManager.SnapshotVersionsMeta> versionMap =
        localDataManager.getVersionNodeMapUnmodifiable();
    assertEquals(2, versionMap.size());
    assertEquals(versionMap.keySet(), new HashSet<>(versionIds));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testInitWithMissingYamlFiles(boolean needsUpgrade) throws IOException {
    Table<String, SnapshotInfo> table = new StringInMemoryTestTable<>();
    when(omMetadataManager.getSnapshotInfoTable()).thenReturn(table);
    UUID snap3 = UUID.randomUUID();
    UUID snap2 = UUID.randomUUID();
    UUID snap1 = UUID.randomUUID();
    CheckedFunction<SnapshotInfo, OmMetadataManagerImpl, IOException> mockedProvider = (snapshotInfo) -> {
      if (snapshotInfo.getSnapshotId().equals(snap2)) {
        throw new IOException("SnapshotId should not be " + snap2 + " since it is deleted");
      }
      mockSnapshotStore(snapshotInfo.getSnapshotId(), ImmutableList.of(createMockLiveFileMetaData(
          snapshotInfo.getSnapshotId() + ".sst", KEY_TABLE, snapshotInfo.getSnapshotId() + "k1",
          snapshotInfo.getSnapshotId() + "k2")));
      OmMetadataManagerImpl snapshotMetadataManager = mock(OmMetadataManagerImpl.class);
      when(snapshotMetadataManager.getStore()).thenReturn(snapshotStore);
      return snapshotMetadataManager;
    };
    table.put("snap3", createMockSnapshotInfo(snap3, null, SNAPSHOT_ACTIVE));
    table.put("snap2", createMockSnapshotInfo(snap2, snap3, SNAPSHOT_DELETED));
    table.put("snap1", createMockSnapshotInfo(snap1, snap2, SNAPSHOT_ACTIVE));
    when(layoutVersionManager.isAllowed(eq(OMLayoutFeature.SNAPSHOT_DEFRAG))).thenReturn(!needsUpgrade);
    localDataManager = getNewOmSnapshotLocalDataManager(mockedProvider);
    if (needsUpgrade) {
      assertEquals(ImmutableSet.of(snap1, snap2, snap3), localDataManager.getVersionNodeMapUnmodifiable().keySet());
      Map<UUID, UUID> previousMap = ImmutableMap.of(snap2, snap3, snap1, snap2);
      Map<UUID, Map<Integer, OmSnapshotLocalData.VersionMeta>> expectedSstFile = ImmutableMap.of(
          snap3, ImmutableMap.of(0,
              new OmSnapshotLocalData.VersionMeta(0, ImmutableList.of(
                  new SstFileInfo(snap3.toString(), snap3 + "k1", snap3 + "k2", KEY_TABLE)))),
          snap1, ImmutableMap.of(0,
              new OmSnapshotLocalData.VersionMeta(0, ImmutableList.of(
                  new SstFileInfo(snap1.toString(), snap1 + "k1", snap1 + "k2", KEY_TABLE)))),
          snap2, ImmutableMap.of(0,
              new OmSnapshotLocalData.VersionMeta(0, ImmutableList.of())));
      for (UUID snapshotId : localDataManager.getVersionNodeMapUnmodifiable().keySet()) {
        try (ReadableOmSnapshotLocalDataProvider readableOmSnapshotLocalDataProvider =
                 localDataManager.getOmSnapshotLocalData(snapshotId)) {
          OmSnapshotLocalData snapshotLocalData = readableOmSnapshotLocalDataProvider.getSnapshotLocalData();
          assertEquals(snapshotId, snapshotLocalData.getSnapshotId());
          assertEquals(previousMap.get(snapshotId), snapshotLocalData.getPreviousSnapshotId());
          assertEquals(expectedSstFile.get(snapshotId), snapshotLocalData.getVersionSstFileInfos());
          assertTrue(readableOmSnapshotLocalDataProvider.needsDefrag());
          assertTrue(snapshotLocalData.getNeedsDefrag());
        }
      }
    } else {
      assertEquals(ImmutableSet.of(), localDataManager.getVersionNodeMapUnmodifiable().keySet());
    }
  }

  /**
   * Regression test for NullPointerException in OmSnapshotManager#createCacheLoader.
   * <p>
   * isSnapshotPurged() now falls back to transactionInfo when getTableKey() returns null.
   * A null transactionInfo means no purge was ever recorded for this snapshot in its YAML,
   * so the snapshot is treated as active and the orphan check should correctly skip it.
   */
  @Test
  public void testCheckOrphanSnapshotVersionsWithStaleSnapshotChain() throws IOException {
    localDataManager = getNewOmSnapshotLocalDataManager();
    UUID snapshotId = createSnapshotLocalData(localDataManager, 1).get(0);

    // Snapshot must be in versionNodeMap before the orphan check.
    assertNotNull(localDataManager.getVersionNodeMapUnmodifiable().get(snapshotId));

    // Use the real isSnapshotPurged
    snapshotUtilMock.when(() -> OmSnapshotManager.isSnapshotPurged(any(), any(), any(), any()))
        .thenCallRealMethod();

    // Simulate a stale SnapshotChainManager: getTableKey returns null for the
    // snapshot because the snapshot chain has not been correctly updated
    SnapshotChainManager staleChain = mock(SnapshotChainManager.class);
    when(staleChain.getTableKey(snapshotId)).thenReturn(null);

    localDataManager.checkOrphanSnapshotVersions(omMetadataManager, staleChain, snapshotId);

    // Before the fix: isSnapshotPurged returned true for any null tableKey, so the snapshot
    // was removed from versionNodeMap. getMeta() then returned null, causing NullPointerException

    // After the fix: null transactionInfo means no purge has been recorded, assuming active snapshot.
    // versionNodeMap entry will survive the orphan check. getMeta() will be non-null.

    assertNotNull(localDataManager.getVersionNodeMapUnmodifiable().get(snapshotId),
        "Active snapshot was removed erroneously from versionNodeMap due to stale SnapshotChainManager");

    try (OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataMetaProvider provider =
             localDataManager.getOmSnapshotLocalDataMeta(snapshotId)) {
      assertNotNull(provider.getMeta(),
          "getMeta() returned null. Calling getVersion() on it throws NullPointerException");
    }
  }

  @Test
  public void testInitWithInvalidPathThrowsException() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    
    // Create a file with wrong location
    OmSnapshotLocalData localData = createMockLocalData(snapshotId, null);
    Path wrongPath = Paths.get(snapshotsDir.getAbsolutePath(), "db-wrong-name.yaml");
    writeLocalDataToFile(localData, wrongPath);
    
    // Should throw IOException during init
    assertThrows(IOException.class, this::getNewOmSnapshotLocalDataManager);
  }

  @Test
  public void testClose() throws IOException {
    localDataManager = getNewOmSnapshotLocalDataManager();
    // Should not throw exception
    localDataManager.close();
  }

  // Helper methods

  private SnapshotInfo createMockSnapshotInfo(UUID snapshotId, UUID previousSnapshotId) {
    return createMockSnapshotInfo(snapshotId, previousSnapshotId, null);
  }

  private SnapshotInfo createMockSnapshotInfo(UUID snapshotId, UUID previousSnapshotId,
      SnapshotInfo.SnapshotStatus snapshotStatus) {
    SnapshotInfo.Builder builder = SnapshotInfo.newBuilder()
        .setSnapshotId(snapshotId)
        .setName("snapshot-" + snapshotId);
    builder.setSnapshotStatus(snapshotStatus == null ? SNAPSHOT_ACTIVE : snapshotStatus);
    if (previousSnapshotId != null) {
      builder.setPathPreviousSnapshotId(previousSnapshotId);
    }

    return builder.build();
  }

  private LiveFileMetaData createMockLiveFileMetaData(String fileName, String columnFamilyName, String smallestKey,
      String largestKey) {
    return createMockLiveFileMetaData(fileName, columnFamilyName, smallestKey, largestKey, 0);
  }

  private LiveFileMetaData createMockLiveFileMetaData(String fileName, String columnFamilyName, String smallestKey,
      String largestKey, long largestSeqNumber) {
    LiveFileMetaData liveFileMetaData = mock(LiveFileMetaData.class);
    when(liveFileMetaData.columnFamilyName()).thenReturn(StringUtils.string2Bytes(columnFamilyName));
    when(liveFileMetaData.fileName()).thenReturn(fileName);
    when(liveFileMetaData.smallestKey()).thenReturn(StringUtils.string2Bytes(smallestKey));
    when(liveFileMetaData.largestKey()).thenReturn(StringUtils.string2Bytes(largestKey));
    when(liveFileMetaData.largestSeqno()).thenReturn(largestSeqNumber);
    return liveFileMetaData;
  }

  private OmSnapshotLocalData createMockLocalData(UUID snapshotId, UUID previousSnapshotId) {
    List<LiveFileMetaData> sstFiles = new ArrayList<>();
    sstFiles.add(createMockLiveFileMetaData("file1.sst", "columnFamily1", "key1", "key7"));
    sstFiles.add(createMockLiveFileMetaData("file2.sst", "columnFamily1", "key3", "key10"));
    sstFiles.add(createMockLiveFileMetaData("file3.sst", "columnFamily2", "key1", "key8"));
    sstFiles.add(createMockLiveFileMetaData("file4.sst", "columnFamily2", "key0", "key10"));
    return new OmSnapshotLocalData(snapshotId, sstFiles, previousSnapshotId, null, 10);
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

  /**
   * Tests the fix for the NoSuchFileException : when a purged snapshot (last in chain)
   * has all its versions removed and YAML deleted by orphan check, it must NOT be
   * re-added to snapshotToBeCheckedForOrphans. Otherwise the next orphan check run
   * would try to load the deleted YAML and throw NoSuchFileException.
   *
   */
  @Test
  public void testPurgedSnapshotNotReAddedAfterYamlDeleted() throws Exception {
    localDataManager = getNewOmSnapshotLocalDataManager();
    List<UUID> snapshotIds = createSnapshotLocalData(localDataManager, 2);
    UUID secondSnapId = snapshotIds.get(1);
    // Simulate purge: set transactionInfo on S2's YAML (purge does this before orphan check runs)
    try (WritableOmSnapshotLocalDataProvider writableProvider =
             localDataManager.getWritableOmSnapshotLocalData(secondSnapId)) {
      writableProvider.setTransactionInfo(TransactionInfo.valueOf(1, 1));
      writableProvider.commit();
    }
    // S2 is last in chain - mark as purged so all versions get removed
    purgedSnapshotIdMap.put(secondSnapId, true);
    // Simulate purge adding S2 to orphan check list
    localDataManager.getSnapshotToBeCheckedForOrphans().clear();
    localDataManager.getSnapshotToBeCheckedForOrphans().put(secondSnapId, 1);
    // Run full orphan check
    java.lang.reflect.Method method = OmSnapshotLocalDataManager.class.getDeclaredMethod(
        "checkOrphanSnapshotVersions", OMMetadataManager.class,
        org.apache.hadoop.ozone.om.SnapshotChainManager.class);
    method.setAccessible(true);
    method.invoke(localDataManager, omMetadataManager, null);
    // S2 should NOT be in the map
    assertFalse(localDataManager.getSnapshotToBeCheckedForOrphans().containsKey(secondSnapId),
        "Purged snapshot should not be re-added after YAML deleted");
  }
}
