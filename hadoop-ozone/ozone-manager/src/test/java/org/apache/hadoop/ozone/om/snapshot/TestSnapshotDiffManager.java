/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.snapshot;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSSTDumpTool;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotDiffJob;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.apache.hadoop.ozone.om.snapshot.SnapshotDiffObject.SnapshotDiffObjectBuilder;
import org.apache.hadoop.ozone.om.snapshot.SnapshotTestUtils.StubbedPersistentMap;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobCancelResult;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ClosableIterator;
import org.apache.hadoop.util.ExitUtil;
import org.apache.ozone.rocksdb.util.ManagedSstFileReader;
import org.apache.ozone.rocksdb.util.RdbUtil;
import org.apache.ozone.rocksdiff.DifferSnapshotInfo;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.apache.ozone.rocksdiff.RocksDiffUtils;
import org.apache.ratis.util.ExitUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.utils.db.DBStoreBuilder.DEFAULT_COLUMN_FAMILY_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_JOB_DEFAULT_WAIT_TIME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_JOB_DEFAULT_WAIT_TIME_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_MAX_ALLOWED_KEYS_CHANGED_PER_DIFF_JOB;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_MAX_ALLOWED_KEYS_CHANGED_PER_DIFF_JOB_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_THREAD_POOL_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_THREAD_POOL_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_SST_DUMPTOOL_EXECUTOR_BUFFER_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_SST_DUMPTOOL_EXECUTOR_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_SST_DUMPTOOL_EXECUTOR_POOL_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_SST_DUMPTOOL_EXECUTOR_POOL_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.DELIMITER;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.SNAP_DIFF_JOB_TABLE_NAME;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.SNAP_DIFF_REPORT_TABLE_NAME;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.LEGACY;
import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.getTableKey;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone.getDiffReportEntryCodec;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.DONE;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.FAILED;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.IN_PROGRESS;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.QUEUED;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.REJECTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for SnapshotDiffManager.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TestSnapshotDiffManager {
  private static final String VOLUME_NAME = "volume";
  private static final String BUCKET_NAME = "bucket";
  private ManagedRocksDB db;
  private ManagedDBOptions dbOptions;
  private ManagedColumnFamilyOptions columnFamilyOptions;
  private List<ColumnFamilyHandle> columnFamilyHandles;
  private ColumnFamilyHandle snapDiffJobTable;
  private ColumnFamilyHandle snapDiffReportTable;
  private SnapshotDiffManager snapshotDiffManager;
  private final List<JobStatus> jobStatuses = Arrays.asList(QUEUED, IN_PROGRESS,
      DONE, REJECTED, FAILED);

  private SnapshotInfo snapshotInfo;
  private final List<String> snapshotNames = new ArrayList<>();
  private final List<SnapshotInfo> snapshotInfoList = new ArrayList<>();
  private final List<SnapshotDiffJob> snapDiffJobs = new ArrayList<>();
  @TempDir
  private File dbDir;
  @Mock
  private RocksDBCheckpointDiffer differ;
  @Mock
  private OMMetadataManager omMetadataManager;
  @Mock
  private OzoneManager ozoneManager;
  @Mock
  private OzoneConfiguration configuration;
  @Mock
  private Table<String, SnapshotInfo> snapshotInfoTable;
  @Mock
  private Table<String, OmBucketInfo> bucketInfoTable;
  @Mock
  private Table<String, OmKeyInfo> keyInfoTable;
  @Mock
  private OmBucketInfo omBucketInfo;
  @Mock
  private RDBStore dbStore;

  private LoadingCache<String, OmSnapshot> snapshotCache;

  @Mock
  private RocksIterator jobTableIterator;

  private static CodecRegistry codecRegistry;

  private final BiFunction<SnapshotInfo, SnapshotInfo, String>
      generateSnapDiffJobKey =
          (SnapshotInfo fromSnapshotInfo, SnapshotInfo toSnapshotInfo) ->
              fromSnapshotInfo.getSnapshotId() + DELIMITER +
                  toSnapshotInfo.getSnapshotId();

  @BeforeAll
  public static void initCodecRegistry() {
    codecRegistry = CodecRegistry.newBuilder()
        .addCodec(DiffReportEntry.class, getDiffReportEntryCodec())
        .addCodec(SnapshotDiffJob.class, SnapshotDiffJob.getCodec())
        .build();
  }

  @BeforeEach
  public void init() throws RocksDBException, IOException, ExecutionException {
    ExitUtils.disableSystemExit();
    ExitUtil.disableSystemExit();

    dbOptions = new ManagedDBOptions();
    dbOptions.setCreateIfMissing(true);
    columnFamilyOptions = new ManagedColumnFamilyOptions();

    List<ColumnFamilyDescriptor> columnFamilyDescriptors =
        Collections.singletonList(new ColumnFamilyDescriptor(
            StringUtils.string2Bytes(DEFAULT_COLUMN_FAMILY_NAME),
            columnFamilyOptions));

    columnFamilyHandles = new ArrayList<>();

    db = ManagedRocksDB.open(dbOptions, dbDir.getAbsolutePath(),
        columnFamilyDescriptors, columnFamilyHandles);

    snapDiffJobTable = db.get().createColumnFamily(
        new ColumnFamilyDescriptor(
            StringUtils.string2Bytes(SNAP_DIFF_JOB_TABLE_NAME),
            columnFamilyOptions));
    snapDiffReportTable = db.get().createColumnFamily(
        new ColumnFamilyDescriptor(
            StringUtils.string2Bytes(SNAP_DIFF_REPORT_TABLE_NAME),
            columnFamilyOptions));

    columnFamilyHandles.add(snapDiffJobTable);
    columnFamilyHandles.add(snapDiffReportTable);

    String snapshotNamePrefix = "snap-";
    String snapshotPath = "snapshotPath";
    String snapshotCheckpointDir = "snapshotCheckpointDir";
    UUID baseSnapshotId = UUID.randomUUID();
    String baseSnapshotName = snapshotNamePrefix + baseSnapshotId;
    snapshotInfo = new SnapshotInfo.Builder()
        .setSnapshotId(baseSnapshotId)
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setName(baseSnapshotName)
        .setSnapshotPath(snapshotPath)
        .setCheckpointDir(snapshotCheckpointDir)
        .build();

    for (JobStatus jobStatus : jobStatuses) {
      UUID targetSnapshotId = UUID.randomUUID();
      String targetSnapshotName = snapshotNamePrefix +
          targetSnapshotId;
      SnapshotInfo targetSnapshot = new SnapshotInfo.Builder()
          .setSnapshotId(targetSnapshotId)
          .setVolumeName(VOLUME_NAME)
          .setBucketName(BUCKET_NAME)
          .setName(targetSnapshotName)
          .setSnapshotPath(snapshotPath)
          .setCheckpointDir(snapshotCheckpointDir)
          .build();

      SnapshotDiffJob diffJob = new SnapshotDiffJob(System.currentTimeMillis(),
          UUID.randomUUID().toString(), jobStatus, VOLUME_NAME, BUCKET_NAME,
          baseSnapshotName, targetSnapshotName, false, 0);

      snapshotNames.add(targetSnapshotName);
      snapshotInfoList.add(targetSnapshot);
      snapDiffJobs.add(diffJob);
    }

    String bucketTableKey =
        OM_KEY_PREFIX + VOLUME_NAME + OM_KEY_PREFIX + BUCKET_NAME;

    when(configuration
        .getTimeDuration(OZONE_OM_SNAPSHOT_DIFF_JOB_DEFAULT_WAIT_TIME,
            OZONE_OM_SNAPSHOT_DIFF_JOB_DEFAULT_WAIT_TIME_DEFAULT,
            TimeUnit.MILLISECONDS))
        .thenReturn(OZONE_OM_SNAPSHOT_DIFF_JOB_DEFAULT_WAIT_TIME_DEFAULT);
    when(configuration
        .getBoolean(OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF,
            OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF_DEFAULT))
        .thenReturn(OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF_DEFAULT);
    when(configuration
        .getLong(OZONE_OM_SNAPSHOT_DIFF_MAX_ALLOWED_KEYS_CHANGED_PER_DIFF_JOB,
            OZONE_OM_SNAPSHOT_DIFF_MAX_ALLOWED_KEYS_CHANGED_PER_DIFF_JOB_DEFAULT
        ))
        .thenReturn(
            OZONE_OM_SNAPSHOT_DIFF_MAX_ALLOWED_KEYS_CHANGED_PER_DIFF_JOB_DEFAULT
        );
    when(configuration
        .getInt(OZONE_OM_SNAPSHOT_DIFF_THREAD_POOL_SIZE,
            OZONE_OM_SNAPSHOT_DIFF_THREAD_POOL_SIZE_DEFAULT))
        .thenReturn(OZONE_OM_SNAPSHOT_DIFF_THREAD_POOL_SIZE_DEFAULT);
    when(configuration
        .getInt(OZONE_OM_SNAPSHOT_SST_DUMPTOOL_EXECUTOR_POOL_SIZE,
            OZONE_OM_SNAPSHOT_SST_DUMPTOOL_EXECUTOR_POOL_SIZE_DEFAULT))
        .thenReturn(OZONE_OM_SNAPSHOT_SST_DUMPTOOL_EXECUTOR_POOL_SIZE_DEFAULT);
    when(configuration
        .getStorageSize(OZONE_OM_SNAPSHOT_SST_DUMPTOOL_EXECUTOR_BUFFER_SIZE,
            OZONE_OM_SNAPSHOT_SST_DUMPTOOL_EXECUTOR_BUFFER_SIZE_DEFAULT,
            StorageUnit.BYTES))
        .thenReturn(FileUtils.ONE_KB_BI.doubleValue());

    for (int i = 0; i < jobStatuses.size(); i++) {
      when(snapshotInfoTable.get(getTableKey(VOLUME_NAME, BUCKET_NAME,
          snapshotNames.get(i)))).thenReturn(snapshotInfoList.get(i));
    }

    when(snapshotInfoTable.get(getTableKey(VOLUME_NAME, BUCKET_NAME,
        baseSnapshotName))).thenReturn(snapshotInfo);

    when(dbStore.getDbLocation()).thenReturn(dbDir);
    when(dbStore.getSnapshotMetadataDir()).thenReturn(dbDir.getAbsolutePath());
    when(omBucketInfo.getBucketLayout()).thenReturn(LEGACY);
    when(bucketInfoTable.get(bucketTableKey)).thenReturn(omBucketInfo);
    when(omMetadataManager.getStore()).thenReturn(dbStore);
    when(omMetadataManager.getSnapshotInfoTable())
        .thenReturn(snapshotInfoTable);
    when(omMetadataManager.getBucketTable()).thenReturn(bucketInfoTable);
    when(omMetadataManager.getBucketKey(VOLUME_NAME, BUCKET_NAME))
        .thenReturn(bucketTableKey);
    when(omMetadataManager.getKeyTable(LEGACY)).thenReturn(keyInfoTable);
    when(ozoneManager.getConfiguration()).thenReturn(configuration);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);

    CacheLoader<String, OmSnapshot> loader =
        new CacheLoader<String, OmSnapshot>() {
          @NotNull
          @Override
          public OmSnapshot load(@NotNull String key) {
            return getMockedOmSnapshot(key);
          }
        };

    snapshotCache = CacheBuilder.newBuilder().maximumSize(10).build(loader);

    snapshotDiffManager = new SnapshotDiffManager(db, differ, ozoneManager,
        snapshotCache, snapDiffJobTable, snapDiffReportTable,
        columnFamilyOptions, codecRegistry);
  }

  @AfterEach
  public void tearDown() {
    if (columnFamilyHandles != null) {
      columnFamilyHandles.forEach(IOUtils::closeQuietly);
    }

    IOUtils.closeQuietly(db);
    IOUtils.closeQuietly(dbOptions);
    IOUtils.closeQuietly(columnFamilyOptions);
    IOUtils.closeQuietly(snapshotDiffManager);
  }

  private OmSnapshot getMockedOmSnapshot(String snapshot) {
    OmSnapshot omSnapshot = mock(OmSnapshot.class);
    when(omSnapshot.getName()).thenReturn(snapshot);
    when(omSnapshot.getMetadataManager()).thenReturn(omMetadataManager);
    when(omMetadataManager.getStore()).thenReturn(dbStore);
    return omSnapshot;
  }

  private SnapshotInfo getMockedSnapshotInfo(UUID snapshotId) {
    SnapshotInfo snapInfo = mock(SnapshotInfo.class);
    when(snapInfo.getSnapshotId()).thenReturn(snapshotId);
    return snapInfo;
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 5, 10, 100, 1000, 10000})
  public void testGetDeltaFilesWithDag(int numberOfFiles)
      throws ExecutionException, RocksDBException, IOException {
    UUID snap1 = UUID.randomUUID();
    UUID snap2 = UUID.randomUUID();

    String diffDir = Files.createTempDirectory("snapdiff_dir").toString();
    Set<String> randomStrings = IntStream.range(0, numberOfFiles)
        .mapToObj(i -> RandomStringUtils.randomAlphabetic(10))
        .collect(Collectors.toSet());

    when(differ.getSSTDiffListWithFullPath(
        any(DifferSnapshotInfo.class),
        any(DifferSnapshotInfo.class),
        eq(diffDir))
    ).thenReturn(Lists.newArrayList(randomStrings));

    SnapshotInfo fromSnapshotInfo = getMockedSnapshotInfo(snap1);
    SnapshotInfo toSnapshotInfo = getMockedSnapshotInfo(snap2);
    when(jobTableIterator.isValid()).thenReturn(false);
    Set<String> deltaFiles = snapshotDiffManager.getDeltaFiles(
        snapshotCache.get(snap1.toString()),
        snapshotCache.get(snap2.toString()),
        Arrays.asList("cf1", "cf2"), fromSnapshotInfo,
        toSnapshotInfo, false,
        Collections.emptyMap(), diffDir);
    assertEquals(randomStrings, deltaFiles);
  }

  @ParameterizedTest
  @CsvSource({"0,true", "1,true", "2,true", "5,true", "10,true", "100,true",
      "1000,true", "10000,true", "0,false", "1,false", "2,false", "5,false",
      "10,false", "100,false", "1000,false", "10000,false"})
  public void testGetDeltaFilesWithFullDiff(int numberOfFiles,
                                            boolean useFullDiff)
      throws ExecutionException, RocksDBException, IOException {
    try (MockedStatic<RdbUtil> mockedRdbUtil =
             Mockito.mockStatic(RdbUtil.class);
         MockedStatic<RocksDiffUtils> mockedRocksDiffUtils =
             Mockito.mockStatic(RocksDiffUtils.class)) {
      Set<String> deltaStrings = new HashSet<>();

      mockedRdbUtil.when(
              () -> RdbUtil.getSSTFilesForComparison(anyString(), anyList()))
          .thenAnswer((Answer<Set<String>>) invocation -> {
            Set<String> retVal = IntStream.range(0, numberOfFiles)
                .mapToObj(i -> RandomStringUtils.randomAlphabetic(10))
                .collect(Collectors.toSet());
            deltaStrings.addAll(retVal);
            return retVal;
          });

      mockedRocksDiffUtils.when(() ->
              RocksDiffUtils.filterRelevantSstFiles(anySet(), anyMap()))
          .thenAnswer((Answer<Void>) invocationOnMock -> {
            invocationOnMock.getArgument(0, Set.class).stream()
                .findAny().ifPresent(val -> {
                  assertTrue(deltaStrings.contains(val));
                  invocationOnMock.getArgument(0, Set.class).remove(val);
                  deltaStrings.remove(val);
                });
            return null;
          });
      SnapshotDiffManager spy = spy(snapshotDiffManager);
      UUID snap1 = UUID.randomUUID();
      UUID snap2 = UUID.randomUUID();
      if (!useFullDiff) {
        Set<String> randomStrings = Collections.emptySet();
        when(differ.getSSTDiffListWithFullPath(
            any(DifferSnapshotInfo.class),
            any(DifferSnapshotInfo.class),
            anyString()))
            .thenReturn(Lists.newArrayList(randomStrings));
      }

      SnapshotInfo fromSnapshotInfo = getMockedSnapshotInfo(snap1);
      SnapshotInfo toSnapshotInfo = getMockedSnapshotInfo(snap1);
      when(jobTableIterator.isValid()).thenReturn(false);
      Set<String> deltaFiles = spy.getDeltaFiles(
          snapshotCache.get(snap1.toString()),
          snapshotCache.get(snap2.toString()),
          Arrays.asList("cf1", "cf2"),
          fromSnapshotInfo,
          toSnapshotInfo,
          false,
          Collections.emptyMap(),
          Files.createTempDirectory("snapdiff_dir").toAbsolutePath()
              .toString());
      assertEquals(deltaStrings, deltaFiles);
    }
  }

  private Table<String, ? extends WithParentObjectId> getMockedTable(
      Map<String, WithParentObjectId> map, String tableName)
      throws IOException {
    Table<String, ? extends WithParentObjectId> mocked = mock(Table.class);
    when(mocked.get(any()))
        .thenAnswer(invocation -> map.get(invocation.getArgument(0)));
    when(mocked.getName()).thenReturn(tableName);
    return mocked;
  }

  private WithParentObjectId getKeyInfo(int objectId, int updateId,
                                        int parentObjectId,
                                        String snapshotTableName) {
    String name = "key" + objectId;
    if (snapshotTableName.equals(OmMetadataManagerImpl.DIRECTORY_TABLE)) {
      return OmDirectoryInfo.newBuilder()
          .setObjectID(objectId).setName(name).build();
    }
    return new OmKeyInfo.Builder().setObjectID(objectId)
        .setParentObjectID(parentObjectId)
        .setVolumeName("vol").setBucketName("bucket").setUpdateID(updateId)
        .setReplicationConfig(new ECReplicationConfig(3, 2))
        .setKeyName(name).build();
  }

  /**
   * Test mocks the SSTFileReader to return object Ids from 0-50
   * when not reading tombstones & Object Ids 0-100 when reading tombstones.
   * Creating a mock snapshot table where the fromSnapshot Table contains
   * Object Ids in the range 0-25 & 50-100 and toSnapshot Table contains data
   * with object Ids in the range 0-50.
   * Function should return 25-50 in the new Persistent map.
   * In the case of reading tombstones old Snapshot Persistent map should have
   * object Ids in the range 50-100 & should be empty otherwise
   */
  @SuppressFBWarnings({"DLS_DEAD_LOCAL_STORE",
      "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"})
  @ParameterizedTest
  @CsvSource({"false," + OmMetadataManagerImpl.DIRECTORY_TABLE,
      "true," + OmMetadataManagerImpl.DIRECTORY_TABLE,
      "false," + OmMetadataManagerImpl.FILE_TABLE,
      "true," + OmMetadataManagerImpl.FILE_TABLE,
      "false," + OmMetadataManagerImpl.KEY_TABLE,
      "true," + OmMetadataManagerImpl.KEY_TABLE})
  public void testObjectIdMapWithTombstoneEntries(boolean nativeLibraryLoaded,
                                                  String snapshotTableName)
      throws NativeLibraryNotLoadedException, IOException, RocksDBException {
    Set<String> keysIncludingTombstones = IntStream.range(0, 100)
        .boxed().map(i -> (i + 100) + "/key" + i).collect(Collectors.toSet());
    // Mocking SST file with keys in SST file excluding tombstones
    Set<String> keysExcludingTombstones = IntStream.range(0, 50).boxed()
        .map(i -> (i + 100) + "/key" + i).collect(Collectors.toSet());

    // Mocking SSTFileReader functions to return the above keys list.
    try (MockedConstruction<ManagedSstFileReader> mockedSSTFileReader =
             Mockito.mockConstruction(ManagedSstFileReader.class,
                 (mock, context) -> {
                   when(mock.getKeyStreamWithTombstone(any()))
                       .thenReturn(keysIncludingTombstones.stream());
                   when(mock.getKeyStream())
                       .thenReturn(keysExcludingTombstones.stream());
                 });
         MockedConstruction<ManagedSSTDumpTool> mockedSSTDumpTool =
             Mockito.mockConstruction(ManagedSSTDumpTool.class,
                 (mock, context) -> {
                 })
    ) {
      Map<String, WithParentObjectId> toSnapshotTableMap =
          IntStream.concat(IntStream.range(0, 25), IntStream.range(50, 100))
              .boxed().collect(Collectors.toMap(i -> (i + 100) + "/key" + i,
                  i -> getKeyInfo(i, i, i + 100,
                      snapshotTableName)));
      Table<String, ? extends WithParentObjectId> toSnapshotTable =
          getMockedTable(toSnapshotTableMap, snapshotTableName);

      Map<String, WithParentObjectId> fromSnapshotTableMap =
          IntStream.range(0, 50)
              .boxed().collect(Collectors.toMap(i -> (i + 100) + "/key" + i,
                  i -> getKeyInfo(i, i, i + 100, snapshotTableName)));

      Table<String, ? extends WithParentObjectId> fromSnapshotTable =
          getMockedTable(fromSnapshotTableMap, snapshotTableName);

      snapshotDiffManager = new SnapshotDiffManager(db, differ, ozoneManager,
          snapshotCache, snapDiffJobTable, snapDiffReportTable,
          columnFamilyOptions, codecRegistry);
      SnapshotDiffManager spy = spy(snapshotDiffManager);

      doAnswer(invocation -> {
            String[] split = invocation.getArgument(0, String.class).split("/");
            String keyName = split[split.length - 1];
            return Integer.parseInt(keyName.substring(3)) % 2 == 0;
          }
      ).when(spy).isKeyInBucket(anyString(), anyMap(), anyString());

      PersistentMap<byte[], byte[]> oldObjectIdKeyMap =
          new StubbedPersistentMap<>();
      PersistentMap<byte[], byte[]> newObjectIdKeyMap =
          new SnapshotTestUtils.StubbedPersistentMap<>();
      PersistentMap<byte[], SnapshotDiffObject> objectIdsToCheck =
          new SnapshotTestUtils.StubbedPersistentMap<>();

      Set<Long> oldParentIds = Sets.newHashSet();
      Set<Long> newParentIds = Sets.newHashSet();

      spy.addToObjectIdMap(toSnapshotTable,
          fromSnapshotTable, Sets.newHashSet("dummy.sst"),
          nativeLibraryLoaded, oldObjectIdKeyMap, newObjectIdKeyMap,
          objectIdsToCheck, Optional.of(oldParentIds),
          Optional.of(newParentIds),
          ImmutableMap.of(OmMetadataManagerImpl.DIRECTORY_TABLE, "",
              OmMetadataManagerImpl.KEY_TABLE, "",
              OmMetadataManagerImpl.FILE_TABLE, ""));

      try (ClosableIterator<Map.Entry<byte[], byte[]>> oldObjectIdIter =
               oldObjectIdKeyMap.iterator()) {
        int oldObjectIdCnt = 0;
        while (oldObjectIdIter.hasNext()) {
          Map.Entry<byte[], byte[]> v = oldObjectIdIter.next();
          long objectId = codecRegistry.asObject(v.getKey(), Long.class);
          assertEquals(0, objectId % 2);
          assertTrue(objectId >= 50);
          assertTrue(objectId < 100);
          oldObjectIdCnt += 1;
        }
        assertEquals(nativeLibraryLoaded ? 25 : 0, oldObjectIdCnt);
      }

      try (ClosableIterator<Map.Entry<byte[], byte[]>> newObjectIdIter =
               newObjectIdKeyMap.iterator()) {
        int newObjectIdCnt = 0;
        while (newObjectIdIter.hasNext()) {
          Map.Entry<byte[], byte[]> v = newObjectIdIter.next();
          long objectId = codecRegistry.asObject(v.getKey(), Long.class);
          assertEquals(0, objectId % 2);
          assertTrue(objectId >= 26);
          assertTrue(objectId < 50);
          newObjectIdCnt += 1;
        }
        assertEquals(12, newObjectIdCnt);
      }

      try (ClosableIterator<Entry<byte[], SnapshotDiffObject>>
               objectIdsToCheckIter = objectIdsToCheck.iterator()) {
        int objectIdCnt = 0;
        while (objectIdsToCheckIter.hasNext()) {
          Entry<byte[], SnapshotDiffObject> entry = objectIdsToCheckIter.next();
          byte[] v = entry.getKey();
          long objectId = codecRegistry.asObject(v, Long.class);
          assertEquals(0, objectId % 2);
          assertTrue(objectId >= 26);
          assertTrue(objectId < (nativeLibraryLoaded ? 100 : 50));
          objectIdCnt += 1;
        }
        assertEquals(nativeLibraryLoaded ? 37 : 12, objectIdCnt);
      }
    }
  }

  @Test
  public void testGenerateDiffReport() throws IOException {
    PersistentMap<byte[], byte[]> oldObjectIdKeyMap =
        new StubbedPersistentMap<>();
    PersistentMap<byte[], byte[]> newObjectIdKeyMap =
        new StubbedPersistentMap<>();
    PersistentMap<byte[], SnapshotDiffObject> objectIdToDiffObject =
        new SnapshotTestUtils.StubbedPersistentMap<>();
    Map<Long, SnapshotDiffReport.DiffType> diffMap = new HashMap<>();
    LongStream.range(0, 100).forEach(objectId -> {
      try {
        SnapshotDiffObjectBuilder builder =
            new SnapshotDiffObjectBuilder(objectId);
        String key = "key" + objectId;
        byte[] objectIdVal = codecRegistry.asRawData(objectId);
        byte[] keyBytes = codecRegistry.asRawData(key);
        if (objectId >= 0 && objectId <= 25 ||
            objectId >= 50 && objectId <= 100) {
          oldObjectIdKeyMap.put(objectIdVal, keyBytes);
          builder.withOldKeyName(key);
        }
        if (objectId >= 0 && objectId <= 25 && objectId % 4 == 0 ||
            objectId > 25 && objectId < 50) {
          newObjectIdKeyMap.put(objectIdVal, keyBytes);
          builder.withNewKeyName(key);
        }
        if (objectId >= 0 && objectId <= 25 && objectId % 4 == 1) {
          String renamedKey = "renamed-key" + objectId;
          byte[] renamedKeyBytes = codecRegistry.asRawData(renamedKey);
          newObjectIdKeyMap.put(objectIdVal, renamedKeyBytes);
          diffMap.put(objectId, SnapshotDiffReport.DiffType.RENAME);
          builder.withOldKeyName(key);
          builder.withNewKeyName(renamedKey);
        }
        objectIdToDiffObject.put(objectIdVal, builder.build());
        if (objectId >= 50 && objectId <= 100 ||
            objectId >= 0 && objectId <= 25 && objectId % 4 > 1) {
          diffMap.put(objectId, SnapshotDiffReport.DiffType.DELETE);
        }
        if (objectId >= 0 && objectId <= 25 && objectId % 4 == 0) {
          diffMap.put(objectId, SnapshotDiffReport.DiffType.MODIFY);
        }
        if (objectId > 25 && objectId < 50) {
          diffMap.put(objectId, SnapshotDiffReport.DiffType.CREATE);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    String volumeName = "vol";
    String bucketName = "buck";
    String fromSnapName = "fs";
    String toSnapName = "ts";

    OmKeyInfo fromKeyInfo = mock(OmKeyInfo.class);
    OmKeyInfo toKeyInfo = mock(OmKeyInfo.class);
    // This is temporary to make sure that
    // SnapshotDeletingService#isBlockLocationInfoSame always return true.
    when(toKeyInfo.isHsync()).thenReturn(true);
    when(fromKeyInfo.isHsync()).thenReturn(true);

    Table<String, OmKeyInfo> fromSnapTable = mock(Table.class);
    Table<String, OmKeyInfo> toSnapTable = mock(Table.class);
    when(fromSnapTable.get(anyString())).thenReturn(fromKeyInfo);
    when(toSnapTable.get(anyString())).thenReturn(toKeyInfo);


    SnapshotDiffManager spy = spy(snapshotDiffManager);
    doReturn(true).when(spy)
        .areDiffJobAndSnapshotsActive(volumeName, bucketName, fromSnapName,
            toSnapName);

    long totalDiffEntries = spy.generateDiffReport("jobId",
        fromSnapTable, toSnapTable, objectIdToDiffObject, oldObjectIdKeyMap,
        newObjectIdKeyMap, volumeName, bucketName, fromSnapName, toSnapName,
        false, Optional.empty(), Optional.empty());

    assertEquals(100, totalDiffEntries);
    SnapshotDiffJob snapshotDiffJob = new SnapshotDiffJob(0, "jobId",
        JobStatus.DONE, "vol", "buck", "fs", "ts",
        true, diffMap.size());
    SnapshotDiffReportOzone snapshotDiffReportOzone =
        snapshotDiffManager.createPageResponse(snapshotDiffJob, "vol",
            "buck", "fs", "ts",
            0, Integer.MAX_VALUE);
    Set<SnapshotDiffReport.DiffType> expectedOrder = new LinkedHashSet<>();
    expectedOrder.add(SnapshotDiffReport.DiffType.DELETE);
    expectedOrder.add(SnapshotDiffReport.DiffType.RENAME);
    expectedOrder.add(SnapshotDiffReport.DiffType.CREATE);
    expectedOrder.add(SnapshotDiffReport.DiffType.MODIFY);

    Set<SnapshotDiffReport.DiffType> actualOrder = new LinkedHashSet<>();
    for (DiffReportEntry entry :
        snapshotDiffReportOzone.getDiffList()) {
      actualOrder.add(entry.getType());

      long objectId = Long.parseLong(
          DFSUtilClient.bytes2String(entry.getSourcePath()).substring(4));
      assertEquals(diffMap.get(objectId), entry.getType());
    }
    assertEquals(expectedOrder, actualOrder);
  }

  private DiffReportEntry getTestDiffEntry(String jobId,
                                           int idx) throws IOException {
    return new DiffReportEntry(
        SnapshotDiffReport.DiffType.values()[idx %
            SnapshotDiffReport.DiffType.values().length],
        codecRegistry.asRawData(jobId + DELIMITER + idx));
  }

  /**
   * Testing generateDiffReport function by providing PersistentMap containing
   * objectId Map of diff keys to be checked with their corresponding key names.
   */
  @ParameterizedTest
  @CsvSource({"0,10,1000", "1,10,8", "1000,1000,10", "-1,1000,10000",
      "1,0,1000", "1,-1,1000"})
  public void testCreatePageResponse(int startIdx,
                                     int pageSize,
                                     int totalNumberOfRecords)
      throws IOException, RocksDBException {
    String testJobId = "jobId";
    String testJobId2 = "jobId2";

    IntStream.range(0, totalNumberOfRecords).boxed().forEach(idx -> {
      try {
        db.get().put(snapDiffReportTable,
            codecRegistry.asRawData(SnapshotDiffManager
                .getReportKeyForIndex(testJobId, idx)),
            codecRegistry.asRawData(getTestDiffEntry(testJobId, idx)));
        db.get().put(snapDiffReportTable,
            codecRegistry.asRawData(testJobId2 + DELIMITER + idx),
            codecRegistry.asRawData(getTestDiffEntry(testJobId2, idx)));
      } catch (IOException | RocksDBException e) {
        throw new RuntimeException(e);
      }
    });

    SnapshotDiffJob snapshotDiffJob = new SnapshotDiffJob(0, testJobId,
        JobStatus.DONE, "vol", "buck", "fs", "ts",
        true, totalNumberOfRecords);

    SnapshotDiffJob snapshotDiffJob2 = new SnapshotDiffJob(0, testJobId2,
        JobStatus.DONE, "vol", "buck", "fs", "ts",
        true, totalNumberOfRecords);

    db.get().put(snapDiffJobTable,
        codecRegistry.asRawData(testJobId),
        codecRegistry.asRawData(snapshotDiffJob));

    db.get().put(snapDiffJobTable,
        codecRegistry.asRawData(testJobId2),
        codecRegistry.asRawData(snapshotDiffJob2));

    if (pageSize <= 0 || startIdx < 0) {
      Assertions.assertThrows(IllegalArgumentException.class,
          () -> snapshotDiffManager.createPageResponse(snapshotDiffJob, "vol",
              "buck", "fs", "ts", startIdx, pageSize));
      return;
    }
    SnapshotDiffReportOzone snapshotDiffReportOzone =
        snapshotDiffManager.createPageResponse(snapshotDiffJob, "vol",
            "buck", "fs", "ts",
            startIdx, pageSize);
    int expectedTotalNumberOfRecords =
        Math.max(Math.min(pageSize, totalNumberOfRecords - startIdx), 0);
    assertEquals(snapshotDiffReportOzone.getDiffList().size(),
        expectedTotalNumberOfRecords);

    int idx = startIdx;
    for (DiffReportEntry entry : snapshotDiffReportOzone.getDiffList()) {
      assertEquals(getTestDiffEntry(testJobId, idx), entry);
      idx++;
    }
  }

  /**
   * Once a job is cancelled, it stays in the table until
   * SnapshotDiffCleanupService removes it.
   * Job response until that happens, is CANCELLED.
   */
  @Test
  public void testGetSnapshotDiffReportForCancelledJob() throws IOException {

    String volumeName = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket-" + RandomStringUtils.randomNumeric(5);

    String fromSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);
    String toSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);

    UUID fromSnapshotUUID = UUID.randomUUID();
    UUID toSnapshotUUID = UUID.randomUUID();

    setupMocksForRunningASnapDiff(volumeName, bucketName);

    setUpSnapshots(volumeName, bucketName, fromSnapshotName,
        toSnapshotName, fromSnapshotUUID, toSnapshotUUID);

    PersistentMap<String, SnapshotDiffJob> snapDiffJobMap =
        snapshotDiffManager.getSnapDiffJobTable();
    String diffJobKey = fromSnapshotUUID + DELIMITER + toSnapshotUUID;

    SnapshotDiffJob diffJob = snapDiffJobMap.get(diffJobKey);
    Assertions.assertNull(diffJob);


    SnapshotDiffManager spy = spy(snapshotDiffManager);
    doNothing().when(spy).generateSnapshotDiffReport(eq(diffJobKey),
        anyString(), eq(volumeName), eq(bucketName), eq(fromSnapshotName),
        eq(toSnapshotName), eq(false));

    // Submit a new job.
    SnapshotDiffResponse snapshotDiffResponse =
        spy.getSnapshotDiffReport(volumeName, bucketName, fromSnapshotName,
            toSnapshotName, 0, 0, false);

    assertEquals(JobStatus.IN_PROGRESS,
        snapshotDiffResponse.getJobStatus());

    // Cancel the job.
    spy.cancelSnapshotDiff(volumeName, bucketName,
        fromSnapshotName, toSnapshotName);

    // Job status should be cancelled until the cleanup
    // service removes the job from the table.
    snapshotDiffResponse = spy.getSnapshotDiffReport(volumeName, bucketName,
        fromSnapshotName, toSnapshotName, 0, 0, false);

    assertEquals(JobStatus.CANCELLED,
        snapshotDiffResponse.getJobStatus());

    // Check snapDiffJobTable.
    diffJob = snapDiffJobMap.get(diffJobKey);
    assertNotNull(diffJob);
    assertEquals(JobStatus.CANCELLED,
        diffJob.getStatus());

    // Response should still be cancelled.
    snapshotDiffResponse = spy.getSnapshotDiffReport(volumeName, bucketName,
        fromSnapshotName, toSnapshotName, 0, 0, false);

    assertEquals(JobStatus.CANCELLED,
        snapshotDiffResponse.getJobStatus());

    // Check snapDiffJobTable.
    diffJob = snapDiffJobMap.get(diffJobKey);
    assertNotNull(diffJob);
    assertEquals(JobStatus.CANCELLED,
        diffJob.getStatus());
  }

  private static Stream<Arguments> snapDiffCancelFailureScenarios() {
    return Stream.of(
        Arguments.of(JobStatus.IN_PROGRESS,
            JobCancelResult.CANCELLATION_SUCCESS, true),
        Arguments.of(JobStatus.CANCELLED,
            JobCancelResult.JOB_ALREADY_CANCELLED, true),
        Arguments.of(JobStatus.DONE,
            JobCancelResult.JOB_DONE, false),
        Arguments.of(JobStatus.QUEUED,
            JobCancelResult.INVALID_STATUS_TRANSITION, false),
        Arguments.of(JobStatus.FAILED,
            JobCancelResult.INVALID_STATUS_TRANSITION, false),
        Arguments.of(JobStatus.REJECTED,
            JobCancelResult.INVALID_STATUS_TRANSITION, false)
    );
  }

  @ParameterizedTest
  @MethodSource("snapDiffCancelFailureScenarios")
  public void testSnapshotDiffCancelFailure(JobStatus jobStatus,
                                            JobCancelResult cancelResult,
                                            boolean jobIsCancelled)
      throws IOException {

    String volumeName = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket-" + RandomStringUtils.randomNumeric(5);

    String fromSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);
    String toSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);

    UUID fromSnapshotUUID = UUID.randomUUID();
    UUID toSnapshotUUID = UUID.randomUUID();

    setupMocksForRunningASnapDiff(volumeName, bucketName);

    setUpSnapshots(volumeName, bucketName, fromSnapshotName,
        toSnapshotName, fromSnapshotUUID, toSnapshotUUID);

    PersistentMap<String, SnapshotDiffJob> snapDiffJobMap =
        snapshotDiffManager.getSnapDiffJobTable();
    String diffJobKey = fromSnapshotUUID + DELIMITER + toSnapshotUUID;

    String jobId = UUID.randomUUID().toString();
    SnapshotDiffJob snapshotDiffJob = new SnapshotDiffJob(0L,
        jobId, jobStatus, volumeName, bucketName,
        fromSnapshotName, toSnapshotName, true, 10);

    snapDiffJobMap.put(diffJobKey, snapshotDiffJob);

    SnapshotDiffResponse snapshotDiffResponse = snapshotDiffManager
        .cancelSnapshotDiff(volumeName, bucketName,
            fromSnapshotName, toSnapshotName);

    assertEquals(cancelResult, snapshotDiffResponse.getJobCancelResult());

    if (jobIsCancelled) {
      assertEquals(JobStatus.CANCELLED, snapshotDiffResponse.getJobStatus());
    }
  }

  @Test
  public void testCancelNewSnapshotDiff() throws IOException {
    String volumeName = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket-" + RandomStringUtils.randomNumeric(5);

    String fromSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);
    String toSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);

    UUID fromSnapshotUUID = UUID.randomUUID();
    UUID toSnapshotUUID = UUID.randomUUID();

    setupMocksForRunningASnapDiff(volumeName, bucketName);

    setUpSnapshots(volumeName, bucketName, fromSnapshotName,
        toSnapshotName, fromSnapshotUUID, toSnapshotUUID);

    SnapshotDiffResponse snapshotDiffResponse = snapshotDiffManager
        .cancelSnapshotDiff(volumeName, bucketName,
            fromSnapshotName, toSnapshotName);

    // The job doesn't exist on the SnapDiffJob table and
    // trying to cancel it should lead to NEW_JOB cancel result.
    assertEquals(JobCancelResult.NEW_JOB,
        snapshotDiffResponse.getJobCancelResult());
  }

  private static Stream<Arguments> listSnapshotDiffJobsScenarios() {
    return Stream.of(
        Arguments.of("queued", false, false),
        Arguments.of("done", false, false),
        Arguments.of("in_progress", false, true),
        Arguments.of("queued", true, true),
        Arguments.of("done", true, true),
        Arguments.of("in_progress", true, true),
        Arguments.of("invalid", true, true),
        Arguments.of("", true, true)
    );
  }

  @ParameterizedTest
  @MethodSource("listSnapshotDiffJobsScenarios")
  public void testListSnapshotDiffJobs(String jobStatus,
                                       boolean listAll,
                                       boolean containsJob)
      throws IOException {
    String volumeName = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket-" + RandomStringUtils.randomNumeric(5);
    String fromSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);
    String toSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);

    UUID fromSnapshotUUID = UUID.randomUUID();
    UUID toSnapshotUUID = UUID.randomUUID();

    setUpSnapshots(volumeName, bucketName, fromSnapshotName,
        toSnapshotName, fromSnapshotUUID, toSnapshotUUID);

    PersistentMap<String, SnapshotDiffJob> snapDiffJobMap =
        snapshotDiffManager.getSnapDiffJobTable();
    String diffJobKey = fromSnapshotUUID + DELIMITER + toSnapshotUUID;

    SnapshotDiffJob diffJob = snapDiffJobMap.get(diffJobKey);
    assertNull(diffJob);

    // There are no jobs in the table, therefore
    // the response list should be empty.
    List<SnapshotDiffJob> jobList = snapshotDiffManager
        .getSnapshotDiffJobList(volumeName, bucketName, jobStatus, listAll);
    assertTrue(jobList.isEmpty());

    SnapshotDiffManager spy = spy(snapshotDiffManager);
    doNothing().when(spy).generateSnapshotDiffReport(eq(diffJobKey),
        anyString(), eq(volumeName), eq(bucketName), eq(fromSnapshotName),
        eq(toSnapshotName), eq(false));

    // SnapshotDiffReport
    SnapshotDiffResponse snapshotDiffResponse =
        spy.getSnapshotDiffReport(volumeName, bucketName, fromSnapshotName,
            toSnapshotName, 0, 0, false);

    assertEquals(SnapshotDiffResponse.JobStatus.IN_PROGRESS,
        snapshotDiffResponse.getJobStatus());

    diffJob = snapDiffJobMap.get(diffJobKey);
    assertNotNull(diffJob);
    assertEquals(SnapshotDiffResponse.JobStatus.IN_PROGRESS,
        diffJob.getStatus());

    jobList = snapshotDiffManager
        .getSnapshotDiffJobList(volumeName, bucketName, jobStatus, listAll);

    // When listAll is true, jobStatus is ignored.
    // If the job is IN_PROGRESS or listAll is used,
    // there should be a response.
    // Otherwise, response list should be empty.
    if (containsJob) {
      assertTrue(jobList.contains(diffJob));
    } else {
      assertTrue(jobList.isEmpty());
    }
  }

  @Test
  public void testListSnapDiffWithInvalidStatus() throws IOException {
    String volumeName = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket-" + RandomStringUtils.randomNumeric(5);
    String fromSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);
    String toSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);

    UUID fromSnapshotUUID = UUID.randomUUID();
    UUID toSnapshotUUID = UUID.randomUUID();

    setUpSnapshots(volumeName, bucketName, fromSnapshotName,
        toSnapshotName, fromSnapshotUUID, toSnapshotUUID);

    String diffJobKey = fromSnapshotUUID + DELIMITER + toSnapshotUUID;
    SnapshotDiffManager spy = spy(snapshotDiffManager);

    doNothing().when(spy).generateSnapshotDiffReport(eq(diffJobKey),
        anyString(), eq(volumeName), eq(bucketName), eq(fromSnapshotName),
        eq(toSnapshotName), eq(false));

    spy.getSnapshotDiffReport(volumeName, bucketName, fromSnapshotName,
            toSnapshotName, 0, 0, false);

    // Invalid status, without listAll true, results in an exception.
    assertThrows(IOException.class, () -> snapshotDiffManager
        .getSnapshotDiffJobList(volumeName, bucketName, "invalid", false));
  }

  @Test
  public void testGenerateDiffReportWhenThereInEntry() {
    PersistentMap<byte[], SnapshotDiffObject> objectIdToDiffObject =
        new StubbedPersistentMap<>();
    PersistentMap<byte[], byte[]> oldObjIdToKeyMap =
        new StubbedPersistentMap<>();
    PersistentMap<byte[], byte[]> newObjIdToKeyMap =
        new StubbedPersistentMap<>();

    long totalDiffEntries = snapshotDiffManager.generateDiffReport("jobId",
        keyInfoTable,
        keyInfoTable,
        objectIdToDiffObject,
        oldObjIdToKeyMap,
        newObjIdToKeyMap,
        "volume",
        "bucket",
        "fromSnapshot",
        "toSnapshot",
        false,
        Optional.empty(),
        Optional.empty());

    assertEquals(0, totalDiffEntries);
  }

  @Test
  public void testGenerateDiffReportFailure() throws IOException {
    String volumeName = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket-" + RandomStringUtils.randomNumeric(5);
    String fromSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);
    String toSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);

    PersistentMap<byte[], SnapshotDiffObject> objectIdToDiffObject =
        new SnapshotTestUtils.StubbedPersistentMap<>();
    PersistentMap<byte[], byte[]> oldObjIdToKeyMap =
        new StubbedPersistentMap<>();
    PersistentMap<byte[], byte[]> newObjIdToKeyMap =
        new StubbedPersistentMap<>();
    objectIdToDiffObject.put(codecRegistry.asRawData("randomKey"),
        new SnapshotDiffObjectBuilder(1L).build());

    SnapshotDiffManager spy = spy(snapshotDiffManager);
    doReturn(true).when(spy)
        .areDiffJobAndSnapshotsActive(volumeName, bucketName,
            fromSnapshotName, toSnapshotName);

    IllegalStateException exception = assertThrows(IllegalStateException.class,
        () -> spy.generateDiffReport("jobId",
            keyInfoTable,
            keyInfoTable,
            objectIdToDiffObject,
            oldObjIdToKeyMap,
            newObjIdToKeyMap,
            volumeName,
            bucketName,
            fromSnapshotName,
            toSnapshotName,
            false,
            Optional.empty(),
            Optional.empty())
    );
    assertEquals("Old and new key name both are null",
        exception.getMessage());
  }

  /**
   * Tests that IN_PROGRESS jobs are submitted to the executor on the service
   * startup.
   */
  @Test
  public void testLoadJobsOnStartUp() throws Exception {
    for (int i = 0; i < snapshotInfoList.size(); i++) {
      uploadSnapshotDiffJobToDb(snapshotInfo, snapshotInfoList.get(i),
          snapDiffJobs.get(i));
    }

    SnapshotDiffManager spy = spy(snapshotDiffManager);

    doAnswer(invocation -> {
          SnapshotDiffJob diffJob = getSnapshotDiffJobFromDb(snapshotInfo,
              snapshotInfoList.get(1));
          diffJob.setTotalDiffEntries(1L);
          diffJob.setStatus(DONE);
          uploadSnapshotDiffJobToDb(snapshotInfo,
              snapshotInfoList.get(1),
              diffJob);
          return null;
        }
    ).when(spy).generateSnapshotDiffReport(anyString(), anyString(),
        eq(VOLUME_NAME), eq(BUCKET_NAME), eq(snapshotInfo.getName()),
        eq(snapshotInfoList.get(1).getName()), eq(false));

    spy.loadJobsOnStartUp();

    // Wait for sometime to make sure that job finishes.
    Thread.sleep(1000L);

    SnapshotDiffJob snapDiffJob = getSnapshotDiffJobFromDb(snapshotInfo,
        snapshotInfoList.get(1));

    assertEquals(DONE, snapDiffJob.getStatus());
    assertEquals(1L, snapDiffJob.getTotalDiffEntries());
  }

  private SnapshotDiffJob getSnapshotDiffJobFromDb(SnapshotInfo fromSnapshot,
                                                   SnapshotInfo toSnapshot)
      throws IOException, RocksDBException {
    String jobKey = generateSnapDiffJobKey.apply(fromSnapshot, toSnapshot);

    byte[] bytes = db.get()
        .get(snapDiffJobTable, codecRegistry.asRawData(jobKey));
    return codecRegistry.asObject(bytes, SnapshotDiffJob.class);
  }

  private void uploadSnapshotDiffJobToDb(SnapshotInfo fromSnapshot,
                                         SnapshotInfo toSnapshot,
                                         SnapshotDiffJob diffJob)
      throws IOException, RocksDBException {
    String jobKey = generateSnapDiffJobKey.apply(fromSnapshot, toSnapshot);

    byte[] keyBytes = codecRegistry.asRawData(jobKey);
    byte[] jobBytes = codecRegistry.asRawData(diffJob);
    db.get().put(snapDiffJobTable, keyBytes, jobBytes);
  }

  private static Stream<Arguments> threadPoolFullScenarios() {
    return Stream.of(
        Arguments.of("When there is a wait time between job batches",
            500L, 45, 0),
        Arguments.of("When there is no wait time between job batches",
            0L, 20, 25)
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("threadPoolFullScenarios")
  public void testThreadPoolIsFull(String description,
                                   long waitBetweenBatches,
                                   int expectInProgressJobsCount,
                                   int expectRejectedJobsCount)
      throws Exception {
    ExecutorService executorService = new ThreadPoolExecutor(100, 100, 0,
        TimeUnit.MILLISECONDS, new SynchronousQueue<>()
    );

    List<SnapshotInfo> snapshotInfos = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      UUID snapshotId = UUID.randomUUID();
      String snapshotName = "snap-" + snapshotId;
      SnapshotInfo snapInfo = new SnapshotInfo.Builder()
          .setSnapshotId(snapshotId)
          .setVolumeName(VOLUME_NAME)
          .setBucketName(BUCKET_NAME)
          .setName(snapshotName)
          .setSnapshotPath("fromSnapshotPath")
          .setCheckpointDir("fromSnapshotCheckpointDir")
          .build();
      snapshotInfos.add(snapInfo);

      when(snapshotInfoTable.get(getTableKey(VOLUME_NAME, BUCKET_NAME,
          snapshotName))).thenReturn(snapInfo);
    }

    SnapshotDiffManager spy = spy(snapshotDiffManager);

    for (int i = 0; i < snapshotInfos.size(); i++) {
      for (int j = i + 1; j < snapshotInfos.size(); j++) {
        String fromSnapshotName = snapshotInfos.get(i).getName();
        String toSnapshotName = snapshotInfos.get(j).getName();

        doAnswer(invocation -> {
          Thread.sleep(250L);
          return null;
        }).when(spy).generateSnapshotDiffReport(anyString(), anyString(),
            eq(VOLUME_NAME), eq(BUCKET_NAME), eq(fromSnapshotName),
            eq(toSnapshotName), eq(false));
      }
    }

    List<Future<SnapshotDiffResponse>> futures = new ArrayList<>();
    for (int i = 0; i < snapshotInfos.size(); i++) {
      for (int j = i + 1; j < snapshotInfos.size(); j++) {
        String fromSnapshotName = snapshotInfos.get(i).getName();
        String toSnapshotName = snapshotInfos.get(j).getName();

        Future<SnapshotDiffResponse> future = executorService.submit(
            () -> submitJob(spy, fromSnapshotName, toSnapshotName));
        futures.add(future);
      }
      Thread.sleep(waitBetweenBatches);
    }

    // Wait to make sure that all jobs finish before assertion.
    Thread.sleep(1000L);
    int inProgressJobsCount = 0;
    int rejectedJobsCount = 0;

    for (Future<SnapshotDiffResponse> future : futures) {
      SnapshotDiffResponse response = future.get();
      if (response.getJobStatus() == IN_PROGRESS) {
        inProgressJobsCount++;
      } else if (response.getJobStatus() == REJECTED) {
        rejectedJobsCount++;
      } else {
        throw new IllegalStateException("Unexpected job status.");
      }
    }

    assertEquals(expectInProgressJobsCount, inProgressJobsCount);
    assertEquals(expectRejectedJobsCount, rejectedJobsCount);

    int notFoundJobs = 0;
    for (int i = 0; i < snapshotInfos.size(); i++) {
      for (int j = i + 1; j < snapshotInfos.size(); j++) {
        SnapshotDiffJob diffJob =
            getSnapshotDiffJobFromDb(snapshotInfos.get(i),
                snapshotInfos.get(j));
        if (diffJob == null) {
          notFoundJobs++;
        }
      }
    }

    // assert that rejected jobs were removed from the job table as well.
    assertEquals(expectRejectedJobsCount, notFoundJobs);
    executorService.shutdown();
  }

  private SnapshotDiffResponse submitJob(SnapshotDiffManager diffManager,
                                         String fromSnapshotName,
                                         String toSnapshotName) {
    try {
      return diffManager.getSnapshotDiffReport(VOLUME_NAME, BUCKET_NAME,
          fromSnapshotName, toSnapshotName, 0, 1000, false);
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
  }

  private void setUpSnapshots(String volumeName, String bucketName,
                              String fromSnapshotName, String toSnapshotName,
                              UUID fromSnapshotUUID, UUID toSnapshotUUID)
      throws IOException {


    SnapshotInfo fromSnapshotInfo =
        getSnapshotInfoInstance(volumeName, bucketName,
            fromSnapshotName, fromSnapshotUUID);
    SnapshotInfo toSnapshotInfo =
        getSnapshotInfoInstance(volumeName, bucketName,
            toSnapshotName, toSnapshotUUID);

    String fromSnapKey = getTableKey(volumeName, bucketName, fromSnapshotName);
    String toSnapKey = getTableKey(volumeName, bucketName, toSnapshotName);

    when(snapshotInfoTable.get(fromSnapKey)).thenReturn(fromSnapshotInfo);
    when(snapshotInfoTable.get(toSnapKey)).thenReturn(toSnapshotInfo);
  }

  private SnapshotInfo getSnapshotInfoInstance(String volumeName,
                                               String bucketName,
                                               String snapshotName,
                                               UUID snapshotUUID) {
    SnapshotInfo info = SnapshotInfo.newInstance(volumeName, bucketName,
        snapshotName, snapshotUUID, System.currentTimeMillis());
    info.setSnapshotStatus(SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE);
    return info;
  }

  private void setupMocksForRunningASnapDiff(
      String volumeName, String bucketName)
      throws IOException {
    Map<BucketLayout, String> keyTableMap = new HashMap<>();
    keyTableMap.put(BucketLayout.FILE_SYSTEM_OPTIMIZED,
        OmMetadataManagerImpl.FILE_TABLE);
    keyTableMap.put(BucketLayout.OBJECT_STORE,
        OmMetadataManagerImpl.KEY_TABLE);
    keyTableMap.put(BucketLayout.LEGACY,
        OmMetadataManagerImpl.KEY_TABLE);

    for (Map.Entry<BucketLayout, String> entry : keyTableMap.entrySet()) {
      when(omMetadataManager.getKeyTable(entry.getKey()))
          .thenReturn(keyInfoTable);
      when(omMetadataManager.getKeyTable(entry.getKey()).getName())
          .thenReturn(entry.getValue());
    }

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .setOwner(ugi.getShortUserName())
        .build();

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    when(bucketInfoTable.get(bucketKey)).thenReturn(bucketInfo);
  }

  @Test
  public void testGetSnapshotDiffReportHappyCase() throws Exception {
    SnapshotInfo fromSnapInfo = snapshotInfo;
    SnapshotInfo toSnapInfo = snapshotInfoList.get(0);

    Set<String> testDeltaFiles = new HashSet<>();

    SnapshotDiffManager spy = spy(snapshotDiffManager);

    doReturn(testDeltaFiles).when(spy).getDeltaFiles(any(OmSnapshot.class),
        any(OmSnapshot.class), anyList(), eq(fromSnapInfo), eq(toSnapInfo),
        eq(false), anyMap(), anyString());

    doReturn(testDeltaFiles).when(spy)
        .getSSTFileListForSnapshot(any(OmSnapshot.class), anyList());

    doNothing().when(spy).addToObjectIdMap(eq(keyInfoTable), eq(keyInfoTable),
        any(), anyBoolean(), any(), any(), any(), any(), any(), anyMap());
    doNothing().when(spy).checkReportsIntegrity(any(), anyInt(), anyInt());

    doReturn(10L).when(spy).generateDiffReport(anyString(),
        any(), any(), any(), any(), any(), anyString(), anyString(),
        anyString(), anyString(), anyBoolean(), any(), any());
    doReturn(LEGACY).when(spy).getBucketLayout(VOLUME_NAME, BUCKET_NAME,
        omMetadataManager);

    spy.getSnapshotDiffReport(VOLUME_NAME, BUCKET_NAME, fromSnapInfo.getName(),
        toSnapInfo.getName(), 0, 1000, false);

    Thread.sleep(1000L);
    spy.getSnapshotDiffReport(VOLUME_NAME, BUCKET_NAME, fromSnapInfo.getName(),
        toSnapInfo.getName(), 0, 1000, false);

    SnapshotDiffJob snapDiffJob = getSnapshotDiffJobFromDb(fromSnapInfo,
        toSnapInfo);
    assertEquals(DONE, snapDiffJob.getStatus());
    assertEquals(10L, snapDiffJob.getTotalDiffEntries());
  }

  /**
   * Tests that only QUEUED jobs are submitted to the executor and rest are
   * short-circuited based on previous one.
   */
  @Disabled
  @Test
  public void testGetSnapshotDiffReportJob() throws Exception {
    for (int i = 0; i < jobStatuses.size(); i++) {
      uploadSnapshotDiffJobToDb(snapshotInfo, snapshotInfoList.get(i),
          snapDiffJobs.get(i));
    }

    SnapshotDiffManager spy = spy(snapshotDiffManager);

    doAnswer(invocation -> {
          SnapshotDiffJob diffJob = getSnapshotDiffJobFromDb(snapshotInfo,
              snapshotInfoList.get(0));
          diffJob.setTotalDiffEntries(1L);
          diffJob.setStatus(DONE);
          uploadSnapshotDiffJobToDb(snapshotInfo,
              snapshotInfoList.get(0),
              diffJob);
          return null;
        }
    ).when(spy).generateSnapshotDiffReport(anyString(), anyString(),
        eq(VOLUME_NAME), eq(BUCKET_NAME), eq(snapshotInfo.getName()),
        eq(snapshotInfoList.get(0).getName()), eq(false));

    for (int i = 0; i < snapshotInfoList.size(); i++) {
      SnapshotDiffResponse snapshotDiffReport =
          spy.getSnapshotDiffReport(VOLUME_NAME, BUCKET_NAME,
              snapshotInfo.getName(), snapshotInfoList.get(i).getName(), 0,
              1000,
              false);
      SnapshotDiffJob diffJob = snapDiffJobs.get(i);
      if (diffJob.getStatus() == QUEUED) {
        assertEquals(IN_PROGRESS, snapshotDiffReport.getJobStatus());
      } else {
        assertEquals(diffJob.getStatus(), snapshotDiffReport.getJobStatus());
      }
    }
  }
}
