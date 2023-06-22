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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSSTDumpTool;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
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
import org.apache.hadoop.ozone.om.snapshot.SnapshotDiffObject.SnapshotDiffObjectBuilder;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobCancelResult;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ClosableIterator;
import org.apache.ozone.rocksdb.util.ManagedSstFileReader;
import org.apache.ozone.rocksdb.util.RdbUtil;
import org.apache.ozone.rocksdiff.DifferSnapshotInfo;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.apache.ozone.rocksdiff.RocksDiffUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.DELIMITER;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;

/**
 * Test class for SnapshotDiffManager Class.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TestSnapshotDiffManager {

  @Mock
  private ManagedRocksDB snapdiffDB;

  @Mock
  private RocksDBCheckpointDiffer differ;

  @Mock
  private OzoneManager ozoneManager;

  private LoadingCache<String, OmSnapshot> snapshotCache;

  @Mock
  private ColumnFamilyHandle snapdiffJobCFH;

  @Mock
  private ColumnFamilyHandle snapdiffReportCFH;

  @Mock
  private ManagedColumnFamilyOptions columnFamilyOptions;

  @Mock
  private RocksDB rocksDB;

  @Mock
  private RocksIterator jobTableIterator;

  private static CodecRegistry codecRegistry;

  @BeforeAll
  public static void initCodecRegistry() {
    // Integers are used for indexing persistent list.
    codecRegistry = CodecRegistry.newBuilder()
        .addCodec(SnapshotDiffReportOzone.DiffReportEntry.class,
            SnapshotDiffReportOzone.getDiffReportEntryCodec())
        .addCodec(SnapshotDiffJob.class, SnapshotDiffJob.getCodec()).build();
  }

  private DBStore getMockedDBStore(String dbStorePath) {
    DBStore dbStore = mock(DBStore.class);
    when(dbStore.getDbLocation()).thenReturn(new File(dbStorePath));
    return dbStore;
  }

  private OmSnapshot getMockedOmSnapshot(String snapshot) {
    OmSnapshot omSnapshot = Mockito.mock(OmSnapshot.class);
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    DBStore dbStore = getMockedDBStore(snapshot);
    Mockito.when(omSnapshot.getName()).thenReturn(snapshot);
    Mockito.when(omSnapshot.getMetadataManager()).thenReturn(metadataManager);
    Mockito.when(metadataManager.getStore()).thenReturn(dbStore);
    return omSnapshot;
  }

  private SnapshotDiffManager getMockedSnapshotDiffManager(int cacheSize)
      throws IOException {
    when(snapdiffDB.get()).thenReturn(rocksDB);
    when(rocksDB.newIterator(snapdiffJobCFH))
        .thenReturn(jobTableIterator);
    when(rocksDB.newIterator(Mockito.eq(snapdiffJobCFH),
            Mockito.any(ReadOptions.class)))
        .thenReturn(jobTableIterator);
    CacheLoader<String, OmSnapshot> loader =
        new CacheLoader<String, OmSnapshot>() {
          @Override
          public OmSnapshot load(String key) {
            return getMockedOmSnapshot(key);
          }
        };
    snapshotCache = CacheBuilder.newBuilder()
        .maximumSize(cacheSize)
        .build(loader);
    Mockito.when(ozoneManager.getConfiguration())
        .thenReturn(new OzoneConfiguration());
    OMMetadataManager mockedMetadataManager =
        Mockito.mock(OMMetadataManager.class);
    RDBStore mockedRDBStore = Mockito.mock(RDBStore.class);
    Path diffDir = Files.createTempDirectory("snapdiff_dir");
    Mockito.when(mockedRDBStore.getSnapshotMetadataDir())
        .thenReturn(diffDir.toString());
    Mockito.when(mockedMetadataManager.getStore()).thenReturn(mockedRDBStore);
    Mockito.when(ozoneManager.getMetadataManager())
        .thenReturn(mockedMetadataManager);
    SnapshotDiffManager snapshotDiffManager = Mockito.spy(
        new SnapshotDiffManager(snapdiffDB, differ, ozoneManager, snapshotCache,
            snapdiffJobCFH, snapdiffReportCFH, columnFamilyOptions,
            codecRegistry));
    PersistentMap<String, SnapshotDiffJob> snapDiffJobTable =
        new SnapshotTestUtils.StubbedPersistentMap<>();
    HddsWhiteboxTestUtils.setInternalState(snapshotDiffManager,
        "snapDiffJobTable", snapDiffJobTable);
    return snapshotDiffManager;
  }

  private SnapshotInfo getMockedSnapshotInfo(UUID snapshotId) {
    SnapshotInfo snapshotInfo = mock(SnapshotInfo.class);
    Mockito.when(snapshotInfo.getSnapshotId()).thenReturn(snapshotId);
    return snapshotInfo;
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 5, 10, 100, 1000, 10000})
  public void testGetDeltaFilesWithDag(int numberOfFiles)
      throws ExecutionException, RocksDBException, IOException {

    SnapshotDiffManager snapshotDiffManager = getMockedSnapshotDiffManager(10);
    UUID snap1 = UUID.randomUUID();
    UUID snap2 = UUID.randomUUID();

    String diffDir = Files.createTempDirectory("snapdiff_dir").toString();
    Set<String> randomStrings = IntStream.range(0, numberOfFiles)
        .mapToObj(i -> RandomStringUtils.randomAlphabetic(10))
        .collect(Collectors.toSet());
    Mockito.when(differ.getSSTDiffListWithFullPath(Mockito.any(),
        Mockito.any(), Mockito.eq(diffDir)))
        .thenReturn(Lists.newArrayList(randomStrings));
    SnapshotInfo fromSnapshotInfo = getMockedSnapshotInfo(snap1);
    SnapshotInfo toSnapshotInfo = getMockedSnapshotInfo(snap1);
    Mockito.when(jobTableIterator.isValid()).thenReturn(false);
    Set<String> deltaFiles = snapshotDiffManager.getDeltaFiles(
        snapshotCache.get(snap1.toString()),
        snapshotCache.get(snap2.toString()),
        Arrays.asList("cf1", "cf2"), fromSnapshotInfo, toSnapshotInfo, false,
        Collections.EMPTY_MAP, diffDir);
    Assertions.assertEquals(randomStrings, deltaFiles);
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
              () -> RdbUtil.getSSTFilesForComparison(Matchers.anyString(),
                  Matchers.anyList()))
          .thenAnswer((Answer<Set<String>>) invocation -> {
            Set<String> retVal = IntStream.range(0, numberOfFiles)
                .mapToObj(i -> RandomStringUtils.randomAlphabetic(10))
                .collect(Collectors.toSet());
            deltaStrings.addAll(retVal);
            return retVal;
          });
      mockedRocksDiffUtils.when(() -> RocksDiffUtils.filterRelevantSstFiles(
              Matchers.anySet(), Matchers.anyMap()))
          .thenAnswer((Answer<Void>) invocationOnMock -> {
            invocationOnMock.getArgument(0, Set.class).stream()
                .findAny().ifPresent(val -> {
                  Assertions.assertTrue(deltaStrings.contains(val));
                  invocationOnMock.getArgument(0, Set.class).remove(val);
                  deltaStrings.remove(val);
                });
            return null;
          });
      SnapshotDiffManager snapshotDiffManager =
          getMockedSnapshotDiffManager(10);
      UUID snap1 = UUID.randomUUID();
      UUID snap2 = UUID.randomUUID();
      if (!useFullDiff) {
        Set<String> randomStrings = Collections.emptySet();
        Mockito.when(differ.getSSTDiffListWithFullPath(
            Mockito.any(DifferSnapshotInfo.class),
            Mockito.any(DifferSnapshotInfo.class),
            Matchers.anyString()))
            .thenReturn(Lists.newArrayList(randomStrings));
      }
      SnapshotInfo fromSnapshotInfo = getMockedSnapshotInfo(snap1);
      SnapshotInfo toSnapshotInfo = getMockedSnapshotInfo(snap1);
      Mockito.when(jobTableIterator.isValid()).thenReturn(false);
      Set<String> deltaFiles = snapshotDiffManager.getDeltaFiles(
          snapshotCache.get(snap1.toString()),
          snapshotCache.get(snap2.toString()),
          Arrays.asList("cf1", "cf2"), fromSnapshotInfo, toSnapshotInfo, false,
          Collections.EMPTY_MAP, Files.createTempDirectory("snapdiff_dir")
              .toAbsolutePath().toString());
      Assertions.assertEquals(deltaStrings, deltaFiles);
    }
  }

  private Table<String, ? extends WithParentObjectId> getMockedTable(
      Map<String, WithParentObjectId> map, String tableName)
      throws IOException {
    Table<String, ? extends WithParentObjectId> mocked = mock(Table.class);
    Mockito.when(mocked.get(Matchers.any()))
        .thenAnswer(invocation -> map.get(invocation.getArgument(0)));
    Mockito.when(mocked.getName()).thenReturn(tableName);
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
   * Creating a mock snapshot table where the from Snapshot Table contains
   * Object Ids in the range 0-25 & 50-100 and to Snaphshot Table contains data
   * with object Ids in the range 0-50.
   * Function should return 25-50 in the new Persistent map.
   * In the case of reading tombstones old Snapshot Persistent map should have
   * object Ids in the range 50-100 & should be empty otherwise
   *
   * @param nativeLibraryLoaded
   * @param snapshotTableName
   * @throws NativeLibraryNotLoadedException
   * @throws IOException
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
    // Mocking SST file with keys in SST file including tombstones
    Set<String> keysWithTombstones = IntStream.range(0, 100)
        .boxed().map(i -> (i + 100) + "/key" + i).collect(Collectors.toSet());
    // Mocking SST file with keys in SST file excluding tombstones
    Set<String> keys = IntStream.range(0, 50).boxed()
        .map(i -> (i + 100) + "/key" + i).collect(Collectors.toSet());
    // Mocking SSTFileReader functions to return the above keys list.
    try (MockedConstruction<ManagedSstFileReader> mockedSSTFileReader =
             Mockito.mockConstruction(ManagedSstFileReader.class,
                 (mock, context) -> {
                   Mockito.when(mock.getKeyStreamWithTombstone(Matchers.any()))
                       .thenReturn(keysWithTombstones.stream());
                   Mockito.when(mock.getKeyStream())
                       .thenReturn(keys.stream());
                 });
         MockedConstruction<ManagedSSTDumpTool> mockedSSTDumpTool =
             Mockito.mockConstruction(ManagedSSTDumpTool.class,
                 (mock, context) -> {
                 })
    ) {
      //
      Map<String, WithParentObjectId> toSnapshotTableMap =
          IntStream.concat(IntStream.range(0, 25), IntStream.range(50, 100))
              .boxed().collect(Collectors.toMap(i -> (i + 100) + "/key" + i,
                  i -> getKeyInfo(i, i, i + 100,
                      snapshotTableName)));
      // Mocking To snapshot table containing list of keys b/w 0-25, 50-100
      Table<String, ? extends WithParentObjectId> toSnapshotTable =
          getMockedTable(toSnapshotTableMap, snapshotTableName);
      // Mocking To snapshot table containing list of keys b/w 0-50
      Map<String, WithParentObjectId> fromSnapshotTableMap =
          IntStream.range(0, 50)
              .boxed().collect(Collectors.toMap(i -> (i + 100) + "/key" + i,
                  i -> getKeyInfo(i, i, i + 100, snapshotTableName)));
      // Expected Diff 25-50 are newly created keys & keys b/w are deleted,
      // when reding keys with tombstones the keys would be added to
      // objectIdsToBeChecked otherwise it wouldn't be added
      Table<String, ? extends WithParentObjectId> fromSnapshotTable =
          getMockedTable(fromSnapshotTableMap, snapshotTableName);
      SnapshotDiffManager snapshotDiffManager =
          getMockedSnapshotDiffManager(10);
      // Mocking to filter even keys in bucket.
      // Odd keys should be filtered out in the diff.
      Mockito.doAnswer((Answer<Boolean>) invocationOnMock ->
          Integer.parseInt(invocationOnMock.getArgument(0, String.class)
              .substring(7)) % 2 == 0).when(snapshotDiffManager)
          .isKeyInBucket(Matchers.anyString(), Matchers.anyMap(),
              Matchers.anyString());
      PersistentMap<byte[], byte[]> oldObjectIdKeyMap =
          new SnapshotTestUtils.StubbedPersistentMap<>();
      PersistentMap<byte[], byte[]> newObjectIdKeyMap =
          new SnapshotTestUtils.StubbedPersistentMap<>();
      PersistentMap<byte[], SnapshotDiffObject> objectIdsToCheck =
          new SnapshotTestUtils.StubbedPersistentMap<>();
      Set<Long> oldParentIds = Sets.newHashSet();
      Set<Long> newParentIds = Sets.newHashSet();
      snapshotDiffManager.addToObjectIdMap(toSnapshotTable,
          fromSnapshotTable, Sets.newHashSet("dummy.sst"),
          nativeLibraryLoaded, oldObjectIdKeyMap, newObjectIdKeyMap,
          objectIdsToCheck, Optional.ofNullable(oldParentIds),
          Optional.ofNullable(newParentIds),
          ImmutableMap.of(OmMetadataManagerImpl.DIRECTORY_TABLE, "",
              OmMetadataManagerImpl.KEY_TABLE, "",
              OmMetadataManagerImpl.FILE_TABLE, ""));

      Iterator<Entry<byte[], byte[]>> oldObjectIdIter =
          oldObjectIdKeyMap.iterator();
      int oldObjectIdCnt = 0;
      while (oldObjectIdIter.hasNext()) {
        Entry<byte[], byte[]> v = oldObjectIdIter.next();
        long objectId = this.codecRegistry.asObject(v.getKey(), Long.class);
        Assertions.assertTrue(objectId % 2 == 0);
        Assertions.assertTrue(objectId >= 50);
        Assertions.assertTrue(objectId < 100);
        oldObjectIdCnt += 1;
      }
      Assertions.assertEquals(nativeLibraryLoaded ? 25 : 0, oldObjectIdCnt);
      Iterator<Entry<byte[], byte[]>> newObjectIdIter =
          newObjectIdKeyMap.iterator();
      int newObjectIdCnt = 0;
      while (newObjectIdIter.hasNext()) {
        Entry<byte[], byte[]> v = newObjectIdIter.next();
        long objectId = this.codecRegistry.asObject(v.getKey(), Long.class);
        Assertions.assertTrue(objectId % 2 == 0);
        Assertions.assertTrue(objectId >= 26);
        Assertions.assertTrue(objectId < 50);
        newObjectIdCnt += 1;
      }
      Assertions.assertEquals(12, newObjectIdCnt);

      ClosableIterator<Entry<byte[], SnapshotDiffObject>> objectIdsToCheckIter =
          objectIdsToCheck.iterator();
      int objectIdCnt = 0;
      while (objectIdsToCheckIter.hasNext()) {
        Entry<byte[], SnapshotDiffObject> entry = objectIdsToCheckIter.next();
        byte[] v = entry.getKey();
        long objectId = this.codecRegistry.asObject(v, Long.class);
        Assertions.assertTrue(objectId % 2 == 0);
        Assertions.assertTrue(objectId >= 26);
        Assertions.assertTrue(objectId < (nativeLibraryLoaded ? 100 : 50));
        objectIdCnt += 1;
      }
      Assertions.assertEquals(nativeLibraryLoaded ? 37 : 12, objectIdCnt);
    }
  }

  /**
    Testing generateDiffReport function by providing PersistentMap containing
    objectId Map of diff keys to be checked with their corresponding key names.
   */
  @Test
  public void testGenerateDiffReport() throws IOException {
    // Mocking RocksDbPersistentMap constructor to use stubbed
    // implementation instead.
    try (MockedConstruction<RocksDbPersistentMap> mockedRocksDbPersistentMap =
            Mockito.mockConstruction(RocksDbPersistentMap.class,
                (mock, context) -> {
                  PersistentMap obj =
                      new SnapshotTestUtils.StubbedPersistentMap<>();
                  when(mock.iterator()).thenReturn(obj.iterator());
                  when(mock.iterator(Mockito.any(Optional.class),
                      Mockito.any(Optional.class)))
                      .thenAnswer(i -> obj.iterator(i.getArgument(0),
                          i.getArgument(1)));
                  when(mock.get(Matchers.any()))
                      .thenAnswer(i -> obj.get(i.getArgument(0)));
                  Mockito.doAnswer((Answer<Void>) i -> {
                    obj.put(i.getArgument(0), i.getArgument(1));
                    return null;
                  }).when(mock).put(Matchers.any(), Matchers.any());
                });
        MockedConstruction<RocksDbPersistentList> mockedPersistentList =
            Mockito.mockConstruction(
                RocksDbPersistentList.class, (mock, context) -> {
                  PersistentList obj =
                      new SnapshotTestUtils.ArrayPersistentList<>();
                  Mockito.when(mock.add(Matchers.any()))
                      .thenAnswer(i -> obj.add(i.getArgument(0)));
                  Mockito.when(mock.get(Matchers.anyInt()))
                      .thenAnswer(i -> obj.get(i.getArgument(0)));
                  Mockito.when(mock.addAll(Matchers.any(PersistentList.class)))
                      .thenAnswer(i -> obj.addAll(i.getArgument(0)));
                  Mockito.when(mock.iterator())
                      .thenAnswer(i -> obj.iterator());
                })) {
      PersistentMap<byte[], byte[]> oldObjectIdKeyMap =
          new SnapshotTestUtils.StubbedPersistentMap<>();
      PersistentMap<byte[], byte[]> newObjectIdKeyMap =
          new SnapshotTestUtils.StubbedPersistentMap<>();
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
      UUID fromSnapId = UUID.randomUUID();
      UUID toSnapId = UUID.randomUUID();

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

      SnapshotDiffManager snapshotDiffManager =
          getMockedSnapshotDiffManager(10);

      setupMocksForRunningASnapDiff(volumeName, bucketName);
      setUpSnapshots(volumeName, bucketName,
          fromSnapName, toSnapName, fromSnapId, toSnapId);
      String jobKey = fromSnapId + DELIMITER + toSnapId;

      SnapshotDiffJob snapshotDiffJob = new SnapshotDiffJob(0, "jobId",
          JobStatus.IN_PROGRESS, volumeName,
          bucketName, fromSnapName, toSnapName,
          true, diffMap.size());

      snapshotDiffManager.getSnapDiffJobTable().put(jobKey, snapshotDiffJob);

      snapshotDiffManager.generateDiffReport("jobId", fromSnapTable,
          toSnapTable, objectIdToDiffObject, oldObjectIdKeyMap,
          newObjectIdKeyMap, volumeName, bucketName, fromSnapName, toSnapName,
          false, null, null);

      snapshotDiffJob.setStatus(JobStatus.DONE);
      snapshotDiffManager.getSnapDiffJobTable().put(jobKey, snapshotDiffJob);

      SnapshotDiffReportOzone snapshotDiffReportOzone =
          snapshotDiffManager.createPageResponse(snapshotDiffJob, volumeName,
              bucketName, fromSnapName, toSnapName,
              0, Integer.MAX_VALUE);
      Set<SnapshotDiffReport.DiffType> expectedOrder = new LinkedHashSet<>();
      expectedOrder.add(SnapshotDiffReport.DiffType.DELETE);
      expectedOrder.add(SnapshotDiffReport.DiffType.RENAME);
      expectedOrder.add(SnapshotDiffReport.DiffType.CREATE);
      expectedOrder.add(SnapshotDiffReport.DiffType.MODIFY);

      Set<SnapshotDiffReport.DiffType> actualOrder = new LinkedHashSet<>();
      for (SnapshotDiffReport.DiffReportEntry entry :
          snapshotDiffReportOzone.getDiffList()) {
        actualOrder.add(entry.getType());

        long objectId = Long.parseLong(
            DFSUtilClient.bytes2String(entry.getSourcePath()).substring(4));
        Assertions.assertEquals(diffMap.get(objectId), entry.getType());
      }
      Assertions.assertEquals(expectedOrder, actualOrder);
    }
  }

  private SnapshotDiffReport.DiffReportEntry getTestDiffEntry(String jobId,
          int idx) throws IOException {
    return new SnapshotDiffReport.DiffReportEntry(
        SnapshotDiffReport.DiffType.values()[idx %
            SnapshotDiffReport.DiffType.values().length],
        codecRegistry.asRawData(jobId + DELIMITER + idx));
  }

  /**
   Testing generateDiffReport function by providing PersistentMap containing
   objectId Map of diff keys to be checked with their corresponding key names.
   */
  @ParameterizedTest
  @CsvSource({"0,10,1000", "1,10,8", "1000,1000,10", "-1,1000,10000",
      "1,0,1000", "1,-1,1000"})
  public void testCreatePageResponse(int startIdx, int pageSize,
        int totalNumberOfRecords) throws IOException {
    // Mocking RocksDbPersistentMap constructor to use stubbed
    // implementation instead.
    Map<ColumnFamilyHandle, RocksDbPersistentMap>
        cfHandleRocksDbPersistentMap = new HashMap<>();
    try (MockedConstruction<RocksDbPersistentMap> mockedRocksDbPersistentMap =
            Mockito.mockConstruction(RocksDbPersistentMap.class,
                (mock, context) -> {
                  ColumnFamilyHandle cf =
                      (ColumnFamilyHandle) context.arguments().stream()
                          .filter(arg -> arg instanceof ColumnFamilyHandle)
                          .findFirst().get();
                  cfHandleRocksDbPersistentMap.put(cf, mock);
                  PersistentMap obj =
                      new SnapshotTestUtils.StubbedPersistentMap<>();
                  when(mock.iterator()).thenReturn(obj.iterator());
                  when(mock.iterator(any(Optional.class),
                      any(Optional.class))).thenAnswer(i ->
                      obj.iterator(i.getArgument(0), i.getArgument(1)));
                  Mockito.when(mock.get(Matchers.any()))
                      .thenAnswer(i -> obj.get(i.getArgument(0)));
                  Mockito.doAnswer((Answer<Void>) i -> {
                    obj.put(i.getArgument(0), i.getArgument(1));
                    return null;
                  }).when(mock).put(Matchers.any(), Matchers.any());
              })) {
      String testJobId = "jobId";
      String testJobId2 = "jobId2";
      SnapshotDiffManager snapshotDiffManager =
          getMockedSnapshotDiffManager(10);
      IntStream.range(0, totalNumberOfRecords).boxed().forEach(idx -> {
        try {
          cfHandleRocksDbPersistentMap.get(snapdiffReportCFH)
              .put(codecRegistry.asRawData(SnapshotDiffManager
                      .getReportKeyForIndex(testJobId, idx)),
                  codecRegistry.asRawData(getTestDiffEntry(testJobId, idx)));
          cfHandleRocksDbPersistentMap.get(snapdiffReportCFH)
              .put(codecRegistry.asRawData(SnapshotDiffManager
                      .getReportKeyForIndex(testJobId2, idx)),
                  codecRegistry.asRawData(getTestDiffEntry(testJobId2, idx)));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      SnapshotDiffJob snapshotDiffJob = new SnapshotDiffJob(0, testJobId,
          SnapshotDiffResponse.JobStatus.DONE, "vol", "buck", "fs", "ts",
          true, totalNumberOfRecords);
      SnapshotDiffJob snapshotDiffJob2 = new SnapshotDiffJob(0, testJobId2,
          SnapshotDiffResponse.JobStatus.DONE, "vol", "buck", "fs", "ts",
          true, totalNumberOfRecords);
      cfHandleRocksDbPersistentMap.get(snapdiffJobCFH)
          .put(codecRegistry.asRawData(testJobId), snapshotDiffJob);
      cfHandleRocksDbPersistentMap.get(snapdiffJobCFH)
          .put(codecRegistry.asRawData(testJobId), snapshotDiffJob2);
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
      Assertions.assertEquals(snapshotDiffReportOzone.getDiffList().size(),
          expectedTotalNumberOfRecords);

      int idx = startIdx;
      for (SnapshotDiffReport.DiffReportEntry entry :
          snapshotDiffReportOzone.getDiffList()) {
        Assertions.assertEquals(getTestDiffEntry(testJobId, idx), entry);
        idx++;
      }
    }
  }

  /**
   * Once a job is cancelled, it stays in the table until
   * SnapshotDiffCleanupService removes it.
   *
   * Job response until that happens, is CANCELLED.
   */
  @Test
  public void testGetSnapshotDiffReportForCancelledJob()
      throws IOException {
    SnapshotDiffManager snapshotDiffManager =
        getMockedSnapshotDiffManager(10);

    String volumeName = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket-" + RandomStringUtils.randomNumeric(5);

    String fromSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);
    String toSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);

    UUID fromSnapshotUUID = UUID.randomUUID();
    UUID toSnapshotUUID = UUID.randomUUID();

    setupMocksForRunningASnapDiff(volumeName, bucketName);

    setUpSnapshots(volumeName, bucketName, fromSnapshotName,
        toSnapshotName, fromSnapshotUUID, toSnapshotUUID);

    PersistentMap<String, SnapshotDiffJob> snapDiffJobTable =
        snapshotDiffManager.getSnapDiffJobTable();
    String diffJobKey = fromSnapshotUUID + DELIMITER + toSnapshotUUID;

    SnapshotDiffJob diffJob = snapDiffJobTable.get(diffJobKey);
    Assertions.assertNull(diffJob);

    // Submit a new job.
    SnapshotDiffResponse snapshotDiffResponse = snapshotDiffManager
        .getSnapshotDiffReport(volumeName, bucketName,
            fromSnapshotName, toSnapshotName,
            0, 0, false);

    Assertions.assertEquals(JobStatus.IN_PROGRESS,
        snapshotDiffResponse.getJobStatus());

    // Cancel the job.
    snapshotDiffManager.cancelSnapshotDiff(volumeName, bucketName,
        fromSnapshotName, toSnapshotName);

    // Job status should be cancelled until the cleanup
    // service removes the job from the table.
    snapshotDiffResponse = snapshotDiffManager
        .getSnapshotDiffReport(volumeName, bucketName,
            fromSnapshotName, toSnapshotName,
            0, 0, false);

    Assertions.assertEquals(JobStatus.CANCELLED,
        snapshotDiffResponse.getJobStatus());

    // Check snapDiffJobTable.
    diffJob = snapDiffJobTable.get(diffJobKey);
    Assertions.assertNotNull(diffJob);
    Assertions.assertEquals(JobStatus.CANCELLED,
        diffJob.getStatus());

    // Response should still be cancelled.
    snapshotDiffResponse = snapshotDiffManager
        .getSnapshotDiffReport(volumeName, bucketName,
            fromSnapshotName, toSnapshotName,
            0, 0, false);

    Assertions.assertEquals(JobStatus.CANCELLED,
        snapshotDiffResponse.getJobStatus());

    // Check snapDiffJobTable.
    diffJob = snapDiffJobTable.get(diffJobKey);
    Assertions.assertNotNull(diffJob);
    Assertions.assertEquals(JobStatus.CANCELLED,
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
    SnapshotDiffManager snapshotDiffManager =
        getMockedSnapshotDiffManager(10);

    String volumeName = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket-" + RandomStringUtils.randomNumeric(5);

    String fromSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);
    String toSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);

    UUID fromSnapshotUUID = UUID.randomUUID();
    UUID toSnapshotUUID = UUID.randomUUID();

    setupMocksForRunningASnapDiff(volumeName, bucketName);

    setUpSnapshots(volumeName, bucketName, fromSnapshotName,
        toSnapshotName, fromSnapshotUUID, toSnapshotUUID);

    PersistentMap<String, SnapshotDiffJob> snapDiffJobTable =
        snapshotDiffManager.getSnapDiffJobTable();
    String diffJobKey = fromSnapshotUUID + DELIMITER + toSnapshotUUID;

    String jobId = UUID.randomUUID().toString();
    SnapshotDiffJob snapshotDiffJob = new SnapshotDiffJob(0L,
        jobId, jobStatus, volumeName, bucketName,
        fromSnapshotName, toSnapshotName, true, 10);

    snapDiffJobTable.put(diffJobKey, snapshotDiffJob);

    SnapshotDiffResponse snapshotDiffResponse = snapshotDiffManager
        .cancelSnapshotDiff(volumeName, bucketName,
            fromSnapshotName, toSnapshotName);

    Assertions.assertEquals(cancelResult,
        snapshotDiffResponse.getJobCancelResult());

    if (jobIsCancelled) {
      Assertions.assertEquals(JobStatus.CANCELLED,
          snapshotDiffResponse.getJobStatus());
    }
  }

  @Test
  public void testCancelNewSnapshotDiff()
      throws IOException {
    SnapshotDiffManager snapshotDiffManager =
        getMockedSnapshotDiffManager(10);

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
    Assertions.assertEquals(JobCancelResult.NEW_JOB,
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
    SnapshotDiffManager snapshotDiffManager =
        getMockedSnapshotDiffManager(10);

    String volumeName = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket-" + RandomStringUtils.randomNumeric(5);

    String fromSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);
    String toSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);

    UUID fromSnapshotUUID = UUID.randomUUID();
    UUID toSnapshotUUID = UUID.randomUUID();

    setupMocksForRunningASnapDiff(volumeName, bucketName);

    setUpSnapshots(volumeName, bucketName, fromSnapshotName,
        toSnapshotName, fromSnapshotUUID, toSnapshotUUID);

    PersistentMap<String, SnapshotDiffJob> snapDiffJobTable =
        snapshotDiffManager.getSnapDiffJobTable();
    String diffJobKey = fromSnapshotUUID + DELIMITER + toSnapshotUUID;

    SnapshotDiffJob diffJob = snapDiffJobTable.get(diffJobKey);
    Assertions.assertNull(diffJob);

    // There are no jobs in the table, therefore
    // the response list should be empty.
    List<SnapshotDiffJob> jobList = snapshotDiffManager
        .getSnapshotDiffJobList(volumeName, bucketName, jobStatus, listAll);
    Assertions.assertTrue(jobList.isEmpty());

    // SnapshotDiffReport
    SnapshotDiffResponse snapshotDiffResponse = snapshotDiffManager
        .getSnapshotDiffReport(volumeName, bucketName,
            fromSnapshotName, toSnapshotName,
            0, 0, false);

    Assertions.assertEquals(SnapshotDiffResponse.JobStatus.IN_PROGRESS,
        snapshotDiffResponse.getJobStatus());

    diffJob = snapDiffJobTable.get(diffJobKey);
    Assertions.assertNotNull(diffJob);
    Assertions.assertEquals(SnapshotDiffResponse.JobStatus.IN_PROGRESS,
        diffJob.getStatus());

    jobList = snapshotDiffManager
        .getSnapshotDiffJobList(volumeName, bucketName, jobStatus, listAll);

    // When listAll is true, jobStatus is ignored.
    // If the job is IN_PROGRESS or listAll is used,
    // there should be a response.
    // Otherwise, response list should be empty.
    if (containsJob) {
      Assertions.assertTrue(jobList.contains(diffJob));
    } else {
      Assertions.assertTrue(jobList.isEmpty());
    }
  }

  @Test
  public void testListSnapDiffWithInvalidStatus() throws IOException {
    SnapshotDiffManager snapshotDiffManager =
        getMockedSnapshotDiffManager(10);

    String volumeName = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket-" + RandomStringUtils.randomNumeric(5);

    String fromSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);
    String toSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);

    UUID fromSnapshotUUID = UUID.randomUUID();
    UUID toSnapshotUUID = UUID.randomUUID();

    setupMocksForRunningASnapDiff(volumeName, bucketName);

    setUpSnapshots(volumeName, bucketName, fromSnapshotName,
        toSnapshotName, fromSnapshotUUID, toSnapshotUUID);

    // SnapshotDiffReport
    snapshotDiffManager.getSnapshotDiffReport(volumeName, bucketName,
        fromSnapshotName, toSnapshotName,
        0, 0, false);

    // Invalid status, without listAll true, results in an exception.
    Assertions.assertThrows(IOException.class, () -> snapshotDiffManager
        .getSnapshotDiffJobList(volumeName, bucketName, "invalid", false));
  }

  private void setUpSnapshots(String volumeName, String bucketName,
                              String fromSnapshotName, String toSnapshotName,
                              UUID fromSnapshotUUID, UUID toSnapshotUUID)
      throws IOException {
    try (MockedStatic<SnapshotUtils> mockedSnapUtils =
             Mockito.mockStatic(SnapshotUtils.class)) {
      // Create 1st snapshot.
      SnapshotInfo fromSnapshotInfo =
          getSnapshotInfoInstance(volumeName, bucketName,
              fromSnapshotName, fromSnapshotUUID);
      mockedSnapUtils.when(() -> SnapshotUtils
              .getSnapshotInfo(ozoneManager, volumeName,
                  bucketName, fromSnapshotName))
          .thenReturn(fromSnapshotInfo);

      String fromSnapKey = SnapshotInfo
          .getTableKey(fromSnapshotInfo.getVolumeName(),
              fromSnapshotInfo.getBucketName(), fromSnapshotInfo.getName());

      Mockito.when(ozoneManager.getMetadataManager()
              .getSnapshotInfoTable().get(fromSnapKey))
          .thenReturn(fromSnapshotInfo);

      mockedSnapUtils.when(() -> SnapshotUtils
              .getSnapshotInfo(ozoneManager, fromSnapKey))
          .thenReturn(fromSnapshotInfo);

      OmSnapshot omSnapshotFrom = getMockedOmSnapshot(fromSnapKey);
      snapshotCache.put(fromSnapKey, omSnapshotFrom);

      // Create 2nd snapshot.
      SnapshotInfo toSnapshotInfo =
          getSnapshotInfoInstance(volumeName, bucketName,
              toSnapshotName, toSnapshotUUID);

      mockedSnapUtils.when(
              () -> SnapshotUtils.getSnapshotInfo(ozoneManager,
                  volumeName, bucketName, toSnapshotName))
          .thenReturn(toSnapshotInfo);

      String toSnapKey = SnapshotInfo
          .getTableKey(toSnapshotInfo.getVolumeName(),
              toSnapshotInfo.getBucketName(), toSnapshotInfo.getName());

      Mockito.when(ozoneManager.getMetadataManager()
          .getSnapshotInfoTable().get(toSnapKey)).thenReturn(toSnapshotInfo);

      mockedSnapUtils.when(() -> SnapshotUtils
              .getSnapshotInfo(ozoneManager, toSnapKey))
          .thenReturn(toSnapshotInfo);

      OmSnapshot omSnapshotTo = getMockedOmSnapshot(toSnapKey);
      snapshotCache.put(toSnapKey, omSnapshotTo);
    }
  }

  private SnapshotInfo getSnapshotInfoInstance(
      String volumeName, String bucketName,
      String snapshotName, UUID snapshotUUID) {
    SnapshotInfo snapshotInfo = SnapshotInfo
        .newInstance(volumeName, bucketName,
            snapshotName, snapshotUUID,
            System.currentTimeMillis());
    snapshotInfo.setSnapshotStatus(SnapshotInfo
        .SnapshotStatus.SNAPSHOT_ACTIVE);
    return snapshotInfo;
  }

  private void setupMocksForRunningASnapDiff(
      String volumeName, String bucketName)
      throws IOException {
    Mockito.when(ozoneManager.getMetadataManager().getSnapshotInfoTable())
        .thenReturn(Mockito.mock(Table.class));
    Mockito.when(ozoneManager.getMetadataManager().getBucketTable())
        .thenReturn(Mockito.mock(Table.class));

    Map<BucketLayout, String> keyTableMap = new HashMap<>();
    keyTableMap.put(BucketLayout.FILE_SYSTEM_OPTIMIZED,
        OmMetadataManagerImpl.FILE_TABLE);
    keyTableMap.put(BucketLayout.OBJECT_STORE,
        OmMetadataManagerImpl.KEY_TABLE);
    keyTableMap.put(BucketLayout.LEGACY,
        OmMetadataManagerImpl.KEY_TABLE);

    for (Map.Entry<BucketLayout, String> entry : keyTableMap.entrySet()) {
      Mockito.when(ozoneManager.getMetadataManager()
              .getKeyTable(entry.getKey()))
          .thenReturn(Mockito.mock(Table.class));
      Mockito.when(ozoneManager.getMetadataManager()
              .getKeyTable(entry.getKey()).getName())
          .thenReturn(entry.getValue());
    }

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .setOwner(ugi.getShortUserName())
        .build();

    String bucketKey = ozoneManager.getMetadataManager()
        .getBucketKey(volumeName, bucketName);
    Mockito.when(ozoneManager.getMetadataManager().getBucketTable()
        .get(bucketKey)).thenReturn(bucketInfo);
  }
}
