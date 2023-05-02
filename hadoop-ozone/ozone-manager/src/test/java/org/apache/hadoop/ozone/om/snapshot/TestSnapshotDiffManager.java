package org.apache.hadoop.ozone.om.snapshot;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.IntegerCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.codec.OmDBDiffReportEntryCodec;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.WithObjectID;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
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
import org.junit.jupiter.params.provider.CsvSource;
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
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;


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

  private Map<String, OmSnapshot> snapshotMap;

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
    codecRegistry = new CodecRegistry();

    // Integers are used for indexing persistent list.
    codecRegistry.addCodec(Integer.class, new IntegerCodec());
    // DiffReportEntry codec for Diff Report.
    codecRegistry.addCodec(SnapshotDiffReportOzone.DiffReportEntry.class,
        new OmDBDiffReportEntryCodec());
    codecRegistry.addCodec(SnapshotDiffJob.class,
        new SnapshotDiffJob.SnapshotDiffJobCodec());
  }

  private DBStore getMockedDBStore(String dbStorePath) {
    DBStore dbStore = Mockito.mock(DBStore.class);
    Mockito.when(dbStore.getDbLocation()).thenReturn(new File(dbStorePath));
    return dbStore;
  }

  private OmSnapshot getMockedOmSnapshot(String snapshot) {
    OmSnapshot omSnapshot = Mockito.mock(OmSnapshot.class);
    OMMetadataManager metadataManager = Mockito.mock(OMMetadataManager.class);
    DBStore dbStore = getMockedDBStore(snapshot);
    Mockito.when(omSnapshot.getName()).thenReturn(snapshot);
    Mockito.when(omSnapshot.getMetadataManager()).thenReturn(metadataManager);
    Mockito.when(metadataManager.getStore()).thenReturn(dbStore);
    return omSnapshot;
  }

  private SnapshotDiffManager getMockedSnapshotDiffManager(int cacheSize) {

    Mockito.when(snapdiffDB.get()).thenReturn(rocksDB);
    Mockito.when(rocksDB.newIterator(snapdiffJobCFH))
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
    SnapshotDiffManager snapshotDiffManager = Mockito.spy(
        new SnapshotDiffManager(snapdiffDB, differ, ozoneManager, snapshotCache,
            snapdiffJobCFH, snapdiffReportCFH, columnFamilyOptions,
            codecRegistry));
    return snapshotDiffManager;
  }

  private SnapshotInfo getMockedSnapshotInfo(String snapshot) {
    SnapshotInfo snapshotInfo = Mockito.mock(SnapshotInfo.class);
    Mockito.when(snapshotInfo.getSnapshotID()).thenReturn(snapshot);
    return snapshotInfo;
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 5, 10, 100, 1000, 10000})
  public void testGetDeltaFilesWithDag(int numberOfFiles)
      throws ExecutionException, RocksDBException, IOException {

    SnapshotDiffManager snapshotDiffManager = getMockedSnapshotDiffManager(10);
    String snap1 = "snap1";
    String snap2 = "snap2";

    Set<String> randomStrings = IntStream.range(0, numberOfFiles)
        .mapToObj(i -> RandomStringUtils.randomAlphabetic(10))
        .collect(Collectors.toSet());
    Mockito.when(differ.getSSTDiffListWithFullPath(Mockito.any(),
        Mockito.any(), Mockito.anyString())).thenReturn(Lists.newArrayList(randomStrings));
    SnapshotInfo fromSnapshotInfo = getMockedSnapshotInfo(snap1);
    SnapshotInfo toSnapshotInfo = getMockedSnapshotInfo(snap1);
    Mockito.when(jobTableIterator.isValid()).thenReturn(false);
    Set<String> deltaFiles = snapshotDiffManager.getDeltaFiles(
        snapshotCache.get(snap1), snapshotCache.get(snap2),
        Arrays.asList("cf1", "cf2"), fromSnapshotInfo, toSnapshotInfo, false,
        Collections.EMPTY_MAP,
        Files.createTempDirectory("snapdiff_dir").toString());
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
      String snap1 = "snap1";
      String snap2 = "snap2";
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
          snapshotCache.get(snap1), snapshotCache.get(snap2),
          Arrays.asList("cf1", "cf2"), fromSnapshotInfo, toSnapshotInfo, false,
          Collections.EMPTY_MAP, Files.createTempDirectory("snapdiff_dir")
              .toAbsolutePath().toString());
      Assertions.assertEquals(deltaStrings, deltaFiles);
    }
  }

  private Table<String, ? extends WithObjectID> getMockedTable(
      Map<String, WithObjectID> map, String tableName)
      throws IOException {
    Table<String, ? extends WithObjectID> mocked = Mockito.mock(Table.class);
    Mockito.when(mocked.get(Matchers.any()))
        .thenAnswer(invocation -> map.get(invocation.getArgument(0)));
    Mockito.when(mocked.getName()).thenReturn(tableName);
    return mocked;
  }

  private WithObjectID getObjectID(int objectId, int updateId,
                                   String snapshotTableName) {
    String name = "key" + objectId;
    if (snapshotTableName.equals(OmMetadataManagerImpl.DIRECTORY_TABLE)) {
      return OmDirectoryInfo.newBuilder()
          .setObjectID(objectId).setName(name).build();
    }
    return new OmKeyInfo.Builder().setObjectID(objectId)
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
  @ParameterizedTest
  @CsvSource({"false," + OmMetadataManagerImpl.DIRECTORY_TABLE,
      "true," + OmMetadataManagerImpl.DIRECTORY_TABLE,
      "false," + OmMetadataManagerImpl.FILE_TABLE,
      "true," + OmMetadataManagerImpl.FILE_TABLE,
      "false," + OmMetadataManagerImpl.KEY_TABLE,
      "true," + OmMetadataManagerImpl.KEY_TABLE})
  public void testObjectIdMapWithTombstoneEntries(boolean nativeLibraryLoaded,
                                                  String snapshotTableName)
      throws NativeLibraryNotLoadedException, IOException {
    Set<String> keysWithTombstones = IntStream.range(0, 100)
        .boxed().map(i -> "key" + i).collect(Collectors.toSet());
    Set<String> keys = IntStream.range(0, 50).boxed()
        .map(i -> "key" + i).collect(Collectors.toSet());
    try (MockedConstruction<ManagedSstFileReader> mocked =
             Mockito.mockConstruction(ManagedSstFileReader.class,
                 (mock, context) -> {
                   Mockito.when(mock.getKeyStreamWithTombstone(Matchers.any()))
                       .thenReturn(keysWithTombstones.stream());
                   Mockito.when(mock.getKeyStream())
                       .thenReturn(keys.stream());
                 })) {
      Map<String, WithObjectID> toSnapshotTableMap =
          IntStream.concat(IntStream.range(0, 25), IntStream.range(50, 100))
              .boxed().collect(Collectors.toMap(i -> "key" + i,
                  i -> getObjectID(i, i, snapshotTableName)));
      Table<String, ? extends WithObjectID> toSnapshotTable =
          getMockedTable(toSnapshotTableMap, snapshotTableName);
      Map<String, WithObjectID> fromSnapshotTableMap =
          IntStream.range(0, 50)
              .boxed().collect(Collectors.toMap(i -> "key" + i,
                  i -> getObjectID(i, i, snapshotTableName)));
      Table<String, ? extends WithObjectID> fromSnapshotTable =
          getMockedTable(fromSnapshotTableMap, snapshotTableName);
      SnapshotDiffManager snapshotDiffManager =
          getMockedSnapshotDiffManager(10);
      Mockito.doAnswer((Answer<Boolean>) invocationOnMock ->
              Integer.parseInt(invocationOnMock.getArgument(0, String.class)
                  .substring(3)) % 2 == 0).when(snapshotDiffManager)
          .isKeyInBucket(Matchers.anyString(), Matchers.any(), Matchers.any());
      PersistentMap<byte[], byte[]> oldObjectIdKeyMap =
          new SnapshotTestUtils.HashPersistentMap<>();
      PersistentMap<byte[], byte[]> newObjectIdKeyMap =
          new SnapshotTestUtils.HashPersistentMap<>();
      PersistentSet<byte[]> objectIdsToCheck =
          new SnapshotTestUtils.HashPersistentSet<>();
      snapshotDiffManager.addToObjectIdMap(toSnapshotTable,
          fromSnapshotTable,
          Pair.of(nativeLibraryLoaded, Mockito.mock(Set.class)),
          oldObjectIdKeyMap, newObjectIdKeyMap, objectIdsToCheck,
          Maps.newHashMap());

      Iterator<Map.Entry<byte[], byte[]>> oldObjectIdIter =
          oldObjectIdKeyMap.iterator();
      int oldObjectIdCnt = 0;
      while (oldObjectIdIter.hasNext()) {
        Map.Entry<byte[], byte[]> v = oldObjectIdIter.next();
        long objectId = this.codecRegistry.asObject(v.getKey(), Long.class);
        Assertions.assertTrue(objectId % 2 == 0);
        Assertions.assertTrue(objectId >= 50);
        Assertions.assertTrue(objectId < 100);
        oldObjectIdCnt += 1;
      }
      Assertions.assertEquals(nativeLibraryLoaded ? 25 : 0, oldObjectIdCnt);
      Iterator<Map.Entry<byte[], byte[]>> newObjectIdIter =
          newObjectIdKeyMap.iterator();
      int newObjectIdCnt = 0;
      while (newObjectIdIter.hasNext()) {
        Map.Entry<byte[], byte[]> v = newObjectIdIter.next();
        long objectId = this.codecRegistry.asObject(v.getKey(), Long.class);
        Assertions.assertTrue(objectId % 2 == 0);
        Assertions.assertTrue(objectId >= 26);
        Assertions.assertTrue(objectId < 50);
        newObjectIdCnt += 1;
      }
      Assertions.assertEquals(12, newObjectIdCnt);

      Iterator<byte[]> objectIdsToCheckIter = objectIdsToCheck.iterator();
      int objectIdCnt = 0;
      while (objectIdsToCheckIter.hasNext()) {
        byte[] v = objectIdsToCheckIter.next();
        long objectId = this.codecRegistry.asObject(v, Long.class);
        Assertions.assertTrue(objectId % 2 == 0);
        Assertions.assertTrue(objectId >= 26);
        Assertions.assertTrue(objectId < (nativeLibraryLoaded ? 100 : 50));
        objectIdCnt += 1;
      }
      Assertions.assertEquals(nativeLibraryLoaded ? 37 : 12, objectIdCnt);
    }
  }

  @Test
  public void testGenerateDiffReport() throws IOException {
    try (
        MockedConstruction<RocksDbPersistentMap> mockedRocksDbPersistentMap =
            Mockito.mockConstruction(RocksDbPersistentMap.class,
                (mock, context) -> {
                  PersistentMap obj =
                      new SnapshotTestUtils.HashPersistentMap<>();
                  Mockito.when(mock.iterator()).thenReturn(obj.iterator());
                  Mockito.when(mock.get(Matchers.any()))
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
          new SnapshotTestUtils.HashPersistentMap<>();
      PersistentMap<byte[], byte[]> newObjectIdKeyMap =
          new SnapshotTestUtils.HashPersistentMap<>();
      PersistentSet<byte[]> objectIdsToCheck =
          new SnapshotTestUtils.HashPersistentSet<>();
      Map<Long, SnapshotDiffReport.DiffType> diffMap = new HashMap<>();
      LongStream.range(0, 100).forEach(objectId -> {
        try {
          byte[] objectIdVal = codecRegistry.asRawData(objectId);
          byte[] key = codecRegistry.asRawData("key" + objectId);
          if (objectId >= 0 && objectId <= 25 ||
              objectId >= 50 && objectId <= 100) {
            oldObjectIdKeyMap.put(objectIdVal, key);
          }
          if (objectId >= 0 && objectId <= 25 && objectId % 4 == 0 ||
              objectId > 25 && objectId < 50) {
            newObjectIdKeyMap.put(objectIdVal, key);
          }
          if (objectId >= 0 && objectId <= 25 && objectId % 4 == 1) {
            byte[] keyVal = codecRegistry.asRawData("renamed-key" + objectId);
            newObjectIdKeyMap.put(objectIdVal, keyVal);
            diffMap.put(objectId, SnapshotDiffReport.DiffType.RENAME);
          }
          objectIdsToCheck.add(objectIdVal);
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
      SnapshotDiffManager snapshotDiffManager =
          getMockedSnapshotDiffManager(10);
      long val = snapshotDiffManager.generateDiffReport("jobId",
          objectIdsToCheck, oldObjectIdKeyMap, newObjectIdKeyMap);
      SnapshotDiffReportOzone snapshotDiffReportOzone =
          snapshotDiffManager.createPageResponse("jobId", "vol",
          "buck", getMockedOmSnapshot("fs"), getMockedOmSnapshot("ts"),
          0, Integer.MAX_VALUE);
      List<SnapshotDiffReport.DiffType> expectedOrder = Arrays.asList(
          SnapshotDiffReport.DiffType.DELETE,
          SnapshotDiffReport.DiffType.RENAME,
          SnapshotDiffReport.DiffType.CREATE,
          SnapshotDiffReport.DiffType.MODIFY);

      List<SnapshotDiffReport.DiffType> actualOrder = Lists.newArrayList();
      for (SnapshotDiffReport.DiffReportEntry entry :
          snapshotDiffReportOzone.getDiffList()) {
        if (actualOrder.size() == 0 ||
            actualOrder.get(actualOrder.size() - 1) != entry.getType()) {
          actualOrder.add(entry.getType());
        }
        long objectId = Long.parseLong(
            DFSUtilClient.bytes2String(entry.getSourcePath()).substring(3));
        Assertions.assertEquals(diffMap.get(objectId), entry.getType());
      }
      Assertions.assertEquals(expectedOrder, actualOrder);
    }
  }

}
