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

package org.apache.hadoop.ozone.om.snapshot.defrag;

import static org.apache.hadoop.ozone.om.OmSnapshotManager.COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELEGATION_TOKEN_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.VOLUME_TABLE;
import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.SNAPSHOT_DB_CONTENT_LOCK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.CodecBufferCodec;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.InMemoryTestTable;
import org.apache.hadoop.hdds.utils.db.RDBSstFileWriter;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.SstFileSetReader;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.StringInMemoryTestTable;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotInternalMetrics;
import org.apache.hadoop.ozone.om.OmSnapshotLocalData;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager.WritableOmSnapshotLocalDataProvider;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.om.snapshot.diff.delta.CompositeDeltaDiffComputer;
import org.apache.hadoop.ozone.om.snapshot.diff.delta.DeltaFileComputer;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.hadoop.ozone.util.ClosableIterator;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for SnapshotDefragService.
 */
public class TestSnapshotDefragService {

  @Mock
  private OzoneManager ozoneManager;

  @Mock
  private OmSnapshotManager omSnapshotManager;

  @Mock
  private OmSnapshotLocalDataManager snapshotLocalDataManager;

  @Mock
  private OmMetadataManagerImpl metadataManager;

  @Mock
  private IOzoneManagerLock omLock;

  @Mock
  private OMLayoutVersionManager versionManager;

  private DeltaFileComputer deltaFileComputer;

  @Mock
  private OmSnapshotInternalMetrics snapshotMetrics;

  @Mock
  private OMPerformanceMetrics perfMetrics;

  @TempDir
  private Path tempDir;
  private OzoneConfiguration configuration;
  private SnapshotDefragService defragService;
  private AutoCloseable mocks;
  private Map<String, CodecBuffer> dummyTableValues;
  private Set<CodecBuffer> closeSet = new HashSet<>();

  @BeforeEach
  public void setup() throws IOException {
    mocks = MockitoAnnotations.openMocks(this);
    configuration = new OzoneConfiguration();

    // Setup basic mocks
    when(ozoneManager.getOmSnapshotManager()).thenReturn(omSnapshotManager);
    when(ozoneManager.getMetadataManager()).thenReturn(metadataManager);
    when(ozoneManager.getThreadNamePrefix()).thenReturn("TestOM");
    when(ozoneManager.isRunning()).thenReturn(true);
    when(ozoneManager.getVersionManager()).thenReturn(versionManager);
    when(ozoneManager.getOmRatisServer()).thenReturn(mock(OzoneManagerRatisServer.class));
    when(ozoneManager.getOmSnapshotIntMetrics()).thenReturn(snapshotMetrics);
    when(ozoneManager.getPerfMetrics()).thenReturn(perfMetrics);

    when(omSnapshotManager.getSnapshotLocalDataManager()).thenReturn(snapshotLocalDataManager);
    when(metadataManager.getLock()).thenReturn(omLock);
    when(metadataManager.getSnapshotParentDir()).thenReturn(tempDir);
    when(versionManager.isAllowed(any(LayoutFeature.class))).thenReturn(true);
    try (MockedConstruction<CompositeDeltaDiffComputer> compositeDeltaDiffComputer =
             mockConstruction(CompositeDeltaDiffComputer.class)) {
      // Initialize service
      defragService = new SnapshotDefragService(
          10000, // interval
          TimeUnit.MILLISECONDS,
          60000, // timeout
          ozoneManager,
          configuration
      );
      assertEquals(1, compositeDeltaDiffComputer.constructed().size());
      this.deltaFileComputer = compositeDeltaDiffComputer.constructed().get(0);
    }
    this.dummyTableValues = new HashMap<>();
    for (char c = 'a'; c <= 'z'; c++) {
      for (char d = 'a'; d <= 'z'; d++) {
        for (int i = 0; i < 10; i++) {
          String key = String.valueOf(c) + d + i;
          CodecBuffer value = getCodecBuffer(key);
          dummyTableValues.put(key, value);
        }
      }
    }
  }

  private CodecBuffer getCodecBuffer(String value) throws CodecException {
    CodecBuffer buffer = StringCodec.get().toDirectCodecBuffer(value);
    closeSet.add(buffer);
    return buffer;
  }

  private String getFromCodecBuffer(CodecBuffer buffer) {
    return StringCodec.get().fromCodecBuffer(buffer);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (defragService != null) {
      defragService.shutdown();
    }
    if (mocks != null) {
      mocks.close();
    }
    closeSet.forEach(CodecBuffer::close);
  }

  @Test
  public void testServiceStartAndPause() {
    defragService.start();
    assertTrue(defragService.getSnapshotsDefraggedCount().get() >= 0);

    defragService.pause();
    assertFalse(defragService.isRunning());

    defragService.resume();
    assertTrue(defragService.isRunning());
  }

  @Test
  public void testNeedsDefragmentationAlreadyDefragmented() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    SnapshotInfo snapshotInfo = createMockSnapshotInfo(snapshotId, "vol1", "bucket1", "snap1");

    WritableOmSnapshotLocalDataProvider provider = mock(WritableOmSnapshotLocalDataProvider.class);
    OmSnapshotLocalData localData = mock(OmSnapshotLocalData.class);
    OmSnapshotLocalData previousLocalData = mock(OmSnapshotLocalData.class);
    Optional<OmSnapshotLocalData> optionalPreviousLocalData = Optional.of(previousLocalData);
    when(snapshotLocalDataManager.getWritableOmSnapshotLocalData(snapshotInfo)).thenReturn(provider);
    when(provider.needsDefrag()).thenReturn(false);
    when(provider.getSnapshotLocalData()).thenReturn(localData);
    when(provider.getPreviousSnapshotLocalData()).thenReturn(optionalPreviousLocalData);
    when(localData.getVersion()).thenReturn(1);
    when(previousLocalData.getVersion()).thenReturn(0);


    OmSnapshotLocalData.VersionMeta versionInfo = mock(OmSnapshotLocalData.VersionMeta.class);
    when(versionInfo.getPreviousSnapshotVersion()).thenReturn(0);
    Map<Integer, OmSnapshotLocalData.VersionMeta> versionMap = ImmutableMap.of(1, versionInfo);
    when(localData.getVersionSstFileInfos()).thenReturn(versionMap);

    Pair<Boolean, Integer> result = defragService.needsDefragmentation(snapshotInfo);

    assertFalse(result.getLeft());
    assertEquals(1, result.getRight());
    verify(provider).commit();
    verify(provider).close();
  }

  @Test
  public void testNeedsDefragmentationRequiresDefrag() throws IOException {
    UUID snapshotId = UUID.randomUUID();
    SnapshotInfo snapshotInfo = createMockSnapshotInfo(snapshotId, "vol1", "bucket1", "snap1");

    WritableOmSnapshotLocalDataProvider provider = mock(WritableOmSnapshotLocalDataProvider.class);
    OmSnapshotLocalData localData = mock(OmSnapshotLocalData.class);
    AtomicInteger commit = new AtomicInteger(0);
    when(snapshotLocalDataManager.getWritableOmSnapshotLocalData(snapshotInfo)).thenReturn(provider);
    when(provider.getSnapshotLocalData()).thenReturn(localData);
    doAnswer(invocationOnMock -> {
      commit.incrementAndGet();
      return null;
    }).when(provider).commit();
    when(provider.needsDefrag()).thenAnswer(i -> commit.get() == 1);
    int version = ThreadLocalRandom.current().nextInt(100);
    when(localData.getVersion()).thenReturn(version);

    Pair<Boolean, Integer> result = defragService.needsDefragmentation(snapshotInfo);

    assertTrue(result.getLeft());
    assertEquals(version, result.getRight());
    verify(provider).close();
  }

  private Map<String, InMemoryTestTable<String, CodecBuffer>> createTables(String... tableNames) {
    return createTables(dummyTableValues, tableNames);
  }

  private Map<String, InMemoryTestTable<String, CodecBuffer>> createTables(
      Map<String, CodecBuffer> tableValues, String... tableNames) {
    Map<String, InMemoryTestTable<String, CodecBuffer>> tables = new HashMap<>();
    for (String tableName : tableNames) {
      tables.put(tableName, new StringInMemoryTestTable<>(tableValues, tableName));
    }
    return tables;
  }

  @Test
  public void testPerformFullDefragmentation() throws Exception {
    DBStore checkpointDBStore = mock(DBStore.class);
    Map<String, InMemoryTestTable<String, CodecBuffer>> tableMap = createTables("cf1", "cf2", "cf3");
    TablePrefixInfo prefixInfo = new TablePrefixInfo(ImmutableMap.of("cf1", "ab", "cf2", "cd",
        "cf3", "ef"));
    Map<String, Map<String, CodecBuffer>> tablesCompacted = new HashMap<>();
    Set<String> incrementalTables = Stream.of("cf1", "cf2").collect(Collectors.toSet());
    when(checkpointDBStore.getTable(anyString(), eq(StringCodec.get()), eq(CodecBufferCodec.get(true))))
        .thenAnswer(i -> {
          String tableName = i.getArgument(0, String.class);
          return tableMap.getOrDefault(tableName, null);
        });
    doAnswer(i -> {
      String tableName = i.getArgument(0, String.class);
      Map<String, CodecBuffer> table = new HashMap<>(tableMap.get(tableName).getMap());
      tablesCompacted.putIfAbsent(tableName, table);
      return null;

    }).when(checkpointDBStore).compactTable(anyString(), any());

    defragService.performFullDefragmentation(checkpointDBStore, prefixInfo, incrementalTables);
    assertEquals(2, tablesCompacted.size());
    for (Map.Entry<String, Map<String, CodecBuffer>> compactedTable : tablesCompacted.entrySet()) {
      String prefix = prefixInfo.getTablePrefix(compactedTable.getKey());
      Map<String, String> compactedStringTable = compactedTable.getValue().entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> getFromCodecBuffer(e.getValue())));
      Map<String, String> expectedValue = dummyTableValues.entrySet().stream()
          .filter(e -> e.getKey().startsWith(prefix))
          .collect(Collectors.toMap(Map.Entry::getKey, e -> getFromCodecBuffer(e.getValue())));
      assertEquals(expectedValue, compactedStringTable);
      assertEquals(expectedValue, tableMap.get(compactedTable.getKey()).getMap().entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> getFromCodecBuffer(e.getValue()))));
    }
    for (Map.Entry<String, InMemoryTestTable<String, CodecBuffer>> nonCompactedTable : tableMap.entrySet()) {
      if (!tablesCompacted.containsKey(nonCompactedTable.getKey())) {
        assertEquals(dummyTableValues, nonCompactedTable.getValue().getMap());
      }
    }
    verify(snapshotMetrics).incNumSnapshotFullDefragTablesCompacted(2L);
  }

  @Test
  public void testIngestNonIncrementalTables() throws Exception {
    DBStore checkpointDBStore = mock(DBStore.class);
    DBStore snapshotDBStore = mock(DBStore.class);
    SnapshotInfo snapshotInfo = createMockSnapshotInfo(UUID.randomUUID(), "vol1", "bucket1", "snap1");
    TablePrefixInfo prefixInfo = new TablePrefixInfo(ImmutableMap.of("cf1", "ab", "cf2", "cd",
        "cf3", "ef", "cf4", ""));
    Set<String> incrementalTables = Stream.of("cf1", "cf2").collect(Collectors.toSet());
    Map<String, String> dumpedFileName = new HashMap<>();
    Map<String, Table<CodecBuffer, CodecBuffer>> snapshotTables = Stream.of("cf1", "cf2", "cf3", "cf4", "cf5")
        .map(name -> {
          Table<CodecBuffer, CodecBuffer> table = mock(Table.class);
          when(table.getName()).thenReturn(name);
          try {
            doAnswer(i -> {
              CodecBuffer prefixBytes = i.getArgument(1) == null ? null : i.getArgument(1, CodecBuffer.class);
              String prefix = prefixBytes == null ? "" : StringCodec.get().fromCodecBuffer(prefixBytes);
              assertEquals(prefixInfo.getTablePrefix(name), prefix);
              dumpedFileName.put(name, i.getArgument(0, File.class).toPath().toAbsolutePath().toString());
              return null;
            }).when(table).dumpToFileWithPrefix(any(File.class), any());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return table;
        }).collect(Collectors.toMap(Table::getName, Function.identity()));
    Map<String, String> ingestedFiles = new HashMap<>();
    Map<String, Table> checkpointTables = Stream.of("cf3", "cf4", "cf5")
        .map(name -> {
          Table<CodecBuffer, CodecBuffer> table = mock(Table.class);
          when(table.getName()).thenReturn(name);
          try {
            doAnswer(i -> {
              File file = i.getArgument(0, File.class);
              ingestedFiles.put(name, file.toPath().toAbsolutePath().toString());
              return null;
            }).when(table).loadFromFile(any(File.class));
          } catch (RocksDatabaseException e) {
            throw new RuntimeException(e);
          }
          return table;
        }).collect(Collectors.toMap(Table::getName, Function.identity()));

    OmSnapshot snapshot = mock(OmSnapshot.class);
    OmMetadataManagerImpl snapshotMetadataManager = mock(OmMetadataManagerImpl.class);
    UncheckedAutoCloseableSupplier<OmSnapshot> snapshotSupplier = new UncheckedAutoCloseableSupplier<OmSnapshot>() {
      @Override
      public OmSnapshot get() {
        return snapshot;
      }

      @Override
      public void close() {

      }
    };

    when(omSnapshotManager.getActiveSnapshot(anyString(), anyString(), anyString())).thenReturn(snapshotSupplier);
    when(snapshot.getMetadataManager()).thenReturn(snapshotMetadataManager);
    when(snapshotMetadataManager.getStore()).thenReturn(snapshotDBStore);
    List<Table<?, ?>> snapshotTableList = new ArrayList<>(snapshotTables.values());
    when(snapshotDBStore.listTables()).thenReturn(snapshotTableList);

    doAnswer(i -> {
      String tableName = i.getArgument(0, String.class);
      return snapshotTables.get(tableName);
    }).when(snapshotDBStore).getTable(anyString(), eq(CodecBufferCodec.get(true)), eq(CodecBufferCodec.get(true)));
    doAnswer(i -> {
      String tableName = i.getArgument(0, String.class);
      return checkpointTables.get(tableName);
    }).when(checkpointDBStore).getTable(anyString(), eq(CodecBufferCodec.get(true)), eq(CodecBufferCodec.get(true)));

    defragService.ingestNonIncrementalTables(checkpointDBStore, snapshotInfo, prefixInfo, incrementalTables);
    assertEquals(checkpointTables.keySet(), dumpedFileName.keySet());
    assertEquals(dumpedFileName, ingestedFiles);
  }

  private void assertContents(Map<String, Map<String, String>> tableContents, DBStore dbStore)
      throws RocksDatabaseException, CodecException {
    for (String tableName : dbStore.getTableNames().values()) {
      Table<String, String> table = dbStore.getTable(tableName, StringCodec.get(), StringCodec.get());
      try (Table.KeyValueIterator<String, String> iterator = table.iterator()) {
        Map<String, String> expectedContents = tableContents.get(tableName);
        Map<String, String> actualContents = new HashMap<>();
        while (iterator.hasNext()) {
          Table.KeyValue<String, String> kv = iterator.next();
          actualContents.put(kv.getKey(), kv.getValue());
        }
        assertNotNull(expectedContents, "Expected contents for table " + tableName + " is null");
        assertEquals(expectedContents, actualContents, "Table contents mismatch for table " + tableName);
      }
    }
  }

  private static Stream<Arguments> testCreateCheckpointCases() {
    // Have random tables to be incremental to ensure content gets preserved.
    return Stream.of(
        Arguments.of(ImmutableSet.of(KEY_TABLE, BUCKET_TABLE, DIRECTORY_TABLE)),
        Arguments.of(ImmutableSet.of(FILE_TABLE, DIRECTORY_TABLE, KEY_TABLE)),
        Arguments.of(ImmutableSet.of(VOLUME_TABLE, BUCKET_TABLE, DELEGATION_TOKEN_TABLE))
    );
  }

  private Map<String, Map<String, String>> createTableContents(Path path, String keyPrefix) throws IOException {
    DBCheckpoint snapshotCheckpointLocation = new RocksDBCheckpoint(path);
    Map<String, Map<String, String>> tableContents = new HashMap<>();
    try (OmMetadataManagerImpl metadataManager = OmMetadataManagerImpl.createCheckpointMetadataManager(configuration,
        snapshotCheckpointLocation, false)) {
      Set<String> metadataManagerTables = new HashSet<>(metadataManager.listTableNames());
      for (String tableName : metadataManager.getStore().getTableNames().values()) {
        if (metadataManagerTables.contains(tableName)) {
          Table<String, String> table = metadataManager.getStore().getTable(tableName,
              StringCodec.get(), StringCodec.get());
          for (int i = 0; i < 10; i++) {
            String key = tableName + keyPrefix + i;
            String value = "value_" + i;
            table.put(key, value);
            tableContents.computeIfAbsent(tableName, k -> new HashMap<>()).put(key, value);
          }
        } else {
          tableContents.put(tableName, Collections.emptyMap());
        }
      }
    }
    return tableContents;
  }

  @ParameterizedTest
  @MethodSource("testCreateCheckpointCases")
  public void testCreateCheckpoint(Set<String> incrementalTables) throws Exception {
    SnapshotInfo snapshotInfo = createMockSnapshotInfo(UUID.randomUUID(), "vol1", "bucket1", "snap1");
    DBCheckpoint snapshotCheckpointLocation =
        new RocksDBCheckpoint(tempDir.resolve(snapshotInfo.getSnapshotId().toString()));
    Map<String, Map<String, String>> tableContents =
        createTableContents(snapshotCheckpointLocation.getCheckpointLocation(), "_key_");
    UncheckedAutoCloseableSupplier<OmSnapshot> snapshotSupplier = new UncheckedAutoCloseableSupplier<OmSnapshot>() {
      private final OmMetadataManagerImpl snapshotMetadataManager =
          OmMetadataManagerImpl.createCheckpointMetadataManager(configuration, snapshotCheckpointLocation, false);

      @Override
      public OmSnapshot get() {
        OmSnapshot snapshot = mock(OmSnapshot.class);
        when(snapshot.getMetadataManager()).thenReturn(snapshotMetadataManager);
        return snapshot;
      }

      @Override
      public void close() {
        try {
          snapshotMetadataManager.close();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    };

    when(omSnapshotManager.getActiveSnapshot(eq(snapshotInfo.getVolumeName()), eq(snapshotInfo.getBucketName()),
        eq(snapshotInfo.getName()))).thenReturn(snapshotSupplier);
    try (OmMetadataManagerImpl result = defragService.createCheckpoint(snapshotInfo, incrementalTables)) {
      try (OmMetadataManagerImpl originalSnapshotStore =
               OmMetadataManagerImpl.createCheckpointMetadataManager(configuration, snapshotCheckpointLocation)) {
        assertContents(tableContents, originalSnapshotStore.getStore());
      } catch (Throwable e) {
        throw new RuntimeException("Failed to load original snapshot store", e);
      }
      // Ensure non-incremental tables are cleared.
      tableContents.entrySet().stream()
          .filter(e -> !incrementalTables.contains(e.getKey()))
          .forEach(e -> e.getValue().clear());
      assertContents(tableContents, result.getStore());
    }
  }

  private void assertContents(Map<String, Map<String, String>> contents, Path path) throws IOException {
    DBCheckpoint dbCheckpoint = new RocksDBCheckpoint(path);
    try (OmMetadataManagerImpl metadataManager = OmMetadataManagerImpl.createCheckpointMetadataManager(configuration,
        dbCheckpoint, true)) {
      assertContents(contents, metadataManager.getStore());
    }
  }

  @Test
  public void testAtomicSwitchSnapshotDB() throws Exception {
    UUID snapshotId = UUID.randomUUID();
    Path checkpointPath = tempDir.resolve("checkpoint");
    Map<String, Map<String, String>> checkpointContent = createTableContents(checkpointPath, "_cp1_");
    WritableOmSnapshotLocalDataProvider provider = mock(WritableOmSnapshotLocalDataProvider.class);
    OmSnapshotLocalData localData = mock(OmSnapshotLocalData.class);

    when(snapshotLocalDataManager.getWritableOmSnapshotLocalData(snapshotId)).thenReturn(provider);
    when(provider.getSnapshotLocalData()).thenReturn(localData);
    AtomicInteger version = new AtomicInteger(1);
    when(localData.getVersion()).thenAnswer(i -> version.get());
    doAnswer(i -> {
      version.incrementAndGet();
      return null;
    }).when(provider).addSnapshotVersion(any());

    Path nextVersionPath = tempDir.resolve(snapshotId + "v2").toAbsolutePath();
    try (MockedStatic<OmSnapshotManager> mockedStatic = Mockito.mockStatic(OmSnapshotManager.class,
        CALLS_REAL_METHODS)) {
      mockedStatic.when(() -> OmSnapshotManager.getSnapshotPath(eq(metadataManager), eq(snapshotId), eq(2)))
          .thenReturn(nextVersionPath);
      Map<String, Map<String, String>> existingVersionContents = createTableContents(nextVersionPath, "_cp2_");
      assertNotEquals(existingVersionContents, checkpointContent);
      assertContents(existingVersionContents, nextVersionPath);
      int result = defragService.atomicSwitchSnapshotDB(snapshotId, checkpointPath);
      assertContents(checkpointContent, nextVersionPath);
      assertEquals(1, result);
      assertEquals(2, version.get());
    }
    assertNull(verify(provider).getSnapshotLocalData());
  }

  private void createMockSnapshot(SnapshotInfo snapshotInfo, Map<String, CodecBuffer> tableContents,
      String... tables) throws IOException {
    OmSnapshot snapshot = mock(OmSnapshot.class);
    UncheckedAutoCloseableSupplier<OmSnapshot> snapshotSupplier = new UncheckedAutoCloseableSupplier<OmSnapshot>() {

      @Override
      public void close() {

      }

      @Override
      public OmSnapshot get() {
        return snapshot;
      }
    };
    Map<String, InMemoryTestTable<String, CodecBuffer>> tableMap = createTables(tableContents, tables);
    OMMetadataManager snapshotMetadataManager = mock(OMMetadataManager.class);
    when(snapshot.getMetadataManager()).thenReturn(snapshotMetadataManager);
    DBStore snapshotDBStore = mock(DBStore.class);
    when(snapshotDBStore.getTable(anyString(), eq(StringCodec.get()), eq(CodecBufferCodec.get(true))))
        .thenAnswer(i -> tableMap.getOrDefault(i.getArgument(0, String.class), null));
    when(snapshotMetadataManager.getStore()).thenReturn(snapshotDBStore);
    when(omSnapshotManager.getActiveSnapshot(eq(snapshotInfo.getVolumeName()), eq(snapshotInfo.getBucketName()),
        eq(snapshotInfo.getName()))).thenReturn(snapshotSupplier);
  }

  /**
   * Tests the incremental defragmentation process between two snapshots.
   *
   * <p>This parameterized test validates the {@code performIncrementalDefragmentation} method
   * across different version scenarios (0, 1, 2, 10) to ensure proper handling of snapshot
   * delta files and version-specific optimizations.</p>
   *
   * <h3>Test Data Generation:</h3>
   * Creates 67,600 synthetic key-value pairs (26×26×100) distributed across two snapshots
   * with the following patterns based on {@code i % 6}:
   * <ul>
   *   <li><b>i % 6 == 0:</b> Different values in snap1 and snap2 (updates)</li>
   *   <li><b>i % 6 == 1:</b> Value exists only in snap2 (insertions)</li>
   *   <li><b>i % 6 == 2:</b> Value exists only in snap1 (deletions)</li>
   *   <li><b>i % 6 == 3:</b> Identical values in both snapshots (no change)</li>
   *   <li><b>i % 6 == 4:</b> Absent in both snapshots</li>
   *   <li><b>i % 6 == 5:</b> Different values, but excluded from delta files</li>
   * </ul>
   *
   * <h3>Mock Setup:</h3>
   * <ul>
   *   <li>Two column families (cf1, cf2) configured for incremental tracking</li>
   *   <li>Three delta SST files: one for cf1, two for cf2</li>
   *   <li>Table prefix mappings: cf1→"ab", cf2→"cd", cf3→"ef"</li>
   *   <li>RDBSstFileWriter mock captures put/delete operations</li>
   *   <li>SstFileSetReader mock returns keys with indices 0-4 (i % 6 < 5)</li>
   * </ul>
   *
   * <h3>Version-Specific Behavior:</h3>
   * <ul>
   *   <li><b>currentVersion == 0 (initial version):</b>
   *     <ul>
   *       <li>All incremental tables are dumped to new SST files</li>
   *       <li>All dumped files are ingested into the checkpoint database</li>
   *     </ul>
   *   </li>
   *   <li><b>currentVersion > 0 (subsequent versions):</b>
   *     <ul>
   *       <li>Single delta file tables (cf1) are ingested directly without merging</li>
   *       <li>Multiple delta file tables (cf2) are merged and dumped before ingestion</li>
   *       <li>Optimization: avoids unnecessary file I/O for single delta files</li>
   *     </ul>
   *   </li>
   * </ul>
   *
   * <h3>Assertions:</h3>
   * <ul>
   *   <li>Verifies correct tables are dumped and ingested based on version</li>
   *   <li>Validates that only modified keys (i % 6 < 3) appear in delta files</li>
   *   <li>Confirms written values match snap2's values or null for deletions</li>
   *   <li>Ensures all incremental tables are ultimately ingested</li>
   * </ul>
   *
   * @param currentVersion the snapshot version being defragmented (0 for initial, >0 for subsequent)
   * @throws Exception if any error occurs during the test execution
   */
  @SuppressWarnings("checkstyle:MethodLength")
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 10})
  public void testPerformIncrementalDefragmentation(int currentVersion) throws Exception {
    DBStore checkpointDBStore = mock(DBStore.class);
    String samePrefix = "samePrefix";
    String snap1Prefix = "snap1Prefix";
    String snap2Prefix = "snap2Prefix";
    Set<String> incrementalTables = Stream.of("cf1", "cf2").collect(Collectors.toSet());
    Map<String, CodecBuffer> snap1TableValues = new HashMap<>();
    Map<String, CodecBuffer> snap2TableValues = new HashMap<>();
    for (char c = 'a'; c <= 'z'; c++) {
      for (char d = 'a'; d <= 'z'; d++) {
        for (int i = 0; i < 100; i++) {
          String key = String.format("%c%c%03d", c, d, i);
          String snap1Value, snap2Value;
          if (i % 6 == 0) {
            // Value is different.
            snap1Value = String.valueOf(c) + d + snap1Prefix + i;
            snap2Value = String.valueOf(c) + d + snap2Prefix + i;
          } else if (i % 6 == 1) {
            // Value is present in snap2 and not present in snap1.
            snap1Value = null;
            snap2Value = String.valueOf(c) + d + snap2Prefix + i;
          } else if (i % 6 == 2) {
            // Value is present in snap1 and not present in snap2.
            snap1Value = String.valueOf(c) + d + snap1Prefix + i;
            snap2Value = null;
          } else if (i % 6 == 3) {
            // Value is same.
            snap1Value = String.valueOf(c) + d + samePrefix + i;
            snap2Value = snap1Value;
          } else if (i % 6 == 4) {
            // both values are absent in snap1 and snap2.
            snap1Value = null;
            snap2Value = null;
          } else {
            // Value is different but this is key which is not present in delta file keys.
            snap1Value = String.valueOf(c) + d + snap1Prefix + i;
            snap2Value = String.valueOf(c) + d + snap2Prefix + i;
          }
          if (snap1Value != null) {
            snap1TableValues.put(key, getCodecBuffer(snap1Value));
          }
          if (snap2Value != null) {
            snap2TableValues.put(key, getCodecBuffer(snap2Value));
          }
        }
      }
    }
    SnapshotInfo snap1Info = createMockSnapshotInfo(UUID.randomUUID(), "vol1", "bucket1", "snap1");
    SnapshotInfo snap2Info = createMockSnapshotInfo(UUID.randomUUID(), "vol1", "bucket1", "snap2");
    createMockSnapshot(snap1Info, snap1TableValues, "cf1", "cf2", "cf3");
    createMockSnapshot(snap2Info, snap2TableValues, "cf1", "cf2", "cf3");
    TablePrefixInfo prefixInfo = new TablePrefixInfo(ImmutableMap.of("cf1", "ab", "cf2", "cd",
        "cf3", "ef"));

    List<Pair<Path, SstFileInfo>> deltaFiles = ImmutableList.of(
        Pair.of(tempDir.resolve("1.sst").toAbsolutePath(), new SstFileInfo("1", "", "", "cf1")),
        Pair.of(tempDir.resolve("2.sst").toAbsolutePath(), new SstFileInfo("2", "", "", "cf2")),
        Pair.of(tempDir.resolve("3.sst").toAbsolutePath(), new SstFileInfo("3", "", "", "cf2")));
    Map<String, String> deltaFileNamesToTableMap = deltaFiles.stream()
        .collect(Collectors.groupingBy(pair -> pair.getValue().getColumnFamily())).entrySet()
        .stream().collect(Collectors.toMap(
            e -> e.getValue().stream().map(Pair::getKey).map(Path::toString).sorted().collect(Collectors.joining(",")),
            Map.Entry::getKey));
    for (Pair<Path, SstFileInfo> deltaFile : deltaFiles) {
      assertTrue(deltaFile.getKey().toFile().createNewFile());
    }
    AtomicReference<String> currentTable = new AtomicReference<>();
    Map<String, String> dumpedFileName = new HashMap<>();
    Map<Path, List<Pair<String, String>>> deltaFileContents = new HashMap<>();
    when(deltaFileComputer.getDeltaFiles(eq(snap1Info), eq(snap2Info), eq(incrementalTables))).thenReturn(deltaFiles);
    try (MockedConstruction<RDBSstFileWriter> rdbSstFileWriter =
             Mockito.mockConstruction(RDBSstFileWriter.class, (mock, context) -> {
               File file = (File) context.arguments().get(0);
               assertTrue(file.createNewFile() || file.exists());
               Path filePath = file.toPath().toAbsolutePath();
               dumpedFileName.put(filePath.toString(), currentTable.get());
               doAnswer(i -> {
                 String key = StringCodec.get().fromCodecBuffer(i.getArgument(0));
                 String value = StringCodec.get().fromCodecBuffer(i.getArgument(1));
                 deltaFileContents.computeIfAbsent(filePath, k -> new ArrayList<>())
                     .add(Pair.of(key, value));
                 return null;
               }).when(mock).put(any(CodecBuffer.class), any(CodecBuffer.class));
               doAnswer(i -> {
                 String key = StringCodec.get().fromCodecBuffer(i.getArgument(0));
                 deltaFileContents.computeIfAbsent(filePath, k -> new ArrayList<>()).add(Pair.of(key, null));
                 return null;
               }).when(mock).delete(any(CodecBuffer.class));
             });
         MockedConstruction<SstFileSetReader> sstFileSetReader =
             Mockito.mockConstruction(SstFileSetReader.class, (mock, context) -> {
               String deltaKey = ((Collection<Path>) context.arguments().get(0))
                   .stream().map(Path::toAbsolutePath).map(Path::toString).sorted().collect(Collectors.joining(","));
               String tableName = deltaFileNamesToTableMap.get(deltaKey);
               currentTable.set(tableName);
               doAnswer(i -> {
                 String lowerBound = i.getArgument(0);
                 String upperBound = i.getArgument(1);
                 assertEquals(prefixInfo.getTablePrefix(tableName), lowerBound);
                 assertEquals(StringUtils.getLexicographicallyHigherString(lowerBound), upperBound);
                 Iterator<String> itr = IntStream.range(0, 100).filter(idx -> idx % 6 < 5).boxed()
                     .map(idx -> String.format("%s%03d", lowerBound, idx)).iterator();
                 return new ClosableIterator<String>() {
                   @Override
                   public void close() {

                   }

                   @Override
                   public boolean hasNext() {
                     return itr.hasNext();
                   }

                   @Override
                   public String next() {
                     return itr.next();
                   }
                 };
               }).when(mock).getKeyStreamWithTombstone(anyString(), anyString());
             })
    ) {
      Map<String, String> ingestedFiles = new HashMap<>();
      Map<String, Table> checkpointTables = incrementalTables.stream()
          .map(name -> {
            Table<CodecBuffer, CodecBuffer> table = mock(Table.class);
            when(table.getName()).thenReturn(name);
            try {
              doAnswer(i -> {
                File file = i.getArgument(0, File.class);
                ingestedFiles.put(file.toPath().toAbsolutePath().toString(), name);
                return null;
              }).when(table).loadFromFile(any(File.class));
            } catch (RocksDatabaseException e) {
              throw new RuntimeException(e);
            }
            return table;
          }).collect(Collectors.toMap(Table::getName, Function.identity()));
      doAnswer(i -> {
        String tableName = i.getArgument(0, String.class);
        return checkpointTables.get(tableName);
      }).when(checkpointDBStore).getTable(anyString());
      defragService.performIncrementalDefragmentation(snap1Info, snap2Info, currentVersion, checkpointDBStore,
          prefixInfo, incrementalTables);
      if (currentVersion == 0) {
        assertEquals(incrementalTables, new HashSet<>(dumpedFileName.values()));
        assertEquals(ingestedFiles, dumpedFileName);
      } else {
        assertEquals(ImmutableSet.of("cf2"), new HashSet<>(dumpedFileName.values()));
        assertEquals("cf1", ingestedFiles.get(deltaFiles.get(0).getLeft().toAbsolutePath().toString()));
        assertEquals(ingestedFiles.entrySet().stream().filter(e -> e.getValue().equals(
            "cf2")).collect(Collectors.toSet()), dumpedFileName.entrySet().stream().filter(e -> e.getValue().equals(
            "cf2")).collect(Collectors.toSet()));
      }
      assertEquals(incrementalTables, new HashSet<>(ingestedFiles.values()));
      for (Map.Entry<Path, List<Pair<String, String>>> deltaFileContent : deltaFileContents.entrySet()) {
        int idx = 0;
        for (int i = 0; i < 100; i++) {
          String tableName = dumpedFileName.get(deltaFileContent.getKey().toString());
          if (i % 6 < 3) {
            String key = String.format("%s%03d", prefixInfo.getTablePrefix(tableName), i);
            Pair<String, String> actualKey = deltaFileContent.getValue().get(idx);
            CodecBuffer value = snap2TableValues.get(key);
            Pair<String, String> expectedKey = Pair.of(key, value == null ? null :
                StringCodec.get().fromCodecBuffer(value));
            assertEquals(expectedKey, actualKey, "Key mismatch for table " + tableName + " at index " + i);
            idx++;
          }
        }
      }
      verify(snapshotMetrics).incNumSnapshotIncDefragDeltaFilesProcessed(3L);
    }
  }

  @Test
  public void testCheckAndDefragDeletedSnapshot() throws IOException {
    SnapshotInfo snapshotInfo = createMockSnapshotInfo(UUID.randomUUID(), "vol1", "bucket1", "snap1");
    snapshotInfo.setSnapshotStatus(SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED);
    SnapshotChainManager chainManager = mock(SnapshotChainManager.class);
    try (MockedStatic<SnapshotUtils> mockedStatic = Mockito.mockStatic(SnapshotUtils.class)) {
      mockedStatic.when(() -> SnapshotUtils.getSnapshotInfo(eq(ozoneManager), eq(chainManager),
          eq(snapshotInfo.getSnapshotId()))).thenReturn(snapshotInfo);
      assertFalse(defragService.checkAndDefragSnapshot(chainManager, snapshotInfo.getSnapshotId()));
      verify(snapshotMetrics).incNumSnapshotDefragSnapshotSkipped();
    }
  }

  @Test
  public void testCheckAndDefragAlreadyDefraggedSnapshot() throws IOException {
    SnapshotInfo snapshotInfo = createMockSnapshotInfo(UUID.randomUUID(), "vol1", "bucket1", "snap1");
    SnapshotChainManager chainManager = mock(SnapshotChainManager.class);
    SnapshotDefragService spyDefragService = Mockito.spy(defragService);
    try (MockedStatic<SnapshotUtils> mockedStatic = Mockito.mockStatic(SnapshotUtils.class)) {
      mockedStatic.when(() -> SnapshotUtils.getSnapshotInfo(eq(ozoneManager), eq(chainManager),
          eq(snapshotInfo.getSnapshotId()))).thenReturn(snapshotInfo);
      doReturn(Pair.of(false, 0)).when(spyDefragService).needsDefragmentation(eq(snapshotInfo));
      assertFalse(spyDefragService.checkAndDefragSnapshot(chainManager, snapshotInfo.getSnapshotId()));
      verify(snapshotMetrics).incNumSnapshotDefragSnapshotSkipped();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCheckAndDefragActiveSnapshot(boolean previousSnapshotExists) throws IOException {
    SnapshotInfo snapshotInfo = createMockSnapshotInfo(UUID.randomUUID(), "vol1", "bucket1", "snap2");
    SnapshotInfo previousSnapshotInfo;
    if (previousSnapshotExists) {
      previousSnapshotInfo = createMockSnapshotInfo(UUID.randomUUID(), "vol1", "bucket1", "snap1");
      snapshotInfo.setPathPreviousSnapshotId(previousSnapshotInfo.getSnapshotId());
    } else {
      previousSnapshotInfo = null;
    }

    SnapshotChainManager chainManager = mock(SnapshotChainManager.class);
    try (MockedStatic<SnapshotUtils> mockedStatic = Mockito.mockStatic(SnapshotUtils.class)) {
      mockedStatic.when(() -> SnapshotUtils.getSnapshotInfo(eq(ozoneManager), eq(chainManager),
          eq(snapshotInfo.getSnapshotId()))).thenReturn(snapshotInfo);
      if (previousSnapshotExists) {
        mockedStatic.when(() -> SnapshotUtils.getSnapshotInfo(eq(ozoneManager), eq(chainManager),
            eq(previousSnapshotInfo.getSnapshotId()))).thenReturn(previousSnapshotInfo);
      }
      SnapshotDefragService spyDefragService = Mockito.spy(defragService);
      doReturn(Pair.of(true, 10)).when(spyDefragService).needsDefragmentation(eq(snapshotInfo));
      OmMetadataManagerImpl checkpointMetadataManager = mock(OmMetadataManagerImpl.class);
      File checkpointPath = tempDir.resolve("checkpoint").toAbsolutePath().toFile();
      DBStore checkpointDBStore = mock(DBStore.class);
      SnapshotInfo checkpointSnapshotInfo = previousSnapshotExists ? previousSnapshotInfo : snapshotInfo;
      when(checkpointMetadataManager.getStore()).thenReturn(checkpointDBStore);
      when(checkpointDBStore.getDbLocation()).thenReturn(checkpointPath);
      doReturn(checkpointMetadataManager).when(spyDefragService).createCheckpoint(eq(checkpointSnapshotInfo),
          eq(COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT));
      TablePrefixInfo prefixInfo = new TablePrefixInfo(Collections.emptyMap());
      when(metadataManager.getTableBucketPrefix(eq(snapshotInfo.getVolumeName()), eq(snapshotInfo.getBucketName())))
          .thenReturn(prefixInfo);
      doNothing().when(spyDefragService).performFullDefragmentation(eq(checkpointDBStore), eq(prefixInfo),
          eq(COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT));
      doNothing().when(spyDefragService).performIncrementalDefragmentation(eq(previousSnapshotInfo),
          eq(snapshotInfo), eq(10), eq(checkpointDBStore), eq(prefixInfo),
          eq(COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT));
      AtomicInteger lockAcquired = new AtomicInteger(0);
      AtomicInteger lockReleased = new AtomicInteger(0);
      when(omLock.acquireWriteLocks(eq(SNAPSHOT_DB_CONTENT_LOCK), anyCollection())).thenAnswer(i -> {
        if (i.getArgument(1) != null && i.getArgument(1, Collection.class).size() == 1) {
          Collection<String[]> keys = i.getArgument(1, Collection.class);
          assertEquals(snapshotInfo.getSnapshotId().toString(), keys.stream().findFirst().get()[0]);
          lockAcquired.incrementAndGet();
        }
        return OMLockDetails.EMPTY_DETAILS_LOCK_ACQUIRED;
      });
      when(omLock.releaseWriteLocks(eq(SNAPSHOT_DB_CONTENT_LOCK), anyCollection())).thenAnswer(i -> {
        if (i.getArgument(1) != null && i.getArgument(1, Collection.class).size() == 1) {
          Collection<String[]> keys = i.getArgument(1, Collection.class);
          assertEquals(snapshotInfo.getSnapshotId().toString(), keys.stream().findFirst().get()[0]);
          lockReleased.incrementAndGet();
        }
        return OMLockDetails.EMPTY_DETAILS_LOCK_NOT_ACQUIRED;
      });
      AtomicBoolean checkpointClosed = new AtomicBoolean(false);
      doAnswer(i -> {
        assertTrue(lockAcquired.get() == 1 && lockReleased.get() == 0);
        return null;
      }).when(spyDefragService).ingestNonIncrementalTables(eq(checkpointDBStore),
          eq(snapshotInfo), eq(prefixInfo), eq(COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT));
      doAnswer(i -> {
        assertTrue(lockAcquired.get() == 1 && lockReleased.get() == 0);
        checkpointClosed.set(true);
        return null;
      }).when(checkpointMetadataManager).close();
      doAnswer(i -> {
        assertTrue(lockAcquired.get() == 1 && lockReleased.get() == 0);
        assertTrue(checkpointClosed.get());
        return 20;
      }).when(spyDefragService).atomicSwitchSnapshotDB(eq(snapshotInfo.getSnapshotId()), eq(checkpointPath.toPath()));
      doAnswer(i -> {
        assertTrue(lockAcquired.get() == 1 && lockReleased.get() == 0);
        assertTrue(checkpointClosed.get());
        return null;
      }).when(omSnapshotManager).deleteSnapshotCheckpointDirectories(eq(snapshotInfo.getSnapshotId()), eq(20));
      InOrder verifier = inOrder(spyDefragService, omSnapshotManager);
      assertTrue(spyDefragService.checkAndDefragSnapshot(chainManager, snapshotInfo.getSnapshotId()));
      assertTrue(lockAcquired.get() == 1 && lockReleased.get() == 1);
      verifier.verify(spyDefragService).needsDefragmentation(eq(snapshotInfo));
      verifier.verify(spyDefragService).createCheckpoint(eq(checkpointSnapshotInfo),
          eq(COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT));
      if (previousSnapshotExists) {
        verifier.verify(spyDefragService).performIncrementalDefragmentation(eq(previousSnapshotInfo),
            eq(snapshotInfo), eq(10), eq(checkpointDBStore), eq(prefixInfo),
            eq(COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT));
      } else {
        verifier.verify(spyDefragService).performFullDefragmentation(eq(checkpointDBStore), eq(prefixInfo),
            eq(COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT));
      }
      verifier.verify(spyDefragService).ingestNonIncrementalTables(eq(checkpointDBStore), eq(snapshotInfo),
          eq(prefixInfo), eq(COLUMN_FAMILIES_TO_TRACK_IN_SNAPSHOT));
      verifier.verify(spyDefragService).atomicSwitchSnapshotDB(eq(snapshotInfo.getSnapshotId()),
          eq(checkpointPath.toPath()));
      verifier.verify(omSnapshotManager).deleteSnapshotCheckpointDirectories(eq(snapshotInfo.getSnapshotId()), eq(20));
      // Verify metrics
      verify(snapshotMetrics).incNumSnapshotDefrag();
      if (previousSnapshotExists) {
        verify(snapshotMetrics).incNumSnapshotIncDefrag();
        verify(perfMetrics).setSnapshotDefragServiceIncLatencyMs(Mockito.anyLong());
      } else {
        verify(snapshotMetrics).incNumSnapshotFullDefrag();
        verify(perfMetrics).setSnapshotDefragServiceFullLatencyMs(Mockito.anyLong());
      }
    }
  }

  /**
   * Helper method to create a mock SnapshotInfo.
   */
  private SnapshotInfo createMockSnapshotInfo(UUID snapshotId, String volume, String bucket, String name) {
    SnapshotInfo.Builder builder = SnapshotInfo.newBuilder();
    builder.setSnapshotId(snapshotId);
    builder.setVolumeName(volume);
    builder.setBucketName(bucket);
    builder.setName(name);
    builder.setSnapshotStatus(SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE);
    builder.setCreationTime(System.currentTimeMillis());
    return builder.build();
  }
}
