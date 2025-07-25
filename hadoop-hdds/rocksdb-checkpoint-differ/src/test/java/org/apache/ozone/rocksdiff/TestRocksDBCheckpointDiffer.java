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

package org.apache.ozone.rocksdiff;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.hadoop.hdds.StringUtils.bytes2String;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_PRUNE_DAEMON_RUN_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_LOAD_NATIVE_LIB;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_LOAD_NATIVE_LIB_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_BACKUP_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_BACKUP_BATCH_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_DAG_DAEMON_RUN_INTERVAL_DEFAULT;
import static org.apache.hadoop.util.Time.now;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.COLUMN_FAMILIES_TO_TRACK_IN_DAG;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.COMPACTION_LOG_FILE_NAME_SUFFIX;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.PRUNED_SST_FILE_TEMP;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.SST_FILE_EXTENSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCheckpoint;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedEnvOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedFlushOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRawSSTFileIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRawSSTFileReader;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileReader;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileReaderIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileWriter;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.hadoop.util.Time;
import org.apache.ozone.compaction.log.CompactionFileInfo;
import org.apache.ozone.compaction.log.CompactionLogEntry;
import org.apache.ozone.rocksdb.util.RdbUtil;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.NodeComparator;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Test RocksDBCheckpointDiffer basic functionality.
 */
public class TestRocksDBCheckpointDiffer {

  private static final Logger LOG = LoggerFactory.getLogger(TestRocksDBCheckpointDiffer.class);

  private static final List<List<String>> SST_FILES_BY_LEVEL = Arrays.asList(
      Arrays.asList("000015", "000013", "000011", "000009"),
      Arrays.asList("000018", "000016", "000017", "000026", "000024", "000022",
          "000020"),
      Arrays.asList("000027", "000030", "000028", "000029", "000031", "000039",
          "000037", "000035", "000033"),
      Arrays.asList("000040", "000044", "000042", "000043", "000045", "000041",
          "000046", "000054", "000052", "000050", "000048"),
      Arrays.asList("000059", "000055", "000056", "000060", "000057", "000058")
  );

  private static final List<List<CompactionNode>> COMPACTION_NODES_BY_LEVEL =
      SST_FILES_BY_LEVEL.stream()
          .map(sstFiles ->
              sstFiles.stream()
                  .map(
                      sstFile -> new CompactionNode(sstFile,
                          1000L,
                          null, null, null
                      ))
                  .collect(Collectors.toList()))
          .collect(Collectors.toList());

  private final List<CompactionLogEntry> compactionLogEntryList = Arrays.asList(
      new CompactionLogEntry(101, System.currentTimeMillis(),
          Arrays.asList(
              new CompactionFileInfo("000068", "/volume/bucket2",
                  "/volume/bucket2", "bucketTable"),
              new CompactionFileInfo("000057", "/volume/bucket1",
                  "/volume/bucket1", "bucketTable")),
          Collections.singletonList(
              new CompactionFileInfo("000086", "/volume/bucket1",
                  "/volume/bucket2", "bucketTable")),
          null),
      new CompactionLogEntry(178, System.currentTimeMillis(),
          Arrays.asList(new CompactionFileInfo("000078",
                  "/volume/bucket1/key-0000001411",
                  "/volume/bucket2/key-0000099649",
                  "keyTable"),
              new CompactionFileInfo("000075",
                  "/volume/bucket1/key-0000016536",
                  "/volume/bucket2/key-0000098897",
                  "keyTable"),
              new CompactionFileInfo("000073",
                  "/volume/bucket1/key-0000000730",
                  "/volume/bucket2/key-0000097010",
                  "keyTable"),
              new CompactionFileInfo("000071",
                  "/volume/bucket1/key-0000001820",
                  "/volume/bucket2/key-0000097895",
                  "keyTable"),
              new CompactionFileInfo("000063",
                  "/volume/bucket1/key-0000001016",
                  "/volume/bucket1/key-0000099930",
                  "keyTable")),
          Collections.singletonList(new CompactionFileInfo("000081",
              "/volume/bucket1/key-0000000730",
              "/volume/bucket2/key-0000099649",
              "keyTable")),
          null
      ),
      new CompactionLogEntry(233, System.currentTimeMillis(),
          Arrays.asList(
              new CompactionFileInfo("000086", "/volume/bucket1",
                  "/volume/bucket2", "bucketTable"),
              new CompactionFileInfo("000088", "/volume/bucket3",
                  "/volume/bucket3", "bucketTable")),
          Collections.singletonList(
              new CompactionFileInfo("000110", "/volume/bucket1",
                  "/volume/bucket3", "bucketTable")
          ),
          null),
      new CompactionLogEntry(256, System.currentTimeMillis(),
          Arrays.asList(new CompactionFileInfo("000081",
                  "/volume/bucket1/key-0000000730",
                  "/volume/bucket2/key-0000099649",
                  "keyTable"),
              new CompactionFileInfo("000103",
                  "/volume/bucket1/key-0000017460",
                  "/volume/bucket3/key-0000097450",
                  "keyTable"),
              new CompactionFileInfo("000099",
                  "/volume/bucket1/key-0000002310",
                  "/volume/bucket3/key-0000098286",
                  "keyTable"),
              new CompactionFileInfo("000097",
                  "/volume/bucket1/key-0000005965",
                  "/volume/bucket3/key-0000099136",
                  "keyTable"),
              new CompactionFileInfo("000095",
                  "/volume/bucket1/key-0000012424",
                  "/volume/bucket3/key-0000083904",
                  "keyTable")),
          Collections.singletonList(new CompactionFileInfo("000106",
              "/volume/bucket1/key-0000000730",
              "/volume/bucket3/key-0000099136",
              "keyTable")),
          null),
      new CompactionLogEntry(397, now(),
          Arrays.asList(new CompactionFileInfo("000106",
                  "/volume/bucket1/key-0000000730",
                  "/volume/bucket3/key-0000099136",
                  "keyTable"),
              new CompactionFileInfo("000128",
                  "/volume/bucket2/key-0000005031",
                  "/volume/bucket3/key-0000084385",
                  "keyTable"),
              new CompactionFileInfo("000125",
                  "/volume/bucket2/key-0000003491",
                  "/volume/bucket3/key-0000088414",
                  "keyTable"),
              new CompactionFileInfo("000123",
                  "/volume/bucket2/key-0000007390",
                  "/volume/bucket3/key-0000094627",
                  "keyTable"),
              new CompactionFileInfo("000121",
                  "/volume/bucket2/key-0000003232",
                  "/volume/bucket3/key-0000094246",
                  "keyTable")),
          Collections.singletonList(new CompactionFileInfo("000131",
              "/volume/bucket1/key-0000000730",
              "/volume/bucket3/key-0000099136",
              "keyTable")),
          null
      )
  );

  private static Map<String, String> columnFamilyToPrefixMap1 =
      new HashMap<String, String>() {
        {
          put("keyTable", "/volume/bucket1/");
          // Simply using bucketName instead of ID for the test.
          put("directoryTable", "/volume/bucket1/");
          put("fileTable", "/volume/bucket1/");
        }
      };

  private static Map<String, String> columnFamilyToPrefixMap2 =
      new HashMap<String, String>() {
        {
          put("keyTable", "/volume/bucket2/");
          // Simply using bucketName instead of ID for the test.
          put("directoryTable", "/volume/bucket2/");
          put("fileTable", "/volume/bucket2/");
        }
      };

  private static Map<String, String> columnFamilyToPrefixMap3 =
      new HashMap<String, String>() {
        {
          put("keyTable", "/volume/bucket3/");
          // Simply using bucketName instead of ID for the test.
          put("directoryTable", "/volume/bucket3/");
          put("fileTable", "/volume/bucket3/");
        }
      };

  private static final int NUM_ROW = 250000;
  private static final int SNAPSHOT_EVERY_SO_MANY_KEYS = 49999;

  /**
   * RocksDB checkpoint path prefix.
   */
  private static final String CP_PATH_PREFIX = "rocksdb-cp-";
  private final List<DifferSnapshotInfo> snapshots = new ArrayList<>();

  private final List<File> cpDirList = new ArrayList<>();

  private final List<List<ColumnFamilyHandle>> colHandles = new ArrayList<>();

  private static final String ACTIVE_DB_DIR_NAME = "./rocksdb-data";
  private static final String METADATA_DIR_NAME = "./metadata";
  private static final String COMPACTION_LOG_DIR_NAME = "compaction-log";
  private static final String SST_BACK_UP_DIR_NAME = "compaction-sst-backup";
  private File activeDbDir;
  private File metadataDirDir;
  private File compactionLogDir;
  private File sstBackUpDir;
  private ConfigurationSource config;
  private ExecutorService executorService = Executors.newCachedThreadPool();
  private RocksDBCheckpointDiffer rocksDBCheckpointDiffer;
  private ManagedRocksDB activeRocksDB;

  private ManagedDBOptions dbOptions;
  private ColumnFamilyHandle keyTableCFHandle;
  private ColumnFamilyHandle directoryTableCFHandle;
  private ColumnFamilyHandle fileTableCFHandle;
  private ColumnFamilyHandle compactionLogTableCFHandle;

  public static final Integer DEBUG_DAG_BUILD_UP = 2;
  public static final Integer DEBUG_DAG_TRAVERSAL = 3;
  public static final Integer DEBUG_DAG_LIVE_NODES = 4;
  public static final Integer DEBUG_READ_ALL_DB_KEYS = 5;
  private static final HashSet<Integer> DEBUG_LEVEL = new HashSet<>();

  static {
    DEBUG_LEVEL.add(DEBUG_DAG_BUILD_UP);
    DEBUG_LEVEL.add(DEBUG_DAG_TRAVERSAL);
    DEBUG_LEVEL.add(DEBUG_DAG_LIVE_NODES);
  }

  @BeforeEach
  public void init() throws RocksDBException {
    // Checkpoint differ log level. Set to DEBUG for verbose output
    GenericTestUtils.setLogLevel(RocksDBCheckpointDiffer.class, Level.INFO);
    // Test class log level. Set to DEBUG for verbose output
    GenericTestUtils.setLogLevel(TestRocksDBCheckpointDiffer.class, Level.INFO);

    activeDbDir = new File(ACTIVE_DB_DIR_NAME);
    createDir(activeDbDir, ACTIVE_DB_DIR_NAME);

    metadataDirDir = new File(METADATA_DIR_NAME);
    createDir(metadataDirDir, METADATA_DIR_NAME);

    compactionLogDir = new File(METADATA_DIR_NAME, COMPACTION_LOG_DIR_NAME);
    createDir(compactionLogDir, METADATA_DIR_NAME + "/" + COMPACTION_LOG_DIR_NAME);

    sstBackUpDir = new File(METADATA_DIR_NAME, SST_BACK_UP_DIR_NAME);
    createDir(sstBackUpDir, METADATA_DIR_NAME + "/" + SST_BACK_UP_DIR_NAME);

    config = mock(ConfigurationSource.class);

    when(config.getTimeDuration(
        OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED,
        OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED_DEFAULT,
        TimeUnit.MILLISECONDS)).thenReturn(MINUTES.toMillis(10));

    when(config.getTimeDuration(
        OZONE_OM_SNAPSHOT_COMPACTION_DAG_PRUNE_DAEMON_RUN_INTERVAL,
        OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_DAG_DAEMON_RUN_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS)).thenReturn(0L);

    when(config.getInt(
        OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_BACKUP_BATCH_SIZE,
        OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_BACKUP_BATCH_SIZE_DEFAULT)).thenReturn(2000);

    when(config.getBoolean(
        OZONE_OM_SNAPSHOT_LOAD_NATIVE_LIB,
        OZONE_OM_SNAPSHOT_LOAD_NATIVE_LIB_DEFAULT)).thenReturn(true);

    try (MockedStatic<ManagedRawSSTFileReader> mockedRawSSTReader = Mockito.mockStatic(ManagedRawSSTFileReader.class)) {
      mockedRawSSTReader.when(ManagedRawSSTFileReader::loadLibrary).thenReturn(true);
      rocksDBCheckpointDiffer = new RocksDBCheckpointDiffer(METADATA_DIR_NAME,
          SST_BACK_UP_DIR_NAME,
          COMPACTION_LOG_DIR_NAME,
          ACTIVE_DB_DIR_NAME,
          config);
    }

    ManagedColumnFamilyOptions cfOpts = new ManagedColumnFamilyOptions();
    cfOpts.optimizeUniversalStyleCompaction();
    List<ColumnFamilyDescriptor> cfDescriptors = getCFDescriptorList(cfOpts);
    List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
    dbOptions = new ManagedDBOptions();
    dbOptions.setCreateIfMissing(true);
    dbOptions.setCreateMissingColumnFamilies(true);

    rocksDBCheckpointDiffer.setRocksDBForCompactionTracking(dbOptions);
    activeRocksDB = ManagedRocksDB.open(dbOptions, ACTIVE_DB_DIR_NAME, cfDescriptors, cfHandles);
    keyTableCFHandle = cfHandles.get(1);
    directoryTableCFHandle = cfHandles.get(2);
    fileTableCFHandle = cfHandles.get(3);
    compactionLogTableCFHandle = cfHandles.get(4);

    rocksDBCheckpointDiffer.setCompactionLogTableCFHandle(cfHandles.get(4));
    rocksDBCheckpointDiffer.setActiveRocksDB(activeRocksDB);
    rocksDBCheckpointDiffer.loadAllCompactionLogs();
  }

  private void createDir(File file, String filePath) {
    // Remove already existed dir.
    if (file.exists()) {
      deleteDirectory(file);
    }

    // Create new Dir.
    if (!file.mkdirs()) {
      fail("Error in creating directory: " + filePath);
    }
  }

  @AfterEach
  public void cleanUp() {
    IOUtils.closeQuietly(rocksDBCheckpointDiffer);
    IOUtils.closeQuietly(keyTableCFHandle);
    IOUtils.closeQuietly(directoryTableCFHandle);
    IOUtils.closeQuietly(fileTableCFHandle);
    IOUtils.closeQuietly(compactionLogTableCFHandle);
    IOUtils.closeQuietly(activeRocksDB);
    deleteDirectory(compactionLogDir);
    deleteDirectory(sstBackUpDir);
    deleteDirectory(metadataDirDir);
    deleteDirectory(activeDbDir);

    for (File dir : cpDirList) {
      deleteDirectory(dir);
    }
  }

  private static List<CompactionLogEntry> getPrunedCompactionEntries(boolean prune, Map<String, String[]> metadata) {
    List<CompactionLogEntry> entries = new ArrayList<>();
    if (!prune) {
      entries.add(createCompactionEntry(1,
          now(),
          Arrays.asList("1", "2"),
          Arrays.asList("4", "5"), metadata));
    }
    entries.addAll(Arrays.asList(createCompactionEntry(2,
            now(),
            Arrays.asList("4", "5"),
            Collections.singletonList("10"), metadata),
        createCompactionEntry(3,
            now(),
            Arrays.asList("3", "13", "14"),
            Arrays.asList("6", "7"), metadata),
        createCompactionEntry(4,
            now(),
            Arrays.asList("6", "7"),
            Collections.singletonList("11"), metadata)));
    return entries;
  }

  /**
   * Test cases for testGetSSTDiffListWithoutDB.
   */
  @SuppressWarnings("methodlength")
  private static Stream<Arguments> casesGetSSTDiffListWithoutDB() {

    String compactionLog =
        // Snapshot 0
        "S 1000 df6410c7-151b-4e90-870e-5ef12875acd5 " + now() + " \n"
            // Additional "compaction" to trigger and test early exit condition
            + "C 1291 000001,000002:000062\n"
            // Snapshot 1
            + "S 3008 ef6410c7-151b-4e90-870e-5ef12875acd5 " + now() + " \n"
            // Regular compaction
            + "C 4023 000068,000062:000069\n"
            // Trivial move
            + "C 5647 000071,000064,000060,000052:000071,000064,000060,000052\n"
            + "C 7658 000073,000066:000074\n"
            + "C 7872 000082,000076,000069:000083\n"
            + "C 9001 000087,000080,000074:000088\n"
            // Deletion?
            + "C 12755 000093,000090,000083:\n"
            // Snapshot 2
            + "S 14980 e7ad72f8-52df-4430-93f6-0ee91d4a47fd " + now() + "\n"
            + "C 16192 000098,000096,000085,000078,000071,000064,000060,000052"
            + ":000099\n"
            + "C 16762 000105,000095,000088:000107\n"
            // Snapshot 3
            + "S 17975 4f084f6e-ed3d-4780-8362-f832303309ea " + now() + "\n";

    List<CompactionLogEntry> compactionLogEntries = Arrays.asList(
        // Additional "compaction" to trigger and test early exit condition
        createCompactionEntry(1291,
            now(),
            Arrays.asList("000001", "000002"),
            Collections.singletonList("000062")),
        // Regular compaction
        createCompactionEntry(4023,
            now(),
            Arrays.asList("000068", "000062"),
            Collections.singletonList("000069")),
        // Trivial move
        createCompactionEntry(5547,
            now(),
            Arrays.asList("000071", "000064", "000060", "000052"),
            Arrays.asList("000071", "000064", "000060", "000062")),
        createCompactionEntry(5647,
            now(),
            Arrays.asList("000073", "000066"),
            Collections.singletonList("000074")),
        createCompactionEntry(7872,
            now(),
            Arrays.asList("000082", "000076", "000069"),
            Collections.singletonList("000083")),
        createCompactionEntry(9001,
            now(),
            Arrays.asList("000087", "000080", "000074"),
            Collections.singletonList("000088")),
        // Deletion
        createCompactionEntry(12755,
            now(),
            Arrays.asList("000093", "000090", "000083"),
            Collections.emptyList()),
        createCompactionEntry(16192,
            now(),
            Arrays.asList("000098", "000096", "000085", "000078", "000071",
                "000064", "000060", "000052"),
            Collections.singletonList("000099")),
        createCompactionEntry(16762,
            now(),
            Arrays.asList("000105", "000095", "000088"),
            Collections.singletonList("000107"))
    );

    DifferSnapshotInfo snapshotInfo1 = new DifferSnapshotInfo(
        "/path/to/dbcp1", UUID.randomUUID(), 3008L, null, Mockito.mock(ManagedRocksDB.class));
    DifferSnapshotInfo snapshotInfo2 = new DifferSnapshotInfo(
        "/path/to/dbcp2", UUID.randomUUID(), 14980L, null, Mockito.mock(ManagedRocksDB.class));
    DifferSnapshotInfo snapshotInfo3 = new DifferSnapshotInfo(
        "/path/to/dbcp3", UUID.randomUUID(), 17975L, null, Mockito.mock(ManagedRocksDB.class));
    DifferSnapshotInfo snapshotInfo4 = new DifferSnapshotInfo(
        "/path/to/dbcp4", UUID.randomUUID(), 18000L, null, Mockito.mock(ManagedRocksDB.class));

    Map<String, String> prefixMap = ImmutableMap.of("col1", "c", "col2", "d");
    DifferSnapshotInfo snapshotInfo5 = new DifferSnapshotInfo(
        "/path/to/dbcp2", UUID.randomUUID(), 0L, prefixMap, Mockito.mock(ManagedRocksDB.class));
    DifferSnapshotInfo snapshotInfo6 = new DifferSnapshotInfo(
        "/path/to/dbcp2", UUID.randomUUID(), 100L, prefixMap, Mockito.mock(ManagedRocksDB.class));

    Set<String> snapshotSstFiles1 = ImmutableSet.of("000059", "000053");
    Set<String> snapshotSstFiles2 = ImmutableSet.of("000088", "000059",
        "000053", "000095");
    Set<String> snapshotSstFiles3 = ImmutableSet.of("000088", "000105",
        "000059", "000053", "000095");
    Set<String> snapshotSstFiles4 = ImmutableSet.of("000088", "000105",
        "000059", "000053", "000095", "000108");
    Set<String> snapshotSstFiles1Alt1 = ImmutableSet.of("000059", "000053",
        "000066");
    Set<String> snapshotSstFiles1Alt2 = ImmutableSet.of("000059", "000053",
        "000052");
    Set<String> snapshotSstFiles2Alt2 = ImmutableSet.of("000088", "000059",
        "000053", "000095", "000099");
    Set<String> snapshotSstFiles2Alt3 = ImmutableSet.of("000088", "000059",
        "000053", "000062");

    return Stream.of(
        Arguments.of("Test 1: Compaction log file regular case. " +
                " Expands expandable SSTs in the initial diff.",
            compactionLog,
            null,
            snapshotInfo3,
            snapshotInfo1,
            snapshotSstFiles3,
            snapshotSstFiles1,
            ImmutableSet.of("000059", "000053"),
            ImmutableSet.of("000066", "000105", "000080", "000087", "000073",
                "000095"),
            ImmutableSet.of("000066", "000105", "000080", "000087", "000073",
                "000095"),
            false, Collections.emptyMap()),
        Arguments.of("Test 2: Compaction log file crafted input: " +
                "One source ('to' snapshot) SST file is never compacted " +
                "(newly flushed)",
            compactionLog,
            null,
            snapshotInfo4,
            snapshotInfo3,
            snapshotSstFiles4,
            snapshotSstFiles3,
            ImmutableSet.of("000088", "000105", "000059", "000053", "000095"),
            ImmutableSet.of("000108"),
            ImmutableSet.of("000108"),
            false, Collections.emptyMap()),
        Arguments.of("Test 3: Compaction log file crafted input: " +
                "Same SST files found during SST expansion",
            compactionLog,
            null,
            snapshotInfo2,
            snapshotInfo1,
            snapshotSstFiles2,
            snapshotSstFiles1Alt1,
            ImmutableSet.of("000066", "000059", "000053"),
            ImmutableSet.of("000080", "000087", "000073", "000095"),
            ImmutableSet.of("000080", "000087", "000073", "000095"),
            false, Collections.emptyMap()),
        Arguments.of("Test 4: Compaction log file crafted input: " +
                "Skipping known processed SST.",
            compactionLog,
            null,
            snapshotInfo2,
            snapshotInfo1,
            snapshotSstFiles2Alt2,
            snapshotSstFiles1Alt2,
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            true, Collections.emptyMap()),
        Arguments.of("Test 5: Compaction log file hit snapshot" +
                " generation early exit condition",
            compactionLog,
            null,
            snapshotInfo2,
            snapshotInfo1,
            snapshotSstFiles2Alt3,
            snapshotSstFiles1,
            ImmutableSet.of("000059", "000053"),
            ImmutableSet.of("000066", "000080", "000087", "000073", "000062"),
            ImmutableSet.of("000066", "000080", "000087", "000073", "000062"),
            false, Collections.emptyMap()),
        Arguments.of("Test 6: Compaction log table regular case. " +
                "Expands expandable SSTs in the initial diff.",
            null,
            compactionLogEntries,
            snapshotInfo3,
            snapshotInfo1,
            snapshotSstFiles3,
            snapshotSstFiles1,
            ImmutableSet.of("000059", "000053"),
            ImmutableSet.of("000066", "000105", "000080", "000087", "000073",
                "000095"),
            ImmutableSet.of("000066", "000105", "000080", "000087", "000073",
                "000095"),
            false, Collections.emptyMap()),
        Arguments.of("Test 7: Compaction log table crafted input: " +
                "One source ('to' snapshot) SST file is never compacted " +
                "(newly flushed)",
            null,
            compactionLogEntries,
            snapshotInfo4,
            snapshotInfo3,
            snapshotSstFiles4,
            snapshotSstFiles3,
            ImmutableSet.of("000088", "000105", "000059", "000053", "000095"),
            ImmutableSet.of("000108"),
            ImmutableSet.of("000108"),
            false, Collections.emptyMap()),
        Arguments.of("Test 8: Compaction log table crafted input: " +
                "Same SST files found during SST expansion",
            null,
            compactionLogEntries,
            snapshotInfo2,
            snapshotInfo1,
            snapshotSstFiles2,
            snapshotSstFiles1Alt1,
            ImmutableSet.of("000066", "000059", "000053"),
            ImmutableSet.of("000080", "000087", "000073", "000095"),
            ImmutableSet.of("000080", "000087", "000073", "000095"),
            false, Collections.emptyMap()),
        Arguments.of("Test 9: Compaction log table crafted input: " +
                "Skipping known processed SST.",
            null,
            compactionLogEntries,
            snapshotInfo2,
            snapshotInfo1,
            snapshotSstFiles2Alt2,
            snapshotSstFiles1Alt2,
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            true, Collections.emptyMap()),
        Arguments.of("Test 10: Compaction log table hit snapshot " +
                "generation early exit condition",
            null,
            compactionLogEntries,
            snapshotInfo2,
            snapshotInfo1,
            snapshotSstFiles2Alt3,
            snapshotSstFiles1,
            ImmutableSet.of("000059", "000053"),
            ImmutableSet.of("000066", "000080", "000087", "000073", "000062"),
            ImmutableSet.of("000066", "000080", "000087", "000073", "000062"),
            false, Collections.emptyMap()),
        Arguments.of("Test 11: Older Compaction log got pruned and source snapshot delta files would be " +
                "unreachable",
            null,
            getPrunedCompactionEntries(false, Collections.emptyMap()),
            snapshotInfo6,
            snapshotInfo5,
            ImmutableSet.of("10", "11", "8", "9", "12"),
            ImmutableSet.of("1", "3", "13", "14"),
            ImmutableSet.of("1", "3", "13", "14"),
            ImmutableSet.of("2", "8", "9", "12"),
            ImmutableSet.of("2", "8", "9", "12"),
            false, Collections.emptyMap()),
        Arguments.of("Test 12: Older Compaction log got pruned and source snapshot delta files would be " +
                "unreachable",
            null,
            getPrunedCompactionEntries(true, Collections.emptyMap()),
            snapshotInfo6,
            snapshotInfo5,
            ImmutableSet.of("10", "11", "8", "9", "12"),
            ImmutableSet.of("1", "3", "13", "14"),
            ImmutableSet.of("3", "13", "14"),
            ImmutableSet.of("4", "5", "8", "9", "12"),
            null,
            false, Collections.emptyMap()),
        Arguments.of("Test 13: Compaction log to test filtering logic based on range and column family",
            null,
            getPrunedCompactionEntries(false,
                new HashMap<String, String[]>() {{
                  put("1", new String[]{"a", "c", "col1"});
                  put("3", new String[]{"a", "d", "col2"});
                  put("13", new String[]{"a", "c", "col13"});
                  put("14", new String[]{"a", "c", "col1"});
                  put("2", new String[]{"a", "c", "col1"});
                  put("4", new String[]{"a", "b", "col1"});
                  put("5", new String[]{"b", "b", "col1"});
                  put("10", new String[]{"a", "b", "col1"});
                  put("8", new String[]{"a", "b", "col1"});
                  put("6", new String[]{"a", "z", "col13"});
                  put("7", new String[]{"a", "z", "col13"});
                }}),
            snapshotInfo6,
            snapshotInfo5,
            ImmutableSet.of("10", "11", "8", "9", "12", "15"),
            ImmutableSet.of("1", "3", "13", "14"),
            ImmutableSet.of("1", "13", "3", "14"),
            ImmutableSet.of("2", "8", "9", "12", "15"),
            ImmutableSet.of("2", "9", "12"),
            false,
            ImmutableMap.of(
                "2", new String[]{"a", "b", "col1"},
                "12", new String[]{"a", "d", "col2"},
                "8", new String[]{"a", "b", "col1"},
                "9", new String[]{"a", "c", "col1"},
                "15", new String[]{"a", "z", "col13"}
            ))

    );
  }

  /**
   * Tests core SST diff list logic. Does not involve DB.
   * Focuses on testing edge cases in internalGetSSTDiffList().
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("casesGetSSTDiffListWithoutDB")
  @SuppressWarnings("parameternumber")
  public void testGetSSTDiffListWithoutDB(String description,
      String compactionLog,
      List<CompactionLogEntry> compactionLogEntries,
      DifferSnapshotInfo srcSnapshot,
      DifferSnapshotInfo destSnapshot,
      Set<String> srcSnapshotSstFiles,
      Set<String> destSnapshotSstFiles,
      Set<String> expectedSameSstFiles,
      Set<String> expectedDiffSstFiles,
      Set<String> expectedSSTDiffFiles,
      boolean expectingException,
      Map<String, String[]> metaDataMap) {
    try (MockedStatic<RocksDiffUtils> mockedRocksdiffUtil = Mockito.mockStatic(RocksDiffUtils.class,
        Mockito.CALLS_REAL_METHODS)) {
      mockedRocksdiffUtil.when(() -> RocksDiffUtils.constructBucketKey(anyString())).thenAnswer(i -> i.getArgument(0));
      boolean exceptionThrown = false;
      if (compactionLog != null) {
        // Construct DAG from compaction log input
        Arrays.stream(compactionLog.split("\n")).forEach(
            rocksDBCheckpointDiffer::processCompactionLogLine);
      } else if (compactionLogEntries != null) {
        compactionLogEntries.forEach(entry ->
            rocksDBCheckpointDiffer.addToCompactionLogTable(entry));
      } else {
        throw new IllegalArgumentException("One of compactionLog and " +
            "compactionLogEntries should be non-null.");
      }
      rocksDBCheckpointDiffer.loadAllCompactionLogs();

      Set<String> actualSameSstFiles = new HashSet<>();
      Set<String> actualDiffSstFiles = new HashSet<>();

      try {
        rocksDBCheckpointDiffer.internalGetSSTDiffList(
            srcSnapshot,
            destSnapshot,
            srcSnapshotSstFiles,
            destSnapshotSstFiles,
            actualSameSstFiles,
            actualDiffSstFiles);
      } catch (RuntimeException rtEx) {
        if (!expectingException) {
          fail("Unexpected exception thrown in test.");
        } else {
          exceptionThrown = true;
        }
      }

      if (expectingException && !exceptionThrown) {
        fail("Expecting exception but none thrown.");
      }

      // Check same and different SST files result
      assertEquals(expectedSameSstFiles, actualSameSstFiles);
      assertEquals(expectedDiffSstFiles, actualDiffSstFiles);
      try (MockedStatic<RdbUtil> mockedHandler = Mockito.mockStatic(RdbUtil.class, Mockito.CALLS_REAL_METHODS)) {
        RocksDB rocksDB = Mockito.mock(RocksDB.class);
        Mockito.when(rocksDB.getName()).thenReturn("dummy");
        Mockito.when(srcSnapshot.getRocksDB().get()).thenReturn(rocksDB);
        Mockito.when(destSnapshot.getRocksDB().get()).thenReturn(rocksDB);
        Mockito.when(srcSnapshot.getRocksDB().getLiveMetadataForSSTFiles())
            .thenAnswer(invocation -> srcSnapshotSstFiles.stream().filter(metaDataMap::containsKey).map(file -> {
              LiveFileMetaData liveFileMetaData = Mockito.mock(LiveFileMetaData.class);
              String[] metaData = metaDataMap.get(file);
              Mockito.when(liveFileMetaData.fileName()).thenReturn("/" + file + SST_FILE_EXTENSION);
              Mockito.when(liveFileMetaData.smallestKey()).thenReturn(metaData[0].getBytes(UTF_8));
              Mockito.when(liveFileMetaData.largestKey()).thenReturn(metaData[1].getBytes(UTF_8));
              Mockito.when(liveFileMetaData.columnFamilyName()).thenReturn(metaData[2].getBytes(UTF_8));
              return liveFileMetaData;
            }).collect(Collectors.toMap(liveFileMetaData -> FilenameUtils.getBaseName(liveFileMetaData.fileName()),
                Function.identity())));
        mockedHandler.when(() -> RdbUtil.getLiveSSTFilesForCFs(any(), any()))
            .thenAnswer(i -> {
              Set<String> sstFiles = i.getArgument(0).equals(srcSnapshot.getRocksDB()) ? srcSnapshotSstFiles
                  : destSnapshotSstFiles;
              return sstFiles.stream().map(fileName -> {
                LiveFileMetaData liveFileMetaData = Mockito.mock(LiveFileMetaData.class);
                Mockito.when(liveFileMetaData.fileName()).thenReturn("/" + fileName + SST_FILE_EXTENSION);
                return liveFileMetaData;
              }).collect(Collectors.toList());
            });
        try {
          Assertions.assertEquals(Optional.ofNullable(expectedSSTDiffFiles)
                  .map(files -> files.stream().sorted().collect(Collectors.toList())).orElse(null),
              rocksDBCheckpointDiffer.getSSTDiffList(srcSnapshot, destSnapshot)
                  .map(i -> i.stream().sorted().collect(Collectors.toList())).orElse(null));
        } catch (RuntimeException rtEx) {
          if (!expectingException) {
            fail("Unexpected exception thrown in test.");
          } else {
            exceptionThrown = true;
          }
        }
      }
      if (expectingException && !exceptionThrown) {
        fail("Expecting exception but none thrown.");
      }
    }
  }

  /**
   * Tests DB listener (compaction log generation, SST backup),
   * SST file list diff.
   * <p>
   * Does actual DB write, flush, compaction.
   */
  @Test
  void testDifferWithDB() throws Exception {
    writeKeysAndCheckpointing();
    readRocksDBInstance(ACTIVE_DB_DIR_NAME, activeRocksDB, null,
        rocksDBCheckpointDiffer);

    if (LOG.isDebugEnabled()) {
      printAllSnapshots();
    }

    traverseGraph(rocksDBCheckpointDiffer.getCompactionNodeMap(),
        rocksDBCheckpointDiffer.getBackwardCompactionDAG(),
        rocksDBCheckpointDiffer.getForwardCompactionDAG());

    diffAllSnapshots(rocksDBCheckpointDiffer);

    // Confirm correct links created
    try (Stream<Path> sstPathStream = Files.list(sstBackUpDir.toPath())) {
      List<String> expectedLinks = sstPathStream.map(Path::getFileName)
              .map(Object::toString).sorted().collect(Collectors.toList());
      assertEquals(expectedLinks, asList(
              "000017.sst", "000019.sst", "000021.sst", "000023.sst",
          "000024.sst", "000026.sst", "000029.sst"));
    }
    rocksDBCheckpointDiffer.getForwardCompactionDAG().nodes().stream().forEach(compactionNode -> {
      Assertions.assertNotNull(compactionNode.getStartKey());
      Assertions.assertNotNull(compactionNode.getEndKey());
    });
    GenericTestUtils.waitFor(() -> rocksDBCheckpointDiffer.getInflightCompactions().isEmpty(), 1000,
        10000);
    if (LOG.isDebugEnabled()) {
      rocksDBCheckpointDiffer.dumpCompactionNodeTable();
    }

    cleanUpSnapshots();
  }

  public void cleanUpSnapshots() {
    for (DifferSnapshotInfo snap : snapshots) {
      snap.getRocksDB().close();
    }
    for (List<ColumnFamilyHandle> colHandle : colHandles) {
      for (ColumnFamilyHandle handle : colHandle) {
        handle.close();
      }
    }
  }

  private static List<ColumnFamilyDescriptor> getColumnFamilyDescriptors() {
    return Stream.of("fileTable", "directoryTable", "keyTable", "default")
        .map(StringUtils::string2Bytes)
        .map(ColumnFamilyDescriptor::new).collect(Collectors.toList());
  }

  /**
   * Test SST differ.
   */
  void diffAllSnapshots(RocksDBCheckpointDiffer differ)
      throws IOException {
    final DifferSnapshotInfo src = snapshots.get(snapshots.size() - 1);

    // Hard-coded expected output.
    // The results are deterministic. Retrieved from a successful run.
    final List<List<String>> expectedDifferResult = asList(
        asList("000023", "000029", "000026", "000019", "000021", "000031"),
        asList("000023", "000029", "000026", "000021", "000031"),
        asList("000023", "000029", "000026", "000031"),
        asList("000029", "000026", "000031"),
        asList("000029", "000031"),
        Collections.singletonList("000031"),
        Collections.emptyList()
    );
    assertEquals(snapshots.size(), expectedDifferResult.size());

    int index = 0;
    for (DifferSnapshotInfo snap : snapshots) {
      // Returns a list of SST files to be fed into RocksDiff
      List<String> sstDiffList = differ.getSSTDiffList(src, snap).orElse(Collections.emptyList());
      LOG.info("SST diff list from '{}' to '{}': {}",
          src.getDbPath(), snap.getDbPath(), sstDiffList);

      assertEquals(expectedDifferResult.get(index), sstDiffList);
      ++index;
    }
  }

  /**
   * Helper function that creates an RDB checkpoint (= Ozone snapshot).
   */
  private void createCheckpoint(ManagedRocksDB rocksDB) throws RocksDBException {

    LOG.trace("Current time: " + System.currentTimeMillis());
    long t1 = Time.monotonicNow();

    final long snapshotGeneration = rocksDB.get().getLatestSequenceNumber();
    final String cpPath = CP_PATH_PREFIX + snapshotGeneration;

    // Delete the checkpoint dir if it already exists for the test
    File dir = new File(cpPath);
    if (dir.exists()) {
      deleteDirectory(dir);
    }
    cpDirList.add(dir);

    createCheckPoint(ACTIVE_DB_DIR_NAME, cpPath, rocksDB);
    final UUID snapshotId = UUID.randomUUID();
    List<ColumnFamilyHandle> colHandle = new ArrayList<>();
    colHandles.add(colHandle);
    final DifferSnapshotInfo currentSnapshot =
        new DifferSnapshotInfo(cpPath, snapshotId, snapshotGeneration, null,
            ManagedRocksDB.openReadOnly(cpPath, getColumnFamilyDescriptors(),
                colHandle));
    this.snapshots.add(currentSnapshot);

    long t2 = Time.monotonicNow();
    LOG.trace("Current time: " + t2);
    LOG.debug("Time elapsed: " + (t2 - t1) + " ms");
  }

  // Flushes the WAL and Creates a RocksDB checkpoint
  void createCheckPoint(String dbPathArg, String cpPathArg, ManagedRocksDB rocksDB) {
    LOG.debug("Creating RocksDB '{}' checkpoint at '{}'", dbPathArg, cpPathArg);
    try (ManagedFlushOptions flushOptions = new ManagedFlushOptions()) {
      rocksDB.get().flush(flushOptions);
      ManagedCheckpoint cp = ManagedCheckpoint.create(rocksDB);
      cp.get().createCheckpoint(cpPathArg);
    } catch (RocksDBException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  void printAllSnapshots() {
    for (DifferSnapshotInfo snap : snapshots) {
      LOG.debug("{}", snap);
    }
  }

  /**
   * Get a list of relevant column family descriptors.
   * @param cfOpts ColumnFamilyOptions
   * @return List of ColumnFamilyDescriptor
   */
  static List<ColumnFamilyDescriptor> getCFDescriptorList(
      ManagedColumnFamilyOptions cfOpts) {
    return asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
        new ColumnFamilyDescriptor("keyTable".getBytes(UTF_8), cfOpts),
        new ColumnFamilyDescriptor("directoryTable".getBytes(UTF_8), cfOpts),
        new ColumnFamilyDescriptor("fileTable".getBytes(UTF_8), cfOpts),
        new ColumnFamilyDescriptor("compactionLogTable".getBytes(UTF_8), cfOpts)
    );
  }

  private void writeKeysAndCheckpointing() throws RocksDBException {
    for (int i = 0; i < NUM_ROW; ++i) {
      String generatedString = RandomStringUtils.secure().nextAlphabetic(7);
      String keyStr = "Key-" + i + "-" + generatedString;
      String valueStr = "Val-" + i + "-" + generatedString;
      byte[] key = keyStr.getBytes(UTF_8);
      // Put entry in keyTable
      activeRocksDB.get().put(keyTableCFHandle, key, valueStr.getBytes(UTF_8));
      if (i % SNAPSHOT_EVERY_SO_MANY_KEYS == 0) {
        createCheckpoint(activeRocksDB);
      }
    }
    createCheckpoint(activeRocksDB);
  }

  private boolean deleteDirectory(File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        if (!deleteDirectory(file)) {
          return false;
        }
      }
    }
    return directoryToBeDeleted.delete();
  }

  public List<ColumnFamilyDescriptor> getColumnFamilyDescriptors(String dbPath) throws RocksDBException {
    try (ManagedOptions emptyOptions = new ManagedOptions()) {
      List<byte[]> cfList = RocksDB.listColumnFamilies(emptyOptions, dbPath);
      return  cfList.stream().map(ColumnFamilyDescriptor::new).collect(Collectors.toList());
    }
  }

  // Read from a given RocksDB instance and optionally write all the
  // keys to a given file.
  private void readRocksDBInstance(String dbPathArg,
                                   ManagedRocksDB rocksDB,
                                   FileWriter file,
                                   RocksDBCheckpointDiffer differ) {

    LOG.debug("Reading RocksDB: " + dbPathArg);
    boolean createdDB = false;

    try (ManagedDBOptions dbOptions = new ManagedDBOptions()) {
      List<ColumnFamilyDescriptor> cfDescriptors = getColumnFamilyDescriptors(dbPathArg);
      List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
      if (rocksDB == null) {
        rocksDB = ManagedRocksDB.openReadOnly(dbOptions, dbPathArg, cfDescriptors, cfHandles);
        createdDB = true;
      }

      List<LiveFileMetaData> liveFileMetaDataList = rocksDB.get().getLiveFilesMetaData();
      for (LiveFileMetaData m : liveFileMetaDataList) {
        LOG.debug("SST File: {}. ", m.fileName());
        LOG.debug("\tLevel: {}", m.level());
        LOG.debug("\tTable: {}", bytes2String(m.columnFamilyName()));
        LOG.debug("\tKey Range: {}", bytes2String(m.smallestKey()) + " <-> " + bytes2String(m.largestKey()));
        if (debugEnabled(DEBUG_DAG_LIVE_NODES)) {
          printMutableGraphFromAGivenNode(
              differ.getCompactionNodeMap(),
              m.fileName(), m.level(),
              differ.getForwardCompactionDAG());
        }
      }

      if (debugEnabled(DEBUG_READ_ALL_DB_KEYS)) {
        try (ManagedRocksIterator iter = new ManagedRocksIterator(rocksDB.get().newIterator())) {
          for (iter.get().seekToFirst(); iter.get().isValid(); iter.get().next()) {
            LOG.debug(
                "Iterator key:" + bytes2String(iter.get().key()) + ", iter value:" + bytes2String(iter.get().value()));
            if (file != null) {
              file.write("iterator key:" + bytes2String(iter.get().key()) + ", iter value:" +
                  bytes2String(iter.get().value()));
              file.write("\n");
            }
          }
        }
      }
    } catch (IOException | RocksDBException e) {
      LOG.error("Caught exception while reading from rocksDB.", e);
    } finally {
      if (createdDB) {
        rocksDB.close();
      }
    }
  }

  public boolean debugEnabled(Integer level) {
    return DEBUG_LEVEL.contains(level);
  }

  /**
   * Helper that traverses the graphs for testing.
   * @param compactionNodeMap
   * @param reverseMutableGraph
   * @param fwdMutableGraph
   */
  private void traverseGraph(
      ConcurrentMap<String, CompactionNode> compactionNodeMap,
      MutableGraph<CompactionNode> reverseMutableGraph,
      MutableGraph<CompactionNode> fwdMutableGraph) {

    List<CompactionNode> nodeList = compactionNodeMap.values().stream()
        .sorted(new NodeComparator()).collect(Collectors.toList());

    for (CompactionNode infileNode : nodeList) {
      // fist go through fwdGraph to find nodes that don't have successors.
      // These nodes will be the top level nodes in reverse graph
      Set<CompactionNode> successors = fwdMutableGraph.successors(infileNode);
      if (successors.isEmpty()) {
        LOG.debug("No successors. Cumulative keys: {}, total keys: {}",
            infileNode.getCumulativeKeysReverseTraversal(),
            infileNode.getTotalNumberOfKeys());
        infileNode.setCumulativeKeysReverseTraversal(
            infileNode.getTotalNumberOfKeys());
      }
    }

    Set<CompactionNode> visited = new HashSet<>();

    for (CompactionNode infileNode : nodeList) {
      if (visited.contains(infileNode)) {
        continue;
      }
      visited.add(infileNode);
      LOG.debug("Visiting node '{}'", infileNode.getFileName());
      Set<CompactionNode> currentLevel = new HashSet<>();
      currentLevel.add(infileNode);
      int level = 1;
      while (!currentLevel.isEmpty()) {
        LOG.debug("BFS Level: {}. Current level has {} nodes",
            level++, currentLevel.size());
        final Set<CompactionNode> nextLevel = new HashSet<>();
        for (CompactionNode current : currentLevel) {
          LOG.debug("Expanding node: {}", current.getFileName());
          Set<CompactionNode> successors =
              reverseMutableGraph.successors(current);
          if (successors.isEmpty()) {
            LOG.debug("No successors. Cumulative keys: {}",
                current.getCumulativeKeysReverseTraversal());
            continue;
          }
          for (CompactionNode node : successors) {
            LOG.debug("Adding to the next level: {}", node.getFileName());
            LOG.debug("'{}' cumulative keys: {}. parent '{}' total keys: {}",
                node.getFileName(), node.getCumulativeKeysReverseTraversal(),
                current.getFileName(), current.getTotalNumberOfKeys());
            node.addCumulativeKeysReverseTraversal(
                current.getCumulativeKeysReverseTraversal());
            nextLevel.add(node);
          }
        }
        currentLevel = nextLevel;
      }
    }
  }

  private void printMutableGraphFromAGivenNode(
      ConcurrentMap<String, CompactionNode> compactionNodeMap,
      String fileName,
      int sstLevel,
      MutableGraph<CompactionNode> mutableGraph) {

    CompactionNode infileNode = compactionNodeMap.get(fileName);
    if (infileNode == null) {
      return;
    }
    LOG.debug("Expanding file: {}. SST compaction level: {}",
        fileName, sstLevel);
    Set<CompactionNode> currentLevel = new HashSet<>();
    currentLevel.add(infileNode);
    int levelCounter = 1;
    while (!currentLevel.isEmpty()) {
      LOG.debug("DAG Level: {}", levelCounter++);
      final Set<CompactionNode> nextLevel = new HashSet<>();
      StringBuilder sb = new StringBuilder();
      for (CompactionNode current : currentLevel) {
        Set<CompactionNode> successors = mutableGraph.successors(current);
        for (CompactionNode succNode : successors) {
          sb.append(succNode.getFileName()).append(' ');
          nextLevel.add(succNode);
        }
      }
      LOG.debug("{}", sb);
      currentLevel = nextLevel;
    }
  }

  /**
   * Creates a backward compaction DAG from a list of level nodes.
   * It assumes that at each level files get compacted to the half of number
   * of files at the next level.
   * e.g. if level-1 has 7 files and level-2 has 9 files, so first 4 files
   * at level-2 are from compaction of level-1 and rests are new.
   */
  private static MutableGraph<CompactionNode> createBackwardDagFromLevelNodes(
      int fromLevel,
      int toLevel
  ) {
    MutableGraph<CompactionNode> dag  = GraphBuilder.directed().build();

    if (fromLevel == toLevel) {
      COMPACTION_NODES_BY_LEVEL.get(fromLevel).forEach(dag::addNode);
      return dag;
    }

    for (int level = fromLevel; level < toLevel; level++) {
      List<CompactionNode> currentLevel = COMPACTION_NODES_BY_LEVEL.get(level);
      List<CompactionNode> nextLevel = COMPACTION_NODES_BY_LEVEL.get(level + 1);

      for (CompactionNode compactionNode : currentLevel) {
        for (int j = 0; j < nextLevel.size(); j++) {
          dag.addNode(compactionNode);
          dag.addNode(nextLevel.get(j));

          int child = nextLevel.size();
          if (level < COMPACTION_NODES_BY_LEVEL.size() - 2) {
            child /= 2;
          }

          if (j < child) {
            dag.putEdge(compactionNode, nextLevel.get(j));
          }
        }
      }
    }

    return dag;
  }

  /**
   * Creates a forward compaction DAG from a list of level nodes.
   * It assumes that at each level first half of the files are from the
   * compaction of the previous level.
   * e.g. if level-1 has 7 files and level-2 has 9 files, so first 4 files
   * at level-2 are from compaction of level-1 and rests are new.
   */
  private static MutableGraph<CompactionNode> createForwardDagFromLevelNodes(
      int fromLevel,
      int toLevel
  ) {
    MutableGraph<CompactionNode> dag  = GraphBuilder.directed().build();

    if (fromLevel == toLevel) {
      COMPACTION_NODES_BY_LEVEL.get(fromLevel).forEach(dag::addNode);
      return dag;
    }

    dag  = GraphBuilder.directed().build();
    for (int level = fromLevel; level > toLevel; level--) {
      List<CompactionNode> currentLevel = COMPACTION_NODES_BY_LEVEL.get(level);
      List<CompactionNode> nextLevel = COMPACTION_NODES_BY_LEVEL.get(level - 1);

      for (int i = 0; i < currentLevel.size(); i++) {
        for (CompactionNode compactionNode : nextLevel) {
          dag.addNode(currentLevel.get(i));
          dag.addNode(compactionNode);

          int parent = currentLevel.size();
          if (level < COMPACTION_NODES_BY_LEVEL.size() - 1) {
            parent /= 2;
          }

          if (i < parent) {
            dag.putEdge(currentLevel.get(i), compactionNode);
          }
        }
      }
    }

    return dag;
  }

  /**
   * Test cases for pruneBackwardDag.
   */
  private static Stream<Arguments> pruneBackwardDagScenarios() {
    Set<String> level0Files = new HashSet<>(SST_FILES_BY_LEVEL.get(0));
    Set<String> level1Files = new HashSet<>(SST_FILES_BY_LEVEL.get(1));
    Set<String> level2Files = new HashSet<>(SST_FILES_BY_LEVEL.get(2));
    Set<String> level3Files = new HashSet<>(SST_FILES_BY_LEVEL.get(3));

    level1Files.addAll(level0Files);
    level2Files.addAll(level1Files);
    level3Files.addAll(level2Files);

    return Stream.of(
        Arguments.of("Remove level 0 from backward DAG",
            createBackwardDagFromLevelNodes(0, 4),
            new HashSet<>(COMPACTION_NODES_BY_LEVEL.get(0)),
            createBackwardDagFromLevelNodes(1, 4),
            level0Files
        ),
        Arguments.of("Remove level 1 from backward DAG",
            createBackwardDagFromLevelNodes(0, 4),
            new HashSet<>(COMPACTION_NODES_BY_LEVEL.get(1)),
            createBackwardDagFromLevelNodes(2, 4),
            level1Files
        ),
        Arguments.of("Remove level 2 from backward DAG",
            createBackwardDagFromLevelNodes(0, 4),
            new HashSet<>(COMPACTION_NODES_BY_LEVEL.get(2)),
            createBackwardDagFromLevelNodes(3, 4),
            level2Files
        ),
        Arguments.of("Remove level 3 from backward DAG",
            createBackwardDagFromLevelNodes(0, 4),
            new HashSet<>(COMPACTION_NODES_BY_LEVEL.get(3)),
            createBackwardDagFromLevelNodes(4, 4),
            level3Files
        )
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("pruneBackwardDagScenarios")
  public void testPruneBackwardDag(String description,
                                   MutableGraph<CompactionNode> originalDag,
                                   Set<CompactionNode> levelToBeRemoved,
                                   MutableGraph<CompactionNode> expectedDag,
                                   Set<String> expectedFileNodesRemoved) {
    Set<String> actualFileNodesRemoved =
        rocksDBCheckpointDiffer.pruneBackwardDag(originalDag, levelToBeRemoved);
    assertEquals(expectedDag, originalDag);
    assertEquals(actualFileNodesRemoved, expectedFileNodesRemoved);
  }

  /**
   * Test cases for pruneBackwardDag.
   */
  private static Stream<Arguments> pruneForwardDagScenarios() {
    Set<String> level0Files = new HashSet<>(SST_FILES_BY_LEVEL.get(0));
    Set<String> level1Files = new HashSet<>(SST_FILES_BY_LEVEL.get(1));
    Set<String> level2Files = new HashSet<>(SST_FILES_BY_LEVEL.get(2));
    Set<String> level3Files = new HashSet<>(SST_FILES_BY_LEVEL.get(3));

    level1Files.addAll(level0Files);
    level2Files.addAll(level1Files);
    level3Files.addAll(level2Files);

    return Stream.of(
        Arguments.of("Remove level 0 from forward DAG",
            createForwardDagFromLevelNodes(4, 0),
            new HashSet<>(COMPACTION_NODES_BY_LEVEL.get(0)),
            createForwardDagFromLevelNodes(4, 1),
            level0Files
        ),
        Arguments.of("Remove level 1 from forward DAG",
            createForwardDagFromLevelNodes(4, 0),
            new HashSet<>(COMPACTION_NODES_BY_LEVEL.get(1)),
            createForwardDagFromLevelNodes(4, 2),
            level1Files
        ),
        Arguments.of("Remove level 2 from forward DAG",
            createForwardDagFromLevelNodes(4, 0),
            new HashSet<>(COMPACTION_NODES_BY_LEVEL.get(2)),
            createForwardDagFromLevelNodes(4, 3),
            level2Files
        ),
        Arguments.of("Remove level 3 from forward DAG",
            createForwardDagFromLevelNodes(4, 0),
            new HashSet<>(COMPACTION_NODES_BY_LEVEL.get(3)),
            createForwardDagFromLevelNodes(4, 4),
            level3Files
        )
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("pruneForwardDagScenarios")
  public void testPruneForwardDag(String description,
                                  MutableGraph<CompactionNode> originalDag,
                                  Set<CompactionNode> levelToBeRemoved,
                                  MutableGraph<CompactionNode> expectedDag,
                                  Set<String> expectedFileNodesRemoved) {
    Set<String> actualFileNodesRemoved =
        rocksDBCheckpointDiffer.pruneForwardDag(originalDag, levelToBeRemoved);
    assertEquals(expectedDag, originalDag);
    assertEquals(actualFileNodesRemoved, expectedFileNodesRemoved);
  }

  @SuppressWarnings("methodlength")
  private static Stream<Arguments> compactionDagPruningScenarios() {
    long currentTimeMillis = System.currentTimeMillis();

    String compactionLogFile0 = "S 1000 snapshotId0 " +
        (currentTimeMillis - MINUTES.toMillis(30)) + " \n";
    String compactionLogFile1 = "C 1500 000015,000013,000011,000009:000018," +
        "000016,000017\n"
        + "S 2000 snapshotId1 " +
        (currentTimeMillis - MINUTES.toMillis(24)) + " \n";

    String compactionLogFile2 = "C 2500 000018,000016,000017,000026,000024," +
        "000022,000020:000027,000030,000028,000031,000029\n"
        + "S 3000 snapshotId2 " +
        (currentTimeMillis - MINUTES.toMillis(18)) + " \n";

    String compactionLogFile3 = "C 3500 000027,000030,000028,000031,000029," +
        "000039,000037,000035,000033:000040,000044,000042,000043,000046," +
        "000041,000045\n"
        + "S 4000 snapshotId3 " +
        (currentTimeMillis - MINUTES.toMillis(12)) + " \n";

    String compactionLogFile4 = "C 4500 000040,000044,000042,000043,000046," +
        "000041,000045,000054,000052,000050,000048:000059,000055,000056," +
        "000060,000057,000058\n"
        + "S 5000 snapshotId4 " +
        (currentTimeMillis - MINUTES.toMillis(6)) + " \n";

    String compactionLogFileWithoutSnapshot1 = "C 1500 000015,000013,000011," +
        "000009:000018,000016,000017\n" +
        "C 2000 000018,000016,000017,000026,000024,000022,000020" +
        ":000027,000030,000028,000031,000029\n";

    String compactionLogFileWithoutSnapshot2 = "C 4500 000040,000044,000042," +
        "000043,000046,000041,000045,000054,000052,000050,000048:000059," +
        "000055,000056,000060,000057,000058\n";

    String compactionLogFileOnlyWithSnapshot1 =
        "S 3000 snapshotIdWithoutCompaction1 " +
            (currentTimeMillis - MINUTES.toMillis(18)) + " \n";

    String compactionLogFileOnlyWithSnapshot2 =
        "S 3000 snapshotIdWithoutCompaction2 " +
            (currentTimeMillis - MINUTES.toMillis(15)) + " \n";

    String compactionLogFileOnlyWithSnapshot3 =
        "S 3000 snapshotIdWithoutCompaction3 " +
            (currentTimeMillis - MINUTES.toMillis(12)) + " \n";

    String compactionLogFileOnlyWithSnapshot4 =
        "S 3000 snapshotIdWithoutCompaction4 " +
            (currentTimeMillis - MINUTES.toMillis(9)) + " \n";

    String compactionLogFileOnlyWithSnapshot5 =
        "S 3000 snapshotIdWithoutCompaction5 " +
            (currentTimeMillis - MINUTES.toMillis(6)) + " \n";

    String compactionLogFileOnlyWithSnapshot6 =
        "S 3000 snapshotIdWithoutCompaction6 " +
            (currentTimeMillis - MINUTES.toMillis(3)) + " \n";

    Set<String> expectedNodes = ImmutableSet.of("000059", "000055", "000056",
        "000060", "000057", "000058");

    return Stream.of(
        Arguments.of("Each compaction log file has only one snapshot and one" +
                " compaction statement except first log file.",
            Arrays.asList(compactionLogFile0, compactionLogFile1,
                compactionLogFile2, compactionLogFile3, compactionLogFile4),
            null,
            expectedNodes,
            4,
            0
        ),
        Arguments.of("Compaction log doesn't have snapshot  because OM" +
                " restarted. Restart happened before snapshot to be deleted.",
            Arrays.asList(compactionLogFile0,
                compactionLogFileWithoutSnapshot1,
                compactionLogFile3,
                compactionLogFile4),
            null,
            expectedNodes,
            4,
            0
        ),
        Arguments.of("Compaction log doesn't have snapshot because OM" +
                " restarted. Restart happened after snapshot to be deleted.",
            Arrays.asList(compactionLogFile0, compactionLogFile1,
                compactionLogFile2, compactionLogFile3,
                compactionLogFileWithoutSnapshot2,
                compactionLogFileOnlyWithSnapshot4),
            null,
            expectedNodes,
            4,
            0
        ),
        Arguments.of("No compaction happened in between two snapshots.",
            Arrays.asList(compactionLogFile0, compactionLogFile1,
                compactionLogFile2, compactionLogFile3,
                compactionLogFileOnlyWithSnapshot1,
                compactionLogFileOnlyWithSnapshot2, compactionLogFile4),
            null,
            expectedNodes,
            4,
            0
        ),
        Arguments.of("Only contains snapshots but no compaction.",
            Arrays.asList(compactionLogFileOnlyWithSnapshot1,
                compactionLogFileOnlyWithSnapshot2,
                compactionLogFileOnlyWithSnapshot3,
                compactionLogFileOnlyWithSnapshot4,
                compactionLogFileOnlyWithSnapshot5,
                compactionLogFileOnlyWithSnapshot6),
            null,
            Collections.emptySet(),
            0,
            0
        ),
        Arguments.of("No file exists because compaction has not happened" +
                " and snapshot is not taken.",
            Collections.emptyList(),
            null,
            Collections.emptySet(),
            0,
            0
        ),
        Arguments.of("When compaction table is used case 1.",
            null,
            asList(createCompactionEntry(1500,
                    (currentTimeMillis - MINUTES.toMillis(24)),
                    asList("000015", "000013", "000011", "000009"),
                    asList("000018", "000016", "000017")),
                createCompactionEntry(2500,
                    (currentTimeMillis - MINUTES.toMillis(20)),
                    asList("000018", "000016", "000017", "000026", "000024",
                        "000022", "000020"),
                    asList("000027", "000030", "000028", "000031", "000029")),
                createCompactionEntry(3500,
                    (currentTimeMillis - MINUTES.toMillis(16)),
                    asList("000027", "000030", "000028", "000031", "000029",
                        "000039", "000037", "000035", "000033"),
                    asList("000040", "000044", "000042", "000043", "000046",
                        "000041", "000045")),
                createCompactionEntry(4500,
                    (currentTimeMillis - MINUTES.toMillis(12)),
                    asList("000040", "000044", "000042", "000043", "000046",
                        "000041", "000045", "000054", "000052", "000050",
                        "000048"),
                    asList("000059", "000055", "000056", "000060", "000057",
                        "000058"))),
            expectedNodes,
            4,
            0
        ),
        Arguments.of("When compaction table is used case 2.",
            null,
            asList(createCompactionEntry(1500,
                    (currentTimeMillis - MINUTES.toMillis(24)),
                    asList("000015", "000013", "000011", "000009"),
                    asList("000018", "000016", "000017")),
                createCompactionEntry(2500,
                    (currentTimeMillis - MINUTES.toMillis(18)),
                    asList("000018", "000016", "000017", "000026", "000024",
                        "000022", "000020"),
                    asList("000027", "000030", "000028", "000031", "000029")),
                createCompactionEntry(3500,
                    (currentTimeMillis - MINUTES.toMillis(12)),
                    asList("000027", "000030", "000028", "000031", "000029",
                        "000039", "000037", "000035", "000033"),
                    asList("000040", "000044", "000042", "000043", "000046",
                        "000041", "000045")),
                createCompactionEntry(4500,
                    (currentTimeMillis - MINUTES.toMillis(6)),
                    asList("000040", "000044", "000042", "000043", "000046",
                        "000041", "000045", "000054", "000052", "000050",
                        "000048"),
                    asList("000059", "000055", "000056", "000060", "000057",
                        "000058"))),
            ImmutableSet.of("000059", "000055", "000056", "000060", "000057",
                "000058", "000040", "000044", "000042", "000043", "000046",
                "000041", "000045", "000054", "000052", "000050", "000048"),
            4,
            1
        )
    );
  }

  /**
   * End-to-end test for snapshot's compaction history pruning.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("compactionDagPruningScenarios")
  public void testPruneOlderSnapshotsWithCompactionHistory(
      String description,
      List<String> compactionLogs,
      List<CompactionLogEntry> compactionLogEntries,
      Set<String> expectedNodes,
      int expectedNumberOfLogEntriesBeforePruning,
      int expectedNumberOfLogEntriesAfterPruning
  ) throws IOException, ExecutionException, InterruptedException,
      TimeoutException {
    List<File> filesCreated = new ArrayList<>();

    if (compactionLogs != null) {
      for (int i = 0; i < compactionLogs.size(); i++) {
        String compactionFileName = METADATA_DIR_NAME + "/" + COMPACTION_LOG_DIR_NAME
            + "/0000" + i + COMPACTION_LOG_FILE_NAME_SUFFIX;
        File compactionFile = new File(compactionFileName);
        Files.write(compactionFile.toPath(),
            compactionLogs.get(i).getBytes(UTF_8));
        filesCreated.add(compactionFile);
      }
    } else if (compactionLogEntries != null) {
      compactionLogEntries.forEach(entry ->
          rocksDBCheckpointDiffer.addToCompactionLogTable(entry));
    } else {
      throw new IllegalArgumentException("One of compactionLog or" +
          " compactionLogEntries should be present.");
    }

    rocksDBCheckpointDiffer.loadAllCompactionLogs();
    assertEquals(expectedNumberOfLogEntriesBeforePruning,
        countEntriesInCompactionLogTable());
    waitForLock(rocksDBCheckpointDiffer,
        RocksDBCheckpointDiffer::pruneOlderSnapshotsWithCompactionHistory);

    Set<String> actualNodesInForwardDAG = rocksDBCheckpointDiffer
        .getForwardCompactionDAG()
        .nodes()
        .stream()
        .map(CompactionNode::getFileName)
        .collect(Collectors.toSet());

    Set<String> actualNodesBackwardDAG = rocksDBCheckpointDiffer
        .getBackwardCompactionDAG()
        .nodes()
        .stream()
        .map(CompactionNode::getFileName)
        .collect(Collectors.toSet());

    assertEquals(expectedNodes, actualNodesInForwardDAG);
    assertEquals(expectedNodes, actualNodesBackwardDAG);

    for (int i = 0; compactionLogs != null && i < compactionLogs.size(); i++) {
      File compactionFile = filesCreated.get(i);
      assertFalse(compactionFile.exists());
    }

    assertEquals(expectedNumberOfLogEntriesAfterPruning,
        countEntriesInCompactionLogTable());
  }

  private int countEntriesInCompactionLogTable() {
    try (ManagedRocksIterator iterator = new ManagedRocksIterator(
        activeRocksDB.get().newIterator(compactionLogTableCFHandle))) {
      iterator.get().seekToFirst();
      int count = 0;
      while (iterator.get().isValid()) {
        iterator.get().next();
        count++;
      }
      return count;
    }
  }

  // Take the lock, confirm that the consumer doesn't finish
  //  then release the lock and confirm that the consumer does finish.
  private void waitForLock(RocksDBCheckpointDiffer differ,
                           Consumer<RocksDBCheckpointDiffer> c)
      throws InterruptedException, ExecutionException, TimeoutException {

    Future<Boolean> future;
    // Take the lock and start the consumer.
    try (BootstrapStateHandler.Lock lock =
        differ.getBootstrapStateLock().lock()) {
      future = executorService.submit(
          () -> {
            c.accept(differ);
            return true;
          });
      // Confirm that the consumer doesn't finish with lock taken.
      assertThrows(TimeoutException.class,
          () -> future.get(1000, TimeUnit.MILLISECONDS));
    }
    // Confirm consumer finishes when unlocked.
    assertTrue(future.get(100, TimeUnit.MILLISECONDS));
  }

  private static Stream<Arguments> sstFilePruningScenarios() {
    List<String> initialFiles1 = Arrays.asList("000015", "000013", "000011",
        "000009");
    List<String> initialFiles2 = Arrays.asList("000015", "000013", "000011",
        "000009", "000018", "000016", "000017", "000026", "000024", "000022",
        "000020");
    List<String> initialFiles3 = Arrays.asList("000015", "000013", "000011",
        "000009", "000018", "000016", "000017", "000026", "000024", "000022",
        "000020", "000027", "000030", "000028", "000031", "000029", "000039",
        "000037", "000035", "000033", "000040", "000044", "000042", "000043",
        "000046", "000041", "000045", "000054", "000052", "000050", "000048",
        "000059", "000055", "000056", "000060", "000057", "000058");

    List<String> expectedFiles1 = Arrays.asList("000015", "000013", "000011",
        "000009");
    List<String> expectedFiles2 = Arrays.asList("000015", "000013", "000011",
        "000009", "000026", "000024", "000022", "000020");
    List<String> expectedFiles3 = Arrays.asList("000013", "000024", "000035",
        "000011", "000022", "000033", "000039", "000015", "000026", "000037",
        "000048", "000009", "000050", "000054", "000020", "000052");

    return Stream.of(
        Arguments.of("Case 1 with compaction log file: " +
                "No compaction.",
            "",
            null,
            initialFiles1,
            expectedFiles1
        ),
        Arguments.of("Case 2 with compaction log file: " +
                "One level compaction.",
            "C 1 000015,000013,000011,000009:000018,000016,000017\n",
            null,
            initialFiles2,
            expectedFiles2
        ),
        Arguments.of("Case 3 with compaction log file: " +
                "Multi-level compaction.",
            "C 1 000015,000013,000011,000009:000018,000016,000017\n" +
                "C 2 000018,000016,000017,000026,000024,000022,000020:000027," +
                "000030,000028,000031,000029\n" +
                "C 3 000027,000030,000028,000031,000029,000039,000037,000035," +
                "000033:000040,000044,000042,000043,000046,000041,000045\n" +
                "C 4 000040,000044,000042,000043,000046,000041,000045,000054," +
                "000052,000050,000048:000059,000055,000056,000060,000057," +
                "000058\n",
            null,
            initialFiles3,
            expectedFiles3
        ),
        Arguments.of("Case 4 with compaction log table: " +
                "No compaction.",
            null,
            Collections.emptyList(),
            initialFiles1,
            expectedFiles1
        ),
        Arguments.of("Case 5 with compaction log table: " +
                "One level compaction.",
            null,
            Collections.singletonList(createCompactionEntry(1,
                now(),
                asList("000015", "000013", "000011", "000009"),
                asList("000018", "000016", "000017"))),
            initialFiles2,
            expectedFiles2
        ),
        Arguments.of("Case 6 with compaction log table: " +
                "Multi-level compaction.",
            null,
            asList(createCompactionEntry(1,
                    now(),
                    asList("000015", "000013", "000011", "000009"),
                    asList("000018", "000016", "000017")),
                createCompactionEntry(2,
                    now(),
                    asList("000018", "000016", "000017", "000026", "000024",
                        "000022", "000020"),
                    asList("000027", "000030", "000028", "000031", "000029")),
                createCompactionEntry(3,
                    now(),
                    asList("000027", "000030", "000028", "000031", "000029",
                        "000039", "000037", "000035", "000033"),
                    asList("000040", "000044", "000042", "000043", "000046",
                        "000041", "000045")),
                createCompactionEntry(4,
                    now(),
                    asList("000040", "000044", "000042", "000043", "000046",
                        "000041", "000045", "000054", "000052", "000050",
                        "000048"),
                    asList("000059", "000055", "000056", "000060", "000057",
                        "000058"))),
            initialFiles3,
            expectedFiles3
        )
    );
  }

  private static CompactionLogEntry createCompactionEntry(long dbSequenceNumber,
                                                          long compactionTime,
                                                          List<String> inputFiles,
                                                          List<String> outputFiles) {
    return createCompactionEntry(dbSequenceNumber, compactionTime, inputFiles, outputFiles, Collections.emptyMap());
  }

  private static CompactionLogEntry createCompactionEntry(long dbSequenceNumber,
                                                          long compactionTime,
                                                          List<String> inputFiles,
                                                          List<String> outputFiles,
                                                          Map<String, String[]> metadata) {
    return new CompactionLogEntry.Builder(dbSequenceNumber, compactionTime,
        toFileInfoList(inputFiles, metadata), toFileInfoList(outputFiles, metadata)).build();
  }

  private static List<CompactionFileInfo> toFileInfoList(List<String> files,
                                                         Map<String, String[]> metadata) {
    return files.stream()
        .map(fileName -> new CompactionFileInfo.Builder(fileName)
            .setStartRange(Optional.ofNullable(metadata.get(fileName)).map(meta -> meta[0]).orElse(null))
            .setEndRange(Optional.ofNullable(metadata.get(fileName)).map(meta -> meta[1]).orElse(null))
            .setColumnFamily(Optional.ofNullable(metadata.get(fileName)).map(meta -> meta[2]).orElse(null))
            .build())
        .collect(Collectors.toList());
  }

  /**
   * End-to-end test for SST file pruning.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("sstFilePruningScenarios")
  public void testSstFilePruning(
      String description,
      String compactionLog,
      List<CompactionLogEntry> compactionLogEntries,
      List<String> initialFiles,
      List<String> expectedFiles
  ) throws IOException, ExecutionException, InterruptedException,
      TimeoutException {

    for (String fileName : initialFiles) {
      createFileWithContext(sstBackUpDir + "/" + fileName + SST_FILE_EXTENSION,
          fileName);
    }

    Path compactionLogFilePath = null;
    if (compactionLog != null) {
      String compactionLogFileName = METADATA_DIR_NAME + "/" +
          COMPACTION_LOG_DIR_NAME + "/compaction_log" +
          COMPACTION_LOG_FILE_NAME_SUFFIX;
      compactionLogFilePath = new File(compactionLogFileName).toPath();
      createFileWithContext(compactionLogFileName, compactionLog);
      assertTrue(Files.exists(compactionLogFilePath));
    } else if (compactionLogEntries != null) {
      compactionLogEntries.forEach(entry ->
          rocksDBCheckpointDiffer.addToCompactionLogTable(entry));
    } else {
      throw new IllegalArgumentException("One of compactionLog or" +
          " compactionLogEntries should be present.");
    }

    rocksDBCheckpointDiffer.loadAllCompactionLogs();

    waitForLock(rocksDBCheckpointDiffer,
        RocksDBCheckpointDiffer::pruneSstFiles);

    Set<String> actualFileSetAfterPruning;
    try (Stream<Path> pathStream = Files.list(
            Paths.get(METADATA_DIR_NAME + "/" + SST_BACK_UP_DIR_NAME))
        .filter(e -> e.toString().toLowerCase()
            .endsWith(SST_FILE_EXTENSION))
        .sorted()) {
      actualFileSetAfterPruning =
          pathStream.map(path -> path.getFileName().toString())
              .map(name -> name.substring(0,
                  name.length() - SST_FILE_EXTENSION.length()))
              .collect(Collectors.toSet());
    }

    Set<String> expectedFileSet = new HashSet<>(expectedFiles);
    assertEquals(expectedFileSet, actualFileSetAfterPruning);

    if (compactionLogFilePath != null) {
      assertFalse(Files.exists(compactionLogFilePath));
    }
  }

  private void createFileWithContext(String fileName, String context)
      throws IOException {
    try (OutputStream fileOutputStream = Files.newOutputStream(Paths.get(fileName))) {
      fileOutputStream.write(context.getBytes(UTF_8));
    }
  }

  /**
   * Test cases for testGetSSTDiffListWithoutDB.
   */
  private static Stream<Arguments> casesGetSSTDiffListWithoutDB2() {
    return Stream.of(
        Arguments.of("Test case 1.",
            ImmutableSet.of("000081"),
            ImmutableSet.of("000063"),
            ImmutableSet.of("000063"),
            ImmutableSet.of("000078", "000071", "000075", "000073"),
            columnFamilyToPrefixMap1),
        Arguments.of("Test case 2.",
            ImmutableSet.of("000106"),
            ImmutableSet.of("000081"),
            ImmutableSet.of("000081"),
            ImmutableSet.of("000099", "000103", "000097", "000095"),
            columnFamilyToPrefixMap1),
        Arguments.of("Test case 3.",
            ImmutableSet.of("000106"),
            ImmutableSet.of("000063"),
            ImmutableSet.of("000063"),
            ImmutableSet.of("000078", "000071", "000075", "000073", "000103",
                "000099", "000097", "000095"),
            columnFamilyToPrefixMap1),
        Arguments.of("Test case 4.",
            ImmutableSet.of("000131"),
            ImmutableSet.of("000106"),
            ImmutableSet.of("000106"),
            ImmutableSet.of("000123", "000121", "000128", "000125"),
            columnFamilyToPrefixMap2),
        Arguments.of("Test case 5.",
            ImmutableSet.of("000131"),
            ImmutableSet.of("000081"),
            ImmutableSet.of("000081"),
            ImmutableSet.of("000123", "000121", "000128", "000125", "000103",
                "000099", "000097", "000095"),
            columnFamilyToPrefixMap2),
        Arguments.of("Test case 6.",
            ImmutableSet.of("000147", "000131", "000141"),
            ImmutableSet.of("000131"),
            ImmutableSet.of("000131"),
            ImmutableSet.of("000147", "000141"),
            columnFamilyToPrefixMap3),
        Arguments.of("Test case 7.",
            ImmutableSet.of("000147", "000131", "000141"),
            ImmutableSet.of("000106"),
            ImmutableSet.of("000106"),
            ImmutableSet.of("000123", "000121", "000128", "000125", "000147",
                "000141"),
            columnFamilyToPrefixMap3)
    );
  }


  /**
   * Test that backup SST files are pruned on loading previous compaction logs.
   */
  @Test
  public void testPruneSSTFileValues() throws Exception {
    SSTFilePruningMetrics sstFilePruningMetrics = rocksDBCheckpointDiffer.getPruningMetrics();
    assertEquals(0L, sstFilePruningMetrics.getPruneQueueSize());
    assertEquals(0L, sstFilePruningMetrics.getFilesPrunedTotal());
    assertEquals(0L, sstFilePruningMetrics.getFilesPrunedLast());
    assertEquals(0L, sstFilePruningMetrics.getCompactionsProcessed());
    assertEquals(0L, sstFilePruningMetrics.getFilesRemovedTotal());

    List<Pair<byte[], Integer>> keys = new ArrayList<Pair<byte[], Integer>>();
    keys.add(Pair.of("key1".getBytes(UTF_8), Integer.valueOf(1)));
    keys.add(Pair.of("key2".getBytes(UTF_8), Integer.valueOf(0)));
    keys.add(Pair.of("key3".getBytes(UTF_8), Integer.valueOf(1)));

    String inputFile78 = "000078";
    String inputFile73 = "000073";
    String outputFile81 = "000081";
    // Create src & destination files in backup & activedirectory.
    // Pruning job should succeed when pruned temp file is already present.
    createSSTFileWithKeys(sstBackUpDir + "/" + inputFile78 + SST_FILE_EXTENSION, keys);
    createSSTFileWithKeys(sstBackUpDir + "/" + inputFile73 + SST_FILE_EXTENSION, keys);
    createSSTFileWithKeys(sstBackUpDir + PRUNED_SST_FILE_TEMP, keys);
    createSSTFileWithKeys(activeDbDir + "/" + outputFile81 + SST_FILE_EXTENSION, keys);

    // Load compaction log
    CompactionLogEntry compactionLogEntry = new CompactionLogEntry(178, System.currentTimeMillis(),
        Arrays.asList(
            new CompactionFileInfo(inputFile78, "/volume/bucket1/key-5", "/volume/bucket2/key-10", "keyTable"),
            new CompactionFileInfo(inputFile73, "/volume/bucket1/key-1", "/volume/bucket2/key-5", "keyTable")),
        Collections.singletonList(
            new CompactionFileInfo(outputFile81, "/volume/bucket1/key-1", "/volume/bucket2/key-10", "keyTable")),
        null
    );
    byte[] compactionLogEntryKey = rocksDBCheckpointDiffer.addToCompactionLogTable(compactionLogEntry);
    rocksDBCheckpointDiffer.loadAllCompactionLogs();
    assertEquals(1L, sstFilePruningMetrics.getPruneQueueSize());

    // Pruning should not fail a source SST file has been removed by another pruner.
    Files.delete(sstBackUpDir.toPath().resolve(inputFile73 + SST_FILE_EXTENSION));
    // Run the SST file pruner.
    ManagedRawSSTFileIterator mockedRawSSTFileItr = mock(ManagedRawSSTFileIterator.class);
    Iterator keyItr = keys.iterator();
    when(mockedRawSSTFileItr.hasNext()).thenReturn(true, true, true, false);
    when(mockedRawSSTFileItr.next()).thenReturn(keyItr.next(), keyItr.next(), keyItr.next());
    try (MockedConstruction<ManagedRawSSTFileReader> mockedRawSSTReader = Mockito.mockConstruction(
        ManagedRawSSTFileReader.class, (mock, context) -> {
          when(mock.newIterator(any(), any(), any())).thenReturn(mockedRawSSTFileItr);
          doNothing().when(mock).close();
        })) {
      rocksDBCheckpointDiffer.pruneSstFileValues();
    }
    // pruned.sst.tmp should be deleted when pruning job exits successfully.
    assertFalse(Files.exists(sstBackUpDir.toPath().resolve(PRUNED_SST_FILE_TEMP)));

    CompactionLogEntry updatedLogEntry;
    try {
      updatedLogEntry = CompactionLogEntry.getCodec().fromPersistedFormat(
          activeRocksDB.get().get(compactionLogTableCFHandle, compactionLogEntryKey));
    } catch (RocksDBException ex) {
      throw new RocksDatabaseException("Failed to get compaction log entry.", ex);
    }
    CompactionFileInfo fileInfo78 = updatedLogEntry.getInputFileInfoList().get(0);
    CompactionFileInfo fileInfo73 = updatedLogEntry.getInputFileInfoList().get(1);

    // Verify 000078.sst has been pruned
    assertEquals(inputFile78, fileInfo78.getFileName());
    assertTrue(fileInfo78.isPruned());
    ManagedSstFileReader sstFileReader = new ManagedSstFileReader(new ManagedOptions());
    sstFileReader.open(sstBackUpDir.toPath().resolve(inputFile78 + SST_FILE_EXTENSION).toFile().getAbsolutePath());
    ManagedSstFileReaderIterator itr = ManagedSstFileReaderIterator
        .managed(sstFileReader.newIterator(new ManagedReadOptions()));
    itr.get().seekToFirst();
    int prunedKeys = 0;
    while (itr.get().isValid()) {
      // Verify that value is removed for non-tombstone keys.
      assertEquals(0, itr.get().value().length);
      prunedKeys++;
      itr.get().next();
    }
    assertEquals(2, prunedKeys);
    itr.close();
    sstFileReader.close();

    // Verify 000073.sst pruning has been skipped
    assertFalse(fileInfo73.isPruned());

    assertEquals(0L, sstFilePruningMetrics.getPruneQueueSize());
    assertEquals(1L, sstFilePruningMetrics.getFilesPrunedTotal());
    assertEquals(1L, sstFilePruningMetrics.getFilesPrunedLast());
    assertEquals(1L, sstFilePruningMetrics.getCompactionsProcessed());
    assertEquals(1L, sstFilePruningMetrics.getFilesRemovedTotal());
  }

  private void createSSTFileWithKeys(String filePath, List<Pair<byte[], Integer>> keys)
      throws Exception {
    try (ManagedSstFileWriter sstFileWriter = new ManagedSstFileWriter(new ManagedEnvOptions(), new ManagedOptions())) {
      sstFileWriter.open(filePath);
      Iterator<Pair<byte[], Integer>> itr = keys.iterator();
      while (itr.hasNext()) {
        Pair<byte[], Integer> entry = itr.next();
        if (entry.getValue() == 0) {
          sstFileWriter.delete(entry.getKey());
        } else {
          sstFileWriter.put(entry.getKey(), "dummyValue".getBytes(UTF_8));
        }
      }
      sstFileWriter.finish();
    } catch (RocksDBException ex) {
      throw new RocksDatabaseException("Failed to get write " + filePath, ex);
    }
  }

  /**
   * Tests core SST diff list logic. Does not involve DB.
   * Focuses on testing edge cases in internalGetSSTDiffList().
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("casesGetSSTDiffListWithoutDB2")
  public void testGetSSTDiffListWithoutDB2(
      String description,
      Set<String> srcSnapshotSstFiles,
      Set<String> destSnapshotSstFiles,
      Set<String> expectedSameSstFiles,
      Set<String> expectedDiffSstFiles,
      Map<String, String> columnFamilyToPrefixMap
  ) {
    compactionLogEntryList.forEach(entry ->
        rocksDBCheckpointDiffer.addToCompactionLogTable(entry));

    rocksDBCheckpointDiffer.loadAllCompactionLogs();

    // Snapshot is used for logging purpose and short-circuiting traversal.
    // Using gen 0 for this test.
    DifferSnapshotInfo mockedSourceSnapshot = new DifferSnapshotInfo(
        "/path/to/dbcp1", UUID.randomUUID(), 0L, columnFamilyToPrefixMap, null);
    DifferSnapshotInfo mockedDestinationSnapshot = new DifferSnapshotInfo(
        "/path/to/dbcp2", UUID.randomUUID(), 0L, columnFamilyToPrefixMap, null);

    Set<String> actualSameSstFiles = new HashSet<>();
    Set<String> actualDiffSstFiles = new HashSet<>();

    rocksDBCheckpointDiffer.internalGetSSTDiffList(
        mockedSourceSnapshot,
        mockedDestinationSnapshot,
        srcSnapshotSstFiles,
        destSnapshotSstFiles,
        actualSameSstFiles,
        actualDiffSstFiles);

    // Check same and different SST files result
    assertEquals(expectedSameSstFiles, actualSameSstFiles);
    assertEquals(expectedDiffSstFiles, actualDiffSstFiles);
  }

  private static Stream<Arguments> shouldSkipNodeCases() {
    List<Boolean> expectedResponse1 = Arrays.asList(true, false, true, false,
        false, false, false, false, true, true, false, false, false, false,
        false, true, true, true, true, true, false);
    List<Boolean> expectedResponse2 = Arrays.asList(true, true, true, false,
        false, false, false, false, true, true, false, false, false, false,
        false, true, false, false, false, false, false);
    List<Boolean> expectedResponse3 = Arrays.asList(true, true, true, true,
        true, true, true, true, true, true, false, false, false, false, false,
        true, false, false, false, false, false);
    return Stream.of(
        Arguments.of(columnFamilyToPrefixMap1, expectedResponse1),
        Arguments.of(columnFamilyToPrefixMap2, expectedResponse2),
        Arguments.of(columnFamilyToPrefixMap3, expectedResponse3));
  }

  @ParameterizedTest()
  @MethodSource("shouldSkipNodeCases")
  public void testShouldSkipNode(Map<String, String> columnFamilyToPrefixMap,
                                 List<Boolean> expectedResponse) {
    compactionLogEntryList.forEach(entry ->
        rocksDBCheckpointDiffer.addToCompactionLogTable(entry));

    rocksDBCheckpointDiffer.loadAllCompactionLogs();

    List<Boolean> actualResponse = rocksDBCheckpointDiffer
        .getCompactionNodeMap().values().stream()
        .sorted(Comparator.comparing(CompactionNode::getFileName))
        .map(node ->
            RocksDiffUtils.shouldSkipNode(node,
                columnFamilyToPrefixMap))
        .collect(Collectors.toList());

    assertEquals(expectedResponse, actualResponse);
  }

  private static Stream<Arguments> shouldSkipNodeEdgeCases() {
    CompactionNode node = new CompactionNode("fileName", 100, "startKey", "endKey", "columnFamily");
    CompactionNode nullColumnFamilyNode = new CompactionNode("fileName", 100, "startKey", "endKey", null);
    CompactionNode nullStartKeyNode = new CompactionNode("fileName", 100, null, "endKey", "columnFamily");
    CompactionNode nullEndKeyNode = new CompactionNode("fileName", 100, "startKey", null, "columnFamily");

    return Stream.of(
        Arguments.of(node, Collections.emptyMap(), false),
        Arguments.of(node, columnFamilyToPrefixMap1, true),
        Arguments.of(nullColumnFamilyNode, columnFamilyToPrefixMap1, false),
        Arguments.of(nullStartKeyNode, columnFamilyToPrefixMap1, false),
        Arguments.of(nullEndKeyNode, columnFamilyToPrefixMap1, false));
  }

  @ParameterizedTest()
  @MethodSource("shouldSkipNodeEdgeCases")
  public void testShouldSkipNodeEdgeCase(
      CompactionNode node,
      Map<String, String> columnFamilyToPrefixMap,
      boolean expectedResponse
  ) {
    compactionLogEntryList.forEach(entry ->
        rocksDBCheckpointDiffer.addToCompactionLogTable(entry));

    rocksDBCheckpointDiffer.loadAllCompactionLogs();

    assertEquals(expectedResponse, RocksDiffUtils.shouldSkipNode(node,
        columnFamilyToPrefixMap));
  }

  private void createKeys(ColumnFamilyHandle cfh,
                          String keyPrefix,
                          String valuePrefix,
                          int numberOfKeys) throws RocksDBException {

    try (ManagedFlushOptions flushOptions = new ManagedFlushOptions()) {
      for (int i = 0; i < numberOfKeys; ++i) {
        String generatedString = RandomStringUtils.secure().nextAlphabetic(7);
        String keyStr = keyPrefix + i + "-" + generatedString;
        String valueStr = valuePrefix + i + "-" + generatedString;
        byte[] key = keyStr.getBytes(UTF_8);
        activeRocksDB.get().put(cfh, key, valueStr.getBytes(UTF_8));
        if (i % 10 == 0) {
          activeRocksDB.get().flush(flushOptions, cfh);
        }
      }
    }
  }

  // End-to-end to verify that only 'keyTable', 'directoryTable'
  // and 'fileTable' column families SST files are added to compaction DAG.
  @Test
  public void testDagOnlyContainsDesiredCfh()
      throws RocksDBException, IOException {
    // Setting is not non-empty table so that 'isSnapshotInfoTableEmpty'
    // returns true.
    rocksDBCheckpointDiffer.setSnapshotInfoTableCFHandle(keyTableCFHandle);
    createKeys(keyTableCFHandle, "keyName-", "keyValue-", 100);
    createKeys(directoryTableCFHandle, "dirName-", "dirValue-", 100);
    createKeys(fileTableCFHandle, "fileName-", "fileValue-", 100);
    createKeys(compactionLogTableCFHandle, "logName-", "logValue-", 100);

    // Make sures that some compaction happened.
    assertThat(rocksDBCheckpointDiffer.getCompactionNodeMap()).isNotEmpty();

    List<CompactionNode> compactionNodes = rocksDBCheckpointDiffer.
        getCompactionNodeMap().values().stream()
        .filter(node -> !COLUMN_FAMILIES_TO_TRACK_IN_DAG.contains(
            node.getColumnFamily()))
        .collect(Collectors.toList());

    // CompactionNodeMap should not contain any node other than 'keyTable',
    // 'directoryTable' and 'fileTable' column families nodes.
    assertThat(compactionNodes).isEmpty();

    // Assert that only 'keyTable', 'directoryTable' and 'fileTable'
    // column families SST files are backed-up.
    try (ManagedOptions options = new ManagedOptions();
         Stream<Path> pathStream = Files.list(
             Paths.get(rocksDBCheckpointDiffer.getSSTBackupDir()))) {
      pathStream.forEach(path -> {
        try (ManagedSstFileReader fileReader = new ManagedSstFileReader(options)) {
          fileReader.open(path.toAbsolutePath().toString());
          String columnFamily = bytes2String(fileReader.getTableProperties().getColumnFamilyName());
          assertThat(COLUMN_FAMILIES_TO_TRACK_IN_DAG).contains(columnFamily);
        } catch (RocksDBException rocksDBException) {
          fail("Failed to read file: " + path.toAbsolutePath());
        }
      });
    }
  }

  private static Stream<Arguments> shouldSkipFileCases() {
    return Stream.of(
        Arguments.of("Case#1: volumeTable is irrelevant column family.",
            "volumeTable".getBytes(UTF_8),
            Arrays.asList("inputFile1", "inputFile2", "inputFile3"),
            Arrays.asList("outputFile1", "outputFile2"), true),
        Arguments.of("Case#2: bucketTable is irrelevant column family.",
            "bucketTable".getBytes(UTF_8),
            Arrays.asList("inputFile1", "inputFile2", "inputFile3"),
            Arrays.asList("outputFile1", "outputFile2"), true),
        Arguments.of("Case#3: snapshotInfoTable is irrelevant column family.",
            "snapshotInfoTable".getBytes(UTF_8),
            Arrays.asList("inputFile1", "inputFile2", "inputFile3"),
            Arrays.asList("outputFile1", "outputFile2"), true),
        Arguments.of("Case#4: compactionLogTable is irrelevant column family.",
            "compactionLogTable".getBytes(UTF_8),
            Arrays.asList("inputFile1", "inputFile2", "inputFile3"),
            Arrays.asList("outputFile1", "outputFile2"), true),
        Arguments.of("Case#5: Input file list is empty..",
            "keyTable".getBytes(UTF_8), Collections.emptyList(),
            Arrays.asList("outputFile1", "outputFile2"), true),
        Arguments.of("Case#6: Input and output file lists are same.",
            "keyTable".getBytes(UTF_8),
            Arrays.asList("inputFile1", "inputFile2", "inputFile3"),
            Arrays.asList("inputFile1", "inputFile2", "inputFile3"), true),
        Arguments.of("Case#7: keyTable is relevant column family.",
            "keyTable".getBytes(UTF_8),
            Arrays.asList("inputFile1", "inputFile2", "inputFile3"),
            Arrays.asList("outputFile1", "outputFile2"), false),
        Arguments.of("Case#8: directoryTable is relevant column family.",
            "directoryTable".getBytes(UTF_8),
            Arrays.asList("inputFile1", "inputFile2", "inputFile3"),
            Arrays.asList("outputFile1", "outputFile2"), false),
        Arguments.of("Case#9: fileTable is relevant column family.",
            "fileTable".getBytes(UTF_8),
            Arrays.asList("inputFile1", "inputFile2", "inputFile3"),
            Arrays.asList("outputFile1", "outputFile2"), false));
  }

  @MethodSource("shouldSkipFileCases")
  @ParameterizedTest(name = "{0}")
  public void testShouldSkipFile(String description,
                                 byte[] columnFamilyBytes,
                                 List<String> inputFiles,
                                 List<String> outputFiles,
                                 boolean expectedResult) {
    assertEquals(expectedResult, rocksDBCheckpointDiffer
        .shouldSkipCompaction(columnFamilyBytes, inputFiles, outputFiles));
  }
}
