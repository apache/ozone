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

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_PRUNE_DAEMON_RUN_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_LOAD_NATIVE_LIB;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_LOAD_NATIVE_LIB_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_BACKUP_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_BACKUP_BATCH_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_DAG_DAEMON_RUN_INTERVAL_DEFAULT;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.COMPACTION_LOG_FILE_NAME_SUFFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.ManagedRawSSTFileReader;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.ozone.compaction.log.CompactionLogEntry;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.event.Level;

/**
 * Test for CompactionDag.
 */
public class TestCompactionDag {

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

  private static final String ACTIVE_DB_DIR_NAME = "./rocksdb-data";
  private static final String METADATA_DIR_NAME = "./metadata";
  private static final String COMPACTION_LOG_DIR_NAME = "compaction-log";
  private static final String SST_BACK_UP_DIR_NAME = "compaction-sst-backup";
  private File activeDbDir;
  private File metadataDirDir;
  private File compactionLogDir;
  private File sstBackUpDir;
  
  private final ExecutorService executorService =
      Executors.newCachedThreadPool();
  private RocksDBCheckpointDiffer rocksDBCheckpointDiffer;
  private ManagedRocksDB activeRocksDB;
  private ColumnFamilyHandle compactionLogTableCFHandle;

  @BeforeEach
  public void init() throws RocksDBException {
    // Checkpoint differ log level. Set to DEBUG for verbose output
    GenericTestUtils.setLogLevel(RocksDBCheckpointDiffer.class, Level.INFO);
    // Test class log level. Set to DEBUG for verbose output
    GenericTestUtils.setLogLevel(TestCompactionDag.class, Level.INFO);

    activeDbDir = new File(ACTIVE_DB_DIR_NAME);
    createDir(activeDbDir, ACTIVE_DB_DIR_NAME);

    metadataDirDir = new File(METADATA_DIR_NAME);
    createDir(metadataDirDir, METADATA_DIR_NAME);

    compactionLogDir = new File(METADATA_DIR_NAME, COMPACTION_LOG_DIR_NAME);
    createDir(compactionLogDir,
        METADATA_DIR_NAME + "/" + COMPACTION_LOG_DIR_NAME);

    sstBackUpDir = new File(METADATA_DIR_NAME, SST_BACK_UP_DIR_NAME);
    createDir(sstBackUpDir,
        METADATA_DIR_NAME + "/" + SST_BACK_UP_DIR_NAME);

    ConfigurationSource config = mock(ConfigurationSource.class);

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
        OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_BACKUP_BATCH_SIZE_DEFAULT))
        .thenReturn(2000);

    when(config.getBoolean(
        OZONE_OM_SNAPSHOT_LOAD_NATIVE_LIB,
        OZONE_OM_SNAPSHOT_LOAD_NATIVE_LIB_DEFAULT)).thenReturn(true);

    try (MockedStatic<ManagedRawSSTFileReader> mockedRawSSTReader =
             Mockito.mockStatic(ManagedRawSSTFileReader.class)) {
      mockedRawSSTReader.when(ManagedRawSSTFileReader::loadLibrary)
          .thenReturn(true);
      ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
      Function<Boolean, UncheckedAutoCloseable> dummyLock = (readLock) -> {
        if (readLock) {
          readWriteLock.readLock().lock();
          return (UncheckedAutoCloseable) () -> readWriteLock.readLock().unlock();
        } else {
          readWriteLock.writeLock().lock();
          return (UncheckedAutoCloseable) () -> readWriteLock.writeLock().unlock();
        }
      };
      rocksDBCheckpointDiffer = new RocksDBCheckpointDiffer(METADATA_DIR_NAME,
          SST_BACK_UP_DIR_NAME,
          COMPACTION_LOG_DIR_NAME,
          ACTIVE_DB_DIR_NAME,
          config, dummyLock);
    }

    ManagedColumnFamilyOptions cfOpts = new ManagedColumnFamilyOptions();
    cfOpts.optimizeUniversalStyleCompaction();
    List<ColumnFamilyDescriptor> cfDescriptors =
        TestRocksDBCheckpointDiffer.getCFDescriptorList(cfOpts);
    List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
    ManagedDBOptions dbOptions = new ManagedDBOptions();
    dbOptions.setCreateIfMissing(true);
    dbOptions.setCreateMissingColumnFamilies(true);

    rocksDBCheckpointDiffer.setRocksDBForCompactionTracking(dbOptions);
    activeRocksDB = ManagedRocksDB.open(dbOptions, ACTIVE_DB_DIR_NAME,
        cfDescriptors, cfHandles);
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

  @AfterEach
  public void cleanUp() {
    IOUtils.closeQuietly(rocksDBCheckpointDiffer);
    IOUtils.closeQuietly(compactionLogTableCFHandle);
    IOUtils.closeQuietly(activeRocksDB);
    deleteDirectory(compactionLogDir);
    deleteDirectory(sstBackUpDir);
    deleteDirectory(metadataDirDir);
    deleteDirectory(activeDbDir);
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
    CompactionDag compactionDag = new CompactionDag();
    Set<String> actualFileNodesRemoved =
        compactionDag.pruneBackwardDag(originalDag, levelToBeRemoved);
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
    CompactionDag compactionDag = new CompactionDag();
    Set<String> actualFileNodesRemoved =
        compactionDag.pruneForwardDag(originalDag, levelToBeRemoved);
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
        "000022,000020:000027,000030,000028,000029,000031,000029\n"
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
        "000009:000018,000016,000017\n"
        + "C 2000 000018,000016,000017,000026,000024,000022,000020" +
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
            asList(TestRocksDBCheckpointDiffer.createCompactionEntry(1500,
                    (currentTimeMillis - MINUTES.toMillis(24)),
                    asList("000015", "000013", "000011", "000009"),
                    asList("000018", "000016", "000017")),
                TestRocksDBCheckpointDiffer.createCompactionEntry(2500,
                    (currentTimeMillis - MINUTES.toMillis(20)),
                    asList("000018", "000016", "000017", "000026", "000024",
                        "000022", "000020"),
                    asList("000027", "000030", "000028", "000031", "000029")),
                TestRocksDBCheckpointDiffer.createCompactionEntry(3500,
                    (currentTimeMillis - MINUTES.toMillis(16)),
                    asList("000027", "000030", "000028", "000031", "000029",
                        "000039", "000037", "000035", "000033"),
                    asList("000040", "000044", "000042", "000043", "000046",
                        "000041", "000045")),
                TestRocksDBCheckpointDiffer.createCompactionEntry(4500,
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
            asList(TestRocksDBCheckpointDiffer.createCompactionEntry(1500,
                    (currentTimeMillis - MINUTES.toMillis(24)),
                    asList("000015", "000013", "000011", "000009"),
                    asList("000018", "000016", "000017")),
                TestRocksDBCheckpointDiffer.createCompactionEntry(2500,
                    (currentTimeMillis - MINUTES.toMillis(18)),
                    asList("000018", "000016", "000017", "000026", "000024",
                        "000022", "000020"),
                    asList("000027", "000030", "000028", "000031", "000029")),
                TestRocksDBCheckpointDiffer.createCompactionEntry(3500,
                    (currentTimeMillis - MINUTES.toMillis(12)),
                    asList("000027", "000030", "000028", "000031", "000029",
                        "000039", "000037", "000035", "000033"),
                    asList("000040", "000044", "000042", "000043", "000046",
                        "000041", "000045")),
                TestRocksDBCheckpointDiffer.createCompactionEntry(4500,
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
        String compactionFileName = METADATA_DIR_NAME + "/" +
            COMPACTION_LOG_DIR_NAME
            + "/0000" + i + COMPACTION_LOG_FILE_NAME_SUFFIX;
        File compactionFile = new File(compactionFileName);
        Files.write(compactionFile.toPath(),
            compactionLogs.get(i).getBytes(StandardCharsets.UTF_8));
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
    try (UncheckedAutoCloseable lock =
             differ.getBootstrapStateLock().acquireWriteLock()) {
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
}
