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
package org.apache.ozone.rocksdiff;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import com.google.common.graph.GraphBuilder;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.graph.MutableGraph;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.NodeComparator;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.DEBUG_DAG_LIVE_NODES;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.DEBUG_READ_ALL_DB_KEYS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test RocksDBCheckpointDiffer basic functionality.
 */
public class TestRocksDBCheckpointDiffer {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRocksDBCheckpointDiffer.class);

  /**
   * RocksDB path for the test.
   */
  private static final String TEST_DB_PATH = "./rocksdb-data";
  private static final int NUM_ROW = 250000;
  private static final int SNAPSHOT_EVERY_SO_MANY_KEYS = 49999;

  /**
   * RocksDB checkpoint path prefix.
   */
  private static final String CP_PATH_PREFIX = "rocksdb-cp-";
  private final List<DifferSnapshotInfo> snapshots = new ArrayList<>();

  @BeforeEach
  public void init() {
    // Checkpoint differ log level. Set to DEBUG for verbose output
    GenericTestUtils.setLogLevel(RocksDBCheckpointDiffer.getLog(), Level.INFO);
    // Test class log level. Set to DEBUG for verbose output
    GenericTestUtils.setLogLevel(TestRocksDBCheckpointDiffer.LOG, Level.INFO);
  }

  /**
   * Test cases for testGetSSTDiffListWithoutDB.
   */
  private static Stream<Arguments> casesGetSSTDiffListWithoutDB() {

    DifferSnapshotInfo snapshotInfo1 = new DifferSnapshotInfo(
        "/path/to/dbcp1", "ssUUID1", 3008L, null);
    DifferSnapshotInfo snapshotInfo2 = new DifferSnapshotInfo(
        "/path/to/dbcp2", "ssUUID2", 14980L, null);
    DifferSnapshotInfo snapshotInfo3 = new DifferSnapshotInfo(
        "/path/to/dbcp3", "ssUUID3", 17975L, null);
    DifferSnapshotInfo snapshotInfo4 = new DifferSnapshotInfo(
        "/path/to/dbcp4", "ssUUID4", 18000L, null);

    Set<String> snapshotSstFiles1 = new HashSet<>(asList(
        "000059", "000053"));
    Set<String> snapshotSstFiles2 = new HashSet<>(asList(
        "000088", "000059", "000053", "000095"));
    Set<String> snapshotSstFiles3 = new HashSet<>(asList(
        "000088", "000105", "000059", "000053", "000095"));
    Set<String> snapshotSstFiles4 = new HashSet<>(asList(
        "000088", "000105", "000059", "000053", "000095", "000108"));
    Set<String> snapshotSstFiles1Alt1 = new HashSet<>(asList(
        "000059", "000053", "000066"));
    Set<String> snapshotSstFiles1Alt2 = new HashSet<>(asList(
        "000059", "000053", "000052"));
    Set<String> snapshotSstFiles2Alt2 = new HashSet<>(asList(
        "000088", "000059", "000053", "000095", "000099"));
    Set<String> snapshotSstFiles2Alt3 = new HashSet<>(asList(
        "000088", "000059", "000053", "000062"));

    return Stream.of(
        Arguments.of("Test 1: Regular case. Expands expandable " +
                "SSTs in the initial diff.",
            snapshotInfo3,
            snapshotInfo1,
            snapshotSstFiles3,
            snapshotSstFiles1,
            new HashSet<>(asList("000059", "000053")),
            new HashSet<>(asList(
                "000066", "000105", "000080", "000087", "000073", "000095")),
            false),
        Arguments.of("Test 2: Crafted input: One source " +
                "('to' snapshot) SST file is never compacted (newly flushed)",
            snapshotInfo4,
            snapshotInfo3,
            snapshotSstFiles4,
            snapshotSstFiles3,
            new HashSet<>(asList(
                "000088", "000105", "000059", "000053", "000095")),
            new HashSet<>(asList("000108")),
            false),
        Arguments.of("Test 3: Crafted input: Same SST files " +
                "found during SST expansion",
            snapshotInfo2,
            snapshotInfo1,
            snapshotSstFiles2,
            snapshotSstFiles1Alt1,
            new HashSet<>(asList("000066", "000059", "000053")),
            new HashSet<>(asList(
                "000080", "000087", "000073", "000095")),
            false),
        Arguments.of("Test 4: Crafted input: Skipping known " +
                "processed SST.",
            snapshotInfo2,
            snapshotInfo1,
            snapshotSstFiles2Alt2,
            snapshotSstFiles1Alt2,
            new HashSet<>(),
            new HashSet<>(),
            true),
        Arguments.of("Test 5: Hit snapshot generation early exit " +
                "condition",
            snapshotInfo2,
            snapshotInfo1,
            snapshotSstFiles2Alt3,
            snapshotSstFiles1,
            new HashSet<>(asList("000059", "000053")),
            new HashSet<>(asList(
                "000066", "000080", "000087", "000073", "000062")),
            false)
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
      DifferSnapshotInfo srcSnapshot,
      DifferSnapshotInfo destSnapshot,
      Set<String> srcSnapshotSstFiles,
      Set<String> destSnapshotSstFiles,
      Set<String> expectedSameSstFiles,
      Set<String> expectedDiffSstFiles,
      boolean expectingException) {

    RocksDBCheckpointDiffer differ =
        new RocksDBCheckpointDiffer(null, null, null, 0L);
    boolean exceptionThrown = false;
    long createdTime = System.currentTimeMillis();

    String compactionLog = ""
        // Snapshot 0
        + "S 1000 df6410c7-151b-4e90-870e-5ef12875acd5 " + createdTime + " \n"
        // Additional "compaction" to trigger and test early exit condition
        + "C 000001,000002:000062\n"
        // Snapshot 1
        + "S 3008 ef6410c7-151b-4e90-870e-5ef12875acd5 " + createdTime + " \n"
        // Regular compaction
        + "C 000068,000062:000069\n"
        // Trivial move
        + "C 000071,000064,000060,000052:000071,000064,000060,000052\n"
        + "C 000073,000066:000074\n"
        + "C 000082,000076,000069:000083\n"
        + "C 000087,000080,000074:000088\n"
        // Deletion?
        + "C 000093,000090,000083:\n"
        // Snapshot 2
        + "S 14980 e7ad72f8-52df-4430-93f6-0ee91d4a47fd " + createdTime + "\n"
        + "C 000098,000096,000085,000078,000071,000064,000060,000052:000099\n"
        + "C 000105,000095,000088:000107\n"
        // Snapshot 3
        + "S 17975 4f084f6e-ed3d-4780-8362-f832303309ea " + createdTime + "\n";

    // Construct DAG from compaction log input
    Arrays.stream(compactionLog.split("\n")).forEach(
        differ::processCompactionLogLine);

    Set<String> actualSameSstFiles = new HashSet<>();
    Set<String> actualDiffSstFiles = new HashSet<>();

    try {
      differ.internalGetSSTDiffList(
              srcSnapshot,
              destSnapshot,
              srcSnapshotSstFiles,
              destSnapshotSstFiles,
              differ.getForwardCompactionDAG(),
              actualSameSstFiles,
              actualDiffSstFiles);
    } catch (RuntimeException rtEx) {
      if (!expectingException) {
        fail("Unexpected exception thrown in test.");
      } else {
        exceptionThrown = true;
      }
    }

    // Check same and different SST files result
    Assertions.assertEquals(expectedSameSstFiles, actualSameSstFiles);
    Assertions.assertEquals(expectedDiffSstFiles, actualDiffSstFiles);

    if (expectingException && !exceptionThrown) {
      fail("Expecting exception but none thrown.");
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

    final String clDirStr = "compaction-log";
    // Delete the compaction log dir for the test, if it exists
    File clDir = new File(clDirStr);
    if (clDir.exists()) {
      deleteDirectory(clDir);
    }

    final String metadataDirStr = ".";
    final String sstDirStr = "compaction-sst-backup";

    final File dbLocation = new File(TEST_DB_PATH);
    RocksDBCheckpointDiffer differ = new RocksDBCheckpointDiffer(
        metadataDirStr, sstDirStr, clDirStr, dbLocation,
        TimeUnit.DAYS.toMillis(1),
        MINUTES.toMillis(5));

    // Empty the SST backup folder first for testing
    File sstDir = new File(sstDirStr);
    deleteDirectory(sstDir);
    if (!sstDir.mkdir()) {
      fail("Unable to create SST backup directory");
    }

    RocksDB rocksDB = createRocksDBInstanceAndWriteKeys(TEST_DB_PATH, differ);
    readRocksDBInstance(TEST_DB_PATH, rocksDB, null, differ);

    if (LOG.isDebugEnabled()) {
      printAllSnapshots();
    }

    traverseGraph(differ.getCompactionNodeMap(),
        differ.getBackwardCompactionDAG(),
        differ.getForwardCompactionDAG());

    diffAllSnapshots(differ);

    // Confirm correct links created
    try (Stream<Path> sstPathStream = Files.list(sstDir.toPath())) {
      List<String> expectedLinks = sstPathStream.map(Path::getFileName)
              .map(Object::toString).sorted().collect(Collectors.toList());
      Assertions.assertEquals(expectedLinks, asList(
              "000015.sst", "000017.sst", "000019.sst", "000021.sst",
              "000022.sst", "000024.sst", "000026.sst"));
    }

    if (LOG.isDebugEnabled()) {
      differ.dumpCompactionNodeTable();
    }

    rocksDB.close();
  }

  /**
   * Test SST differ.
   */
  void diffAllSnapshots(RocksDBCheckpointDiffer differ) {
    final DifferSnapshotInfo src = snapshots.get(snapshots.size() - 1);

    // Hard-coded expected output.
    // The results are deterministic. Retrieved from a successful run.
    final List<List<String>> expectedDifferResult = asList(
        asList("000024", "000017", "000028", "000026", "000019", "000021"),
        asList("000024", "000028", "000026", "000019", "000021"),
        asList("000024", "000028", "000026", "000021"),
        asList("000024", "000028", "000026"),
        asList("000028", "000026"),
        Collections.singletonList("000028"),
        Collections.emptyList()
    );
    Assertions.assertEquals(snapshots.size(), expectedDifferResult.size());

    int index = 0;
    for (DifferSnapshotInfo snap : snapshots) {
      // Returns a list of SST files to be fed into RocksDiff
      List<String> sstDiffList = differ.getSSTDiffList(src, snap);
      LOG.debug("SST diff list from '{}' to '{}': {}",
          src.getDbPath(), snap.getDbPath(), sstDiffList);

      Assertions.assertEquals(expectedDifferResult.get(index), sstDiffList);
      ++index;
    }
  }

  /**
   * Helper function that creates an RDB checkpoint (= Ozone snapshot).
   */
  private void createCheckpoint(RocksDBCheckpointDiffer differ,
      RocksDB rocksDB) {

    LOG.trace("Current time: " + System.currentTimeMillis());
    long t1 = System.currentTimeMillis();

    final long snapshotGeneration = rocksDB.getLatestSequenceNumber();
    final String cpPath = CP_PATH_PREFIX + snapshotGeneration;

    // Delete the checkpoint dir if it already exists for the test
    File dir = new File(cpPath);
    if (dir.exists()) {
      deleteDirectory(dir);
    }

    final long dbLatestSequenceNumber = rocksDB.getLatestSequenceNumber();

    createCheckPoint(TEST_DB_PATH, cpPath, rocksDB);
    final String snapshotId = "snap_id_" + snapshotGeneration;
    final DifferSnapshotInfo currentSnapshot =
        new DifferSnapshotInfo(cpPath, snapshotId, snapshotGeneration, null);
    this.snapshots.add(currentSnapshot);

    // Same as what OmSnapshotManager#createOmSnapshotCheckpoint would do
    differ.appendSnapshotInfoToCompactionLog(dbLatestSequenceNumber,
        snapshotId,
        System.currentTimeMillis());

    differ.setCurrentCompactionLog(dbLatestSequenceNumber);

    long t2 = System.currentTimeMillis();
    LOG.trace("Current time: " + t2);
    LOG.debug("Time elapsed: " + (t2 - t1) + " ms");
  }

  // Flushes the WAL and Creates a RocksDB checkpoint
  void createCheckPoint(String dbPathArg, String cpPathArg,
      RocksDB rocksDB) {
    LOG.debug("Creating RocksDB '{}' checkpoint at '{}'", dbPathArg, cpPathArg);
    try {
      rocksDB.flush(new FlushOptions());
      Checkpoint cp = Checkpoint.create(rocksDB);
      cp.createCheckpoint(cpPathArg);
    } catch (RocksDBException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  void printAllSnapshots() {
    for (DifferSnapshotInfo snap : snapshots) {
      LOG.debug("{}", snap);
    }
  }

  // Test Code to create sample RocksDB instance.
  private RocksDB createRocksDBInstanceAndWriteKeys(String dbPathArg,
      RocksDBCheckpointDiffer differ) throws RocksDBException {

    LOG.debug("Creating RocksDB at '{}'", dbPathArg);

    // Delete the test DB dir if it exists
    File dir = new File(dbPathArg);
    if (dir.exists()) {
      deleteDirectory(dir);
    }

    final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
        .optimizeUniversalStyleCompaction();
    final List<ColumnFamilyDescriptor> cfDescriptors =
        RocksDBCheckpointDiffer.getCFDescriptorList(cfOpts);
    List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

    // Create a RocksDB instance with compaction tracking
    final DBOptions dbOptions = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
    differ.setRocksDBForCompactionTracking(dbOptions);
    RocksDB rocksDB = RocksDB.open(dbOptions, dbPathArg, cfDescriptors,
        cfHandles);

    differ.setCurrentCompactionLog(rocksDB.getLatestSequenceNumber());

    // key-value
    for (int i = 0; i < NUM_ROW; ++i) {
      String generatedString = RandomStringUtils.randomAlphabetic(7);
      String keyStr = "Key-" + i + "-" + generatedString;
      String valueStr = "Val-" + i + "-" + generatedString;
      byte[] key = keyStr.getBytes(UTF_8);
      // Put entry in keyTable
      rocksDB.put(cfHandles.get(1), key, valueStr.getBytes(UTF_8));
      if (i % SNAPSHOT_EVERY_SO_MANY_KEYS == 0) {
        createCheckpoint(differ, rocksDB);
      }
    }
    createCheckpoint(differ, rocksDB);
    return rocksDB;
  }

  static boolean deleteDirectory(java.io.File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (java.io.File file : allContents) {
        if (!deleteDirectory(file)) {
          return false;
        }
      }
    }
    return directoryToBeDeleted.delete();
  }

  // Read from a given RocksDB instance and optionally write all the
  // keys to a given file.
  void readRocksDBInstance(String dbPathArg, RocksDB rocksDB, FileWriter file,
      RocksDBCheckpointDiffer differ) {

    LOG.debug("Reading RocksDB: " + dbPathArg);
    boolean createdDB = false;

    try (Options options = new Options()
        .setParanoidChecks(true)
        .setForceConsistencyChecks(false)) {

      if (rocksDB == null) {
        rocksDB = RocksDB.openReadOnly(options, dbPathArg);
        createdDB = true;
      }

      List<LiveFileMetaData> liveFileMetaDataList =
          rocksDB.getLiveFilesMetaData();
      for (LiveFileMetaData m : liveFileMetaDataList) {
        LOG.debug("SST File: {}. ", m.fileName());
        LOG.debug("\tLevel: {}", m.level());
        LOG.debug("\tTable: {}", toStr(m.columnFamilyName()));
        LOG.debug("\tKey Range: {}", toStr(m.smallestKey())
            + " <-> " + toStr(m.largestKey()));
        if (differ.debugEnabled(DEBUG_DAG_LIVE_NODES)) {
          printMutableGraphFromAGivenNode(
              differ.getCompactionNodeMap(),
              m.fileName(), m.level(),
              differ.getForwardCompactionDAG());
        }
      }

      if (differ.debugEnabled(DEBUG_READ_ALL_DB_KEYS)) {
        RocksIterator iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
          LOG.debug("Iterator key:" + toStr(iter.key()) + ", " +
              "iter value:" + toStr(iter.value()));
          if (file != null) {
            file.write("iterator key:" + toStr(iter.key()) + ", iter " +
                "value:" + toStr(iter.value()));
            file.write("\n");
          }
        }
      }
    } catch (IOException | RocksDBException e) {
      e.printStackTrace();
    } finally {
      if (createdDB) {
        rocksDB.close();
      }
    }
  }

  /**
   * Return String object encoded in UTF-8 from a byte array.
   */
  private String toStr(byte[] bytes) {
    return new String(bytes, UTF_8);
  }

  /**
   * Helper that traverses the graphs for testing.
   * @param compactionNodeMap
   * @param reverseMutableGraph
   * @param fwdMutableGraph
   */
  private void traverseGraph(
      ConcurrentHashMap<String, CompactionNode> compactionNodeMap,
      MutableGraph<CompactionNode> reverseMutableGraph,
      MutableGraph<CompactionNode> fwdMutableGraph) {

    List<CompactionNode> nodeList = compactionNodeMap.values().stream()
        .sorted(new NodeComparator()).collect(Collectors.toList());

    for (CompactionNode infileNode : nodeList) {
      // fist go through fwdGraph to find nodes that don't have successors.
      // These nodes will be the top level nodes in reverse graph
      Set<CompactionNode> successors = fwdMutableGraph.successors(infileNode);
      if (successors.size() == 0) {
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
      ConcurrentHashMap<String, CompactionNode> compactionNodeMap,
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
          sb.append(succNode.getFileName()).append(" ");
          nextLevel.add(succNode);
        }
      }
      LOG.debug("{}", sb);
      currentLevel = nextLevel;
    }
  }

  private void printMutableGraph(String srcSnapId, String destSnapId,
      MutableGraph<CompactionNode> mutableGraph) {

    LOG.debug("Gathering all SST file nodes from src '{}' to dest '{}'",
        srcSnapId, destSnapId);

    final Queue<CompactionNode> nodeQueue = new LinkedList<>();
    // Queue source snapshot SST file nodes
    for (CompactionNode node : mutableGraph.nodes()) {
      if (srcSnapId == null ||
          node.getSnapshotId().compareToIgnoreCase(srcSnapId) == 0) {
        nodeQueue.add(node);
      }
    }

    final Set<CompactionNode> allNodesSet = new HashSet<>();
    while (!nodeQueue.isEmpty()) {
      CompactionNode node = nodeQueue.poll();
      Set<CompactionNode> succSet = mutableGraph.successors(node);
      LOG.debug("Current node: {}", node);
      if (succSet.isEmpty()) {
        LOG.debug("Has no successor node");
        allNodesSet.add(node);
        continue;
      }
      for (CompactionNode succNode : succSet) {
        LOG.debug("Has successor node: {}", succNode);
        if (srcSnapId == null ||
            succNode.getSnapshotId().compareToIgnoreCase(destSnapId) == 0) {
          allNodesSet.add(succNode);
          continue;
        }
        nodeQueue.add(succNode);
      }
    }

    LOG.debug("Files are: {}", allNodesSet);
  }

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
                          UUID.randomUUID().toString(),
                          1000L,
                          Long.parseLong(sstFile.substring(0, 6))
                      ))
                  .collect(Collectors.toList()))
          .collect(Collectors.toList());

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

      for (int i = 0; i < currentLevel.size(); i++) {
        for (int j = 0; j < nextLevel.size(); j++) {
          dag.addNode(currentLevel.get(i));
          dag.addNode(nextLevel.get(j));

          int child = nextLevel.size();
          if (level < COMPACTION_NODES_BY_LEVEL.size() - 2) {
            child /= 2;
          }

          if (j < child) {
            dag.putEdge(currentLevel.get(i), nextLevel.get(j));
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
        for (int j = 0; j < nextLevel.size(); j++) {
          dag.addNode(currentLevel.get(i));
          dag.addNode(nextLevel.get(j));

          int parent = currentLevel.size();
          if (level < COMPACTION_NODES_BY_LEVEL.size() - 1) {
            parent /= 2;
          }

          if (i < parent) {
            dag.putEdge(currentLevel.get(i), nextLevel.get(j));
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

    RocksDBCheckpointDiffer differ =
        new RocksDBCheckpointDiffer(null, null, null, 0L);
    Set<String> actualFileNodesRemoved =
        differ.pruneBackwardDag(originalDag, levelToBeRemoved);
    Assertions.assertEquals(expectedDag, originalDag);
    Assertions.assertEquals(actualFileNodesRemoved, expectedFileNodesRemoved);
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

    RocksDBCheckpointDiffer differ =
        new RocksDBCheckpointDiffer(null, null, null, 0L);
    Set<String> actualFileNodesRemoved =
        differ.pruneForwardDag(originalDag, levelToBeRemoved);
    Assertions.assertEquals(expectedDag, originalDag);
    Assertions.assertEquals(actualFileNodesRemoved, expectedFileNodesRemoved);
  }

  /**
   * End-to-end test for snapshot's compaction history pruning.
   */
  @Test
  public void testPruneOlderSnapshotsWithCompactionHistory()
      throws IOException {
    String compactionLogDirName = "./test-compaction-log";
    File compactionLogDir = new File(compactionLogDirName);
    if (!compactionLogDir.exists() && !compactionLogDir.mkdirs()) {
      fail("Error creating compaction log directory: " + compactionLogDirName);
    }

    String sstBackUpDirName = "./test-compaction-sst-backup";
    File sstBackUpDir = new File(sstBackUpDirName);
    if (!sstBackUpDir.exists() && !sstBackUpDir.mkdirs()) {
      fail("Error creating SST backup directory: " + sstBackUpDir);
    }

    long currentTimeMillis = System.currentTimeMillis();

    String compactionLog0 = "S 1000 snapshotId0 " +
        (currentTimeMillis - MINUTES.toMillis(30)) + " \n";
    String compactionLog1 = "C 000015,000013,000011,000009:000018,000016," +
        "000017\n"
        + "S 2000 snapshotId1 " +
        (currentTimeMillis - MINUTES.toMillis(24)) + " \n";

    String compactionLog2 = "C 000018,000016,000017,000026,000024,000022," +
        "000020:000027,000030,000028,000031,000029\n"
        + "S 3000 snapshotId2 " +
        (currentTimeMillis - MINUTES.toMillis(18)) + " \n";

    String compactionLog3 = "C 000027,000030,000028,000031,000029,000039," +
        "000037,000035,000033:000040,000044,000042,000043,000046,000041," +
        "000045\n"
        + "S 3000 snapshotId3 " +
        (currentTimeMillis - MINUTES.toMillis(12)) + " \n";

    String compactionLog4 = "C 000040,000044,000042,000043,000046,000041," +
        "000045,000054,000052,000050,000048:000059,000055,000056,000060," +
        "000057,000058\n"
        + "S 3000 snapshotId4 " +
        (currentTimeMillis - MINUTES.toMillis(6)) + " \n";

    List<String> compactionLogs = Arrays.asList(compactionLog0,
        compactionLog1,
        compactionLog2,
        compactionLog3,
        compactionLog4);

    for (int i = 0; i < compactionLogs.size(); i++) {
      String compactionFileName = compactionLogDirName + "/0000" + i + ".log";
      File compactionFile = new File(compactionFileName);
      Files.write(compactionFile.toPath(),
          compactionLogs.get(i).getBytes(UTF_8));
    }

    RocksDBCheckpointDiffer differ =
        new RocksDBCheckpointDiffer(sstBackUpDirName,
            compactionLogDirName,
            null,
            MINUTES.toMillis(10));

    differ.loadAllCompactionLogs();

    differ.pruneOlderSnapshotsWithCompactionHistory();

    Set<String> expectedNodes = new HashSet<>(
        Arrays.asList("000054", "000052", "000050", "000048", "000059",
            "000055", "000056", "000060", "000057", "000058")
    );

    Set<String> expectedArcs = new HashSet<>(
        Arrays.asList(
            "000059->000054",
            "000055->000054",
            "000056->000054",
            "000060->000054",
            "000057->000054",
            "000058->000054",
            "000059->000052",
            "000055->000052",
            "000056->000052",
            "000060->000052",
            "000057->000052",
            "000058->000052",
            "000059->000050",
            "000055->000050",
            "000056->000050",
            "000060->000050",
            "000057->000050",
            "000058->000050",
            "000059->000048",
            "000055->000048",
            "000056->000048",
            "000060->000048",
            "000057->000048",
            "000058->000048")
    );

    Set<String> actualNodes = differ.getForwardCompactionDAG().nodes().stream()
        .map(CompactionNode::getFileName)
        .collect(Collectors.toSet());

    Set<String> actualArcs = differ.getForwardCompactionDAG().edges().stream()
        .map(e -> e.source().getFileName() + "->" + e.target().getFileName())
        .collect(Collectors.toSet());


    assertEquals(expectedNodes, actualNodes);
    assertEquals(expectedArcs, actualArcs);

    for (int i = 0; i < 4; i++) {
      String compactionFileName = compactionLogDirName + "/0000" + i + ".log";
      File compactionFile = new File(compactionFileName);
      assertFalse(compactionFile.exists());
    }

    for (int i = 4; i < compactionLogs.size(); i++) {
      String compactionFileName = compactionLogDirName + "/0000" + i + ".log";
      File compactionFile = new File(compactionFileName);
      assertTrue(compactionFile.exists());
    }

    deleteDirectory(compactionLogDir);
    deleteDirectory(sstBackUpDir);
  }
}
