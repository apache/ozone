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
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.graph.MutableGraph;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.NodeComparator;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_PRUNE_DAEMON_RUN_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_DAG_DAEMON_RUN_INTERVAL_DEFAULT;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.COMPACTION_LOG_FILE_NAME_SUFFIX;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.DEBUG_DAG_LIVE_NODES;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.DEBUG_READ_ALL_DB_KEYS;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.SST_FILE_EXTENSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test RocksDBCheckpointDiffer basic functionality.
 */
public class TestRocksDBCheckpointDiffer {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRocksDBCheckpointDiffer.class);

  private static final int NUM_ROW = 250000;
  private static final int SNAPSHOT_EVERY_SO_MANY_KEYS = 49999;

  /**
   * RocksDB checkpoint path prefix.
   */
  private static final String CP_PATH_PREFIX = "rocksdb-cp-";
  private final List<DifferSnapshotInfo> snapshots = new ArrayList<>();

  private final List<List<ColumnFamilyHandle>> colHandles = new ArrayList<>();

  private final String activeDbDirName = "./rocksdb-data";
  private final String metadataDirName = "./metadata";
  private final String compactionLogDirName = "compaction-log";
  private final String sstBackUpDirName = "compaction-sst-backup";
  private File activeDbDir;
  private File metadataDirDir;
  private File compactionLogDir;
  private File sstBackUpDir;
  private ConfigurationSource config;
  private ExecutorService executorService = Executors.newCachedThreadPool();
  private RocksDBCheckpointDiffer rocksDBCheckpointDiffer;
  private RocksDB activeRocksDB;
  private ColumnFamilyHandle keyTableCFHandle;
  private ColumnFamilyHandle compactionLogTableCFHandle;

  @BeforeEach
  public void init() throws RocksDBException {
    // Checkpoint differ log level. Set to DEBUG for verbose output
    GenericTestUtils.setLogLevel(RocksDBCheckpointDiffer.getLog(), Level.INFO);
    // Test class log level. Set to DEBUG for verbose output
    GenericTestUtils.setLogLevel(TestRocksDBCheckpointDiffer.LOG, Level.INFO);

    activeDbDir = new File(activeDbDirName);
    createDir(activeDbDir, activeDbDirName);

    metadataDirDir = new File(metadataDirName);
    createDir(metadataDirDir, metadataDirName);

    compactionLogDir = new File(metadataDirName, compactionLogDirName);
    createDir(compactionLogDir, metadataDirName + "/" + compactionLogDirName);

    sstBackUpDir = new File(metadataDirName, sstBackUpDirName);
    createDir(sstBackUpDir, metadataDirName + "/" + sstBackUpDirName);

    config = Mockito.mock(ConfigurationSource.class);

    Mockito.when(config.getTimeDuration(
        OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED,
        OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED_DEFAULT,
        TimeUnit.MILLISECONDS)).thenReturn(MINUTES.toMillis(10));

    Mockito.when(config.getTimeDuration(
        OZONE_OM_SNAPSHOT_COMPACTION_DAG_PRUNE_DAEMON_RUN_INTERVAL,
        OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_DAG_DAEMON_RUN_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS)).thenReturn(0L);

    rocksDBCheckpointDiffer = new RocksDBCheckpointDiffer(metadataDirName,
        sstBackUpDirName,
        compactionLogDirName,
        activeDbDirName,
        config);

    ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
        .optimizeUniversalStyleCompaction();
    List<ColumnFamilyDescriptor> cfDescriptors = getCFDescriptorList(cfOpts);
    List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
    DBOptions dbOptions = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);

    rocksDBCheckpointDiffer.setRocksDBForCompactionTracking(dbOptions);
    activeRocksDB = RocksDB.open(dbOptions, activeDbDirName, cfDescriptors,
        cfHandles);
    keyTableCFHandle = cfHandles.get(1);
    compactionLogTableCFHandle = cfHandles.get(4);

    rocksDBCheckpointDiffer.setCurrentCompactionLog(
        activeRocksDB.getLatestSequenceNumber());
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
    IOUtils.closeQuietly(compactionLogTableCFHandle);
    IOUtils.closeQuietly(activeRocksDB);
    deleteDirectory(compactionLogDir);
    deleteDirectory(sstBackUpDir);
    deleteDirectory(metadataDirDir);
    deleteDirectory(activeDbDir);
  }

  /**
   * Test cases for testGetSSTDiffListWithoutDB.
   */
  private static Stream<Arguments> casesGetSSTDiffListWithoutDB() {

    DifferSnapshotInfo snapshotInfo1 = new DifferSnapshotInfo(
        "/path/to/dbcp1", UUID.randomUUID(), 3008L, null, null);
    DifferSnapshotInfo snapshotInfo2 = new DifferSnapshotInfo(
        "/path/to/dbcp2", UUID.randomUUID(), 14980L, null, null);
    DifferSnapshotInfo snapshotInfo3 = new DifferSnapshotInfo(
        "/path/to/dbcp3", UUID.randomUUID(), 17975L, null, null);
    DifferSnapshotInfo snapshotInfo4 = new DifferSnapshotInfo(
        "/path/to/dbcp4", UUID.randomUUID(), 18000L, null, null);

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

    boolean exceptionThrown = false;
    long createdTime = System.currentTimeMillis();

    String compactionLog = ""
        // Snapshot 0
        + "S 1000 df6410c7-151b-4e90-870e-5ef12875acd5 " + createdTime + " \n"
        // Additional "compaction" to trigger and test early exit condition
        + "C 1291 000001,000002:000062\n"
        // Snapshot 1
        + "S 3008 ef6410c7-151b-4e90-870e-5ef12875acd5 " + createdTime + " \n"
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
        + "S 14980 e7ad72f8-52df-4430-93f6-0ee91d4a47fd " + createdTime + "\n"
        + "C 16192 000098,000096,000085,000078,000071,000064,000060,000052"
        + ":000099\n"
        + "C 16762 000105,000095,000088:000107\n"
        // Snapshot 3
        + "S 17975 4f084f6e-ed3d-4780-8362-f832303309ea " + createdTime + "\n";

    // Construct DAG from compaction log input
    Arrays.stream(compactionLog.split("\n")).forEach(
        rocksDBCheckpointDiffer::processCompactionLogLine);
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
          actualDiffSstFiles,
          Collections.emptyMap());
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
    writeKeysAndCheckpointing();
    readRocksDBInstance(activeDbDirName, activeRocksDB, null,
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
      Assertions.assertEquals(expectedLinks, asList(
              "000017.sst", "000019.sst", "000021.sst", "000023.sst",
          "000024.sst", "000026.sst", "000029.sst"));
    }

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
    Assertions.assertEquals(snapshots.size(), expectedDifferResult.size());

    int index = 0;
    for (DifferSnapshotInfo snap : snapshots) {
      // Returns a list of SST files to be fed into RocksDiff
      List<String> sstDiffList = differ.getSSTDiffList(src, snap,
          Collections.emptyMap());
      LOG.info("SST diff list from '{}' to '{}': {}",
          src.getDbPath(), snap.getDbPath(), sstDiffList);

      Assertions.assertEquals(expectedDifferResult.get(index), sstDiffList);
      ++index;
    }
  }

  /**
   * Helper function that creates an RDB checkpoint (= Ozone snapshot).
   */
  private void createCheckpoint(RocksDBCheckpointDiffer differ,
      RocksDB rocksDB) throws RocksDBException {

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

    createCheckPoint(activeDbDirName, cpPath, rocksDB);
    final UUID snapshotId = UUID.randomUUID();
    List<ColumnFamilyHandle> colHandle = new ArrayList<>();
    colHandles.add(colHandle);
    final DifferSnapshotInfo currentSnapshot =
        new DifferSnapshotInfo(cpPath, snapshotId, snapshotGeneration, null,
            ManagedRocksDB.openReadOnly(cpPath, getColumnFamilyDescriptors(),
                colHandle));
    this.snapshots.add(currentSnapshot);

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

  /**
   * Get a list of relevant column family descriptors.
   * @param cfOpts ColumnFamilyOptions
   * @return List of ColumnFamilyDescriptor
   */
  static List<ColumnFamilyDescriptor> getCFDescriptorList(
      ColumnFamilyOptions cfOpts) {
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
      String generatedString = RandomStringUtils.randomAlphabetic(7);
      String keyStr = "Key-" + i + "-" + generatedString;
      String valueStr = "Val-" + i + "-" + generatedString;
      byte[] key = keyStr.getBytes(UTF_8);
      // Put entry in keyTable
      activeRocksDB.put(keyTableCFHandle, key, valueStr.getBytes(UTF_8));
      if (i % SNAPSHOT_EVERY_SO_MANY_KEYS == 0) {
        createCheckpoint(rocksDBCheckpointDiffer, activeRocksDB);
      }
    }
    createCheckpoint(rocksDBCheckpointDiffer, activeRocksDB);
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

  // Read from a given RocksDB instance and optionally write all the
  // keys to a given file.
  private void readRocksDBInstance(String dbPathArg,
                                   RocksDB rocksDB,
                                   FileWriter file,
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
                          Long.parseLong(sstFile.substring(0, 6)),
                          null,
                          null,
                          null
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
    Set<String> actualFileNodesRemoved =
        rocksDBCheckpointDiffer.pruneBackwardDag(originalDag, levelToBeRemoved);
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
    Set<String> actualFileNodesRemoved =
        rocksDBCheckpointDiffer.pruneForwardDag(originalDag, levelToBeRemoved);
    Assertions.assertEquals(expectedDag, originalDag);
    Assertions.assertEquals(actualFileNodesRemoved, expectedFileNodesRemoved);
  }

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

    String compactionLogFileWithoutSnapshot2 = "C 3500 000027,000030,000028," +
        "000031,000029,000039,000037,000035,000033:000040,000044,000042," +
        "000043,000046,000041,000045\n";

    String compactionLogFileWithoutSnapshot3 = "C 4500 000040,000044,000042," +
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

    Set<String> expectedNodes = new HashSet<>(
        Arrays.asList("000059", "000055", "000056", "000060", "000057",
            "000058")
    );

    return Stream.of(
        Arguments.of("Each compaction log file has only one snapshot and one" +
                " compaction statement except first log file.",
            Arrays.asList(compactionLogFile0, compactionLogFile1,
                compactionLogFile2, compactionLogFile3, compactionLogFile4),
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
            expectedNodes,
            4,
            0
        ),
        Arguments.of("Compaction log doesn't have snapshot because OM" +
                " restarted. Restart happened after snapshot to be deleted.",
            Arrays.asList(compactionLogFile0, compactionLogFile1,
                compactionLogFile2, compactionLogFile3,
                compactionLogFileWithoutSnapshot3,
                compactionLogFileOnlyWithSnapshot4),
            expectedNodes,
            4,
            0
        ),
        Arguments.of("No compaction happened in between two snapshots.",
            Arrays.asList(compactionLogFile0, compactionLogFile1,
                compactionLogFile2, compactionLogFile3,
                compactionLogFileOnlyWithSnapshot1,
                compactionLogFileOnlyWithSnapshot2, compactionLogFile4),
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
            Collections.emptySet(),
            0,
            0
        ),
        Arguments.of("No file exists because compaction has not happened" +
                " and snapshot is not taken.",
            Collections.emptyList(),
            Collections.emptySet(),
            0,
            0
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
      Set<String> expectedNodes,
      int expectedNumberOfLogEntriesBeforePruning,
      int expectedNumberOfLogEntriesAfterPruning
  ) throws IOException, ExecutionException, InterruptedException,
      TimeoutException {
    List<File> filesCreated = new ArrayList<>();

    for (int i = 0; i < compactionLogs.size(); i++) {
      String compactionFileName = metadataDirName + "/" + compactionLogDirName
          + "/0000" + i + COMPACTION_LOG_FILE_NAME_SUFFIX;
      File compactionFile = new File(compactionFileName);
      Files.write(compactionFile.toPath(),
          compactionLogs.get(i).getBytes(UTF_8));
      filesCreated.add(compactionFile);
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

    for (int i = 0; i < compactionLogs.size(); i++) {
      File compactionFile = filesCreated.get(i);
      assertFalse(compactionFile.exists());
    }
    assertEquals(expectedNumberOfLogEntriesAfterPruning,
        countEntriesInCompactionLogTable());
  }

  private int countEntriesInCompactionLogTable() {
    try (ManagedRocksIterator iterator = new ManagedRocksIterator(
        activeRocksDB.newIterator(compactionLogTableCFHandle))) {
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
          () -> future.get(5000, TimeUnit.MILLISECONDS));
    }
    // Confirm consumer finishes when unlocked.
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));
  }

  private static Stream<Arguments> sstFilePruningScenarios() {
    return Stream.of(
        Arguments.of("Case 1: No compaction.",
            "",
            Arrays.asList("000015", "000013", "000011", "000009"),
            Arrays.asList("000015", "000013", "000011", "000009")
        ),
        Arguments.of("Case 2: One level compaction.",
            "C 1 000015,000013,000011,000009:000018,000016,000017\n",
            Arrays.asList("000015", "000013", "000011", "000009", "000018",
                "000016", "000017", "000026", "000024", "000022", "000020"),
            Arrays.asList("000015", "000013", "000011", "000009", "000026",
                "000024", "000022", "000020")
        ),
        Arguments.of("Case 3: Multi-level compaction.",
            "C 1 000015,000013,000011,000009:000018,000016,000017\n" +
                "C 2 000018,000016,000017,000026,000024,000022,000020:000027," +
                "000030,000028,000031,000029\n" +
                "C 3 000027,000030,000028,000031,000029,000039,000037,000035," +
                "000033:000040,000044,000042,000043,000046,000041,000045\n" +
                "C 4 000040,000044,000042,000043,000046,000041,000045,000054," +
                "000052,000050,000048:000059,000055,000056,000060,000057," +
                "000058\n",
            Arrays.asList("000015", "000013", "000011", "000009", "000018",
                "000016", "000017", "000026", "000024", "000022", "000020",
                "000027", "000030", "000028", "000031", "000029", "000039",
                "000037", "000035", "000033", "000040", "000044", "000042",
                "000043", "000046", "000041", "000045", "000054", "000052",
                "000050", "000048", "000059", "000055", "000056", "000060",
                "000057", "000058"),
            Arrays.asList("000013", "000024", "000035", "000011", "000022",
                "000033", "000039", "000015", "000026", "000037", "000048",
                "000009", "000050", "000054", "000020", "000052")
        )
    );
  }

  /**
   * End-to-end test for SST file pruning.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("sstFilePruningScenarios")
  public void testSstFilePruning(
      String description,
      String compactionLog,
      List<String> initialFiles,
      List<String> expectedFiles
  ) throws IOException, ExecutionException, InterruptedException,
      TimeoutException {
    createFileWithContext(metadataDirName + "/" + compactionLogDirName
            + "/compaction_log" + COMPACTION_LOG_FILE_NAME_SUFFIX,
        compactionLog);

    for (String fileName : initialFiles) {
      createFileWithContext(sstBackUpDir + "/" + fileName + SST_FILE_EXTENSION,
          fileName);
    }

    rocksDBCheckpointDiffer.loadAllCompactionLogs();

    waitForLock(rocksDBCheckpointDiffer,
        RocksDBCheckpointDiffer::pruneSstFiles);

    Set<String> actualFileSetAfterPruning;
    try (Stream<Path> pathStream = Files.list(
            Paths.get(metadataDirName + "/" + sstBackUpDirName))
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
  }

  private void createFileWithContext(String fileName, String context)
      throws IOException {
    try (FileOutputStream fileOutputStream = new FileOutputStream(fileName)) {
      fileOutputStream.write(context.getBytes(UTF_8));
    }
  }
}
