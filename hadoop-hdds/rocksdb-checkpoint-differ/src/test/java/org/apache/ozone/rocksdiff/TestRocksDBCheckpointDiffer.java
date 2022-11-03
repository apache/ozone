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
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.DEBUG_DAG_LIVE_NODES;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.DEBUG_READ_ALL_DB_KEYS;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.DifferSnapshotInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
  private final ArrayList<DifferSnapshotInfo> snapshots = new ArrayList<>();

  /**
   * Graph type.
   */
  enum GType {
    FNAME,
    KEYSIZE,
    CUMUTATIVE_SIZE
  }

  @BeforeAll
  public static void init() {
    // Checkpoint differ log level. Set to DEBUG for verbose output
    GenericTestUtils.setLogLevel(RocksDBCheckpointDiffer.getLog(), Level.INFO);
    // Test class log level. Set to DEBUG for verbose output
    GenericTestUtils.setLogLevel(TestRocksDBCheckpointDiffer.LOG, Level.INFO);
  }

  @Test
  void testMain() throws Exception {

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
        metadataDirStr, sstDirStr, clDirStr, dbLocation);

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

    differ.traverseGraph(
        differ.getBackwardCompactionDAG(),
        differ.getForwardCompactionDAG());

    diffAllSnapshots(differ);

    if (LOG.isDebugEnabled()) {
      differ.dumpCompactionNodeTable();
    }

    for (GType gtype : GType.values()) {
      String fname = "fwdGraph_" + gtype +  ".png";
      String rname = "reverseGraph_" + gtype + ".png";
/*
      differ.pngPrintMutableGrapth(differ.getCompactionFwdDAG(), fname, gtype);
      differ.pngPrintMutableGrapth(
          differ.getCompactionReverseDAG(), rname, gtype);
 */
    }

    rocksDB.close();
  }

  private String getRandomString(Random random, int length) {
    // Ref: https://www.baeldung.com/java-random-string
    final int leftLimit = 48; // numeral '0'
    final int rightLimit = 122; // letter 'z'

    return random.ints(leftLimit, rightLimit + 1)
        .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
        .limit(length)
        .collect(StringBuilder::new,
            StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
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
        new DifferSnapshotInfo(cpPath, snapshotId, snapshotGeneration);
    this.snapshots.add(currentSnapshot);

    // Same as what OmSnapshotManager#createOmSnapshotCheckpoint would do
    differ.appendSequenceNumberToCompactionLog(dbLatestSequenceNumber);

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

    Random random = new Random();
    // key-value
    for (int i = 0; i < NUM_ROW; ++i) {
      String generatedString = getRandomString(random, 7);
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

  /**
   * RocksDB.DEFAULT_COLUMN_FAMILY.
   */
  private void updateRocksDBInstance(String dbPathArg, RocksDB rocksDB) {
    System.out.println("Updating RocksDB instance at :" + dbPathArg);

    try (Options options = new Options().setCreateIfMissing(true)) {
      if (rocksDB == null) {
        rocksDB = RocksDB.open(options, dbPathArg);
      }

      Random random = new Random();
      // key-value
      for (int i = 0; i < NUM_ROW; ++i) {
        String generatedString = getRandomString(random, 7);
        String keyStr = " MyUpdated" + generatedString + "StringKey" + i;
        String valueStr = " My Updated" + generatedString + "StringValue" + i;
        byte[] key = keyStr.getBytes(UTF_8);
        rocksDB.put(key, valueStr.getBytes(UTF_8));
        System.out.println(toStr(rocksDB.get(key)));
      }
    } catch (RocksDBException e) {
      e.printStackTrace();
    }
  }

  /**
   * RocksDB.DEFAULT_COLUMN_FAMILY.
   */
  public void testDefaultColumnFamilyOriginal() {
    System.out.println("testDefaultColumnFamily begin...");

    try (Options options = new Options().setCreateIfMissing(true)) {
      try (RocksDB rocksDB = RocksDB.open(options, "./rocksdb-data")) {
        // key-value
        byte[] key = "Hello".getBytes(UTF_8);
        rocksDB.put(key, "World".getBytes(UTF_8));

        System.out.println(toStr(rocksDB.get(key)));

        rocksDB.put("SecondKey".getBytes(UTF_8), "SecondValue".getBytes(UTF_8));

        // List
        List<byte[]> keys = asList(key, "SecondKey".getBytes(UTF_8),
            "missKey".getBytes(UTF_8));
        List<byte[]> values = rocksDB.multiGetAsList(keys);
        for (int i = 0; i < keys.size(); i++) {
          System.out.println("multiGet " + toStr(keys.get(i)) + ":" +
              (values.get(i) != null ? toStr(values.get(i)) : null));
        }

        // [key - value]
        RocksIterator iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
          System.out.println("iterator key:" + toStr(iter.key()) + ", " +
              "iter value:" + toStr(iter.value()));
        }

        // key
        rocksDB.delete(key);
        System.out.println("after remove key:" + toStr(key));

        iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
          System.out.println("iterator key:" + toStr(iter.key()) + ", " +
              "iter value:" + toStr(iter.value()));
        }
      }
    } catch (RocksDBException e) {
      e.printStackTrace();
    }
  }

  // (table)
  public void testCertainColumnFamily() {
    System.out.println("\ntestCertainColumnFamily begin...");
    try (ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
        .optimizeUniversalStyleCompaction()) {
      String cfName = "my-first-columnfamily";
      // list of column family descriptors, first entry must always be
      // default column family
      final List<ColumnFamilyDescriptor> cfDescriptors = asList(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
          new ColumnFamilyDescriptor(cfName.getBytes(UTF_8), cfOpts)
      );

      List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
      try (DBOptions dbOptions = new DBOptions()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true);
          RocksDB rocksDB = RocksDB.open(dbOptions,
              "./rocksdb-data-cf/", cfDescriptors, cfHandles)) {
        ColumnFamilyHandle cfHandle = cfHandles.stream().filter(x -> {
          try {
            return (toStr(x.getName())).equals(cfName);
          } catch (RocksDBException e) {
            return false;
          }
        }).collect(Collectors.toList()).get(0);

        // key/value
        String key = "FirstKey";
        rocksDB.put(cfHandles.get(0), key.getBytes(UTF_8),
            "FirstValue".getBytes(UTF_8));
        // key
        byte[] getValue = rocksDB.get(cfHandles.get(0), key.getBytes(UTF_8));
        LOG.debug("get Value: " + toStr(getValue));
        // key/value
        rocksDB.put(cfHandles.get(1), "SecondKey".getBytes(UTF_8),
            "SecondValue".getBytes(UTF_8));

        List<byte[]> keys = asList(key.getBytes(UTF_8),
            "SecondKey".getBytes(UTF_8));
        List<ColumnFamilyHandle> cfHandleList = asList(cfHandle, cfHandle);

        // key
        List<byte[]> values = rocksDB.multiGetAsList(cfHandleList, keys);
        for (int i = 0; i < keys.size(); i++) {
          LOG.debug("multiGet:" + toStr(keys.get(i)) + "--" +
              (values.get(i) == null ? null : toStr(values.get(i))));
        }

        List<LiveFileMetaData> liveFileMetaDataList =
            rocksDB.getLiveFilesMetaData();
        for (LiveFileMetaData m : liveFileMetaDataList) {
          System.out.println("Live File Metadata");
          System.out.println("\tFile :" + m.fileName());
          System.out.println("\tTable :" + toStr(m.columnFamilyName()));
          System.out.println("\tKey Range :" + toStr(m.smallestKey()) +
              " " + "<->" + toStr(m.largestKey()));
        }

        // key
        rocksDB.delete(cfHandle, key.getBytes(UTF_8));

        // key
        RocksIterator iter = rocksDB.newIterator(cfHandle);
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
          System.out.println("Iterator:" + toStr(iter.key()) + ":" +
              toStr(iter.value()));
        }
      } finally {
        // NOTE frees the column family handles before freeing the db
        for (final ColumnFamilyHandle cfHandle : cfHandles) {
          cfHandle.close();
        }
      }
    } catch (RocksDBException e) {
      e.printStackTrace();
    } // frees the column family options
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
          differ.printMutableGraphFromAGivenNode(m.fileName(), m.level(),
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
}