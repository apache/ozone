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

import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.DEBUG_DAG_LIVE_NODES;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.DEBUG_READ_ALL_DB_KEYS;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

////CHECKSTYLE:OFF
public class TestRocksDBCheckpointDiffer {

  private static final String dbPath   = "./rocksdb-data";
  private static final int NUM_ROW = 25000000;
  private static final int SNAPSHOT_EVERY_SO_MANY_KEYS = 999999;

  // keeps track of all the snapshots created so far.
  private static int lastSnapshotCounter = 0;
  private static String lastSnapshotPrefix = "snap_id_";


  public static void main(String[] args) throws Exception {
    TestRocksDBCheckpointDiffer tester= new TestRocksDBCheckpointDiffer();
    RocksDBCheckpointDiffer differ = new RocksDBCheckpointDiffer(
        "./rocksdb-data",
        1000,
        "./rocksdb-data-cp",
        "./SavedCompacted_Files/",
        "./rocksdb-data-cf/",
        0,
        "snap_id_");
    lastSnapshotPrefix = "snap_id_" + lastSnapshotCounter;
    RocksDB rocksDB = tester.CreateRocksDBInstance(dbPath, differ);
    Thread.sleep(10000);

    tester.ReadRocksDBInstance(dbPath, rocksDB, null, differ);
    differ.printAllSnapshots();
    differ.traverseGraph(
        differ.getCompactionReverseDAG(),
        differ.getCompactionFwdDAG());
    differ.diffAllSnapshots();
    differ.dumpCompactioNodeTable();
    for (RocksDBCheckpointDiffer.GType gtype :
        RocksDBCheckpointDiffer.GType.values()) {
      String fname = "fwdGraph_" + gtype.toString() +  ".png";
      String rname = "reverseGraph_"+ gtype.toString() + ".png";

      //differ.pngPrintMutableGrapth(differ.getCompactionFwdDAG(),
      //  fname, gtype);
      //differ.pngPrintMutableGrapth(differ.getCompactionReverseDAG(), rname,
      //    gtype);
    }
    rocksDB.close();
  }

  //  Test Code to create sample RocksDB instance.
  public RocksDB CreateRocksDBInstance(String dbPathArg,
                                              RocksDBCheckpointDiffer differ)
      throws RocksDBException, InterruptedException {

    System.out.println("Creating RocksDB instance at :" + dbPathArg);

    RocksDB rocksDB = null;
    rocksDB = differ.getRocksDBInstanceWithCompactionTracking(dbPathArg);
    //
    // key-value
    for (int i = 0; i< NUM_ROW; ++i) {
      byte[] array = new byte[7]; // length is bounded by 7
      new Random().nextBytes(array);
      String keyStr = " My" + array + "StringKey" + i;
      String valueStr = " My " + array + "StringValue" + i;
      byte[] key = keyStr.getBytes();
      rocksDB.put(keyStr.getBytes(StandardCharsets.UTF_8),
          valueStr.getBytes(StandardCharsets.UTF_8));
      if (i % SNAPSHOT_EVERY_SO_MANY_KEYS == 0) {
        differ.createSnapshot(rocksDB);
      }
      //System.out.println(new String(rocksDB.get(key)));
    }
    differ.createSnapshot(rocksDB);
    return rocksDB;
  }

  //  RocksDB.DEFAULT_COLUMN_FAMILY
  private void UpdateRocksDBInstance(String dbPathArg, RocksDB rocksDB) {
    System.out.println("Updating RocksDB instance at :" + dbPathArg);
    //
    try (final Options options =
             new Options().setCreateIfMissing(true).
                 setCompressionType(CompressionType.NO_COMPRESSION)) {
      if (rocksDB == null) {
        rocksDB = RocksDB.open(options, dbPathArg);
      }
      // key-value
      for (int i = 0; i< NUM_ROW; ++i) {
        byte[] array = new byte[7]; // length is bounded by 7
        new Random().nextBytes(array);
        String keyStr = " MyUpdated" + array + "StringKey" + i;
        String valueStr = " My Updated" + array + "StringValue" + i;
        byte[] key = keyStr.getBytes();
        rocksDB.put(keyStr.getBytes(StandardCharsets.UTF_8),
            valueStr.getBytes(StandardCharsets.UTF_8));
        System.out.println(new String(rocksDB.get(key)));
      }
    } catch (RocksDBException e) {
      e.printStackTrace();
    }
  }

  //  RocksDB.DEFAULT_COLUMN_FAMILY
  public void testDefaultColumnFamilyOriginal() {
    System.out.println("testDefaultColumnFamily begin...");
    //
    try (final Options options = new Options().setCreateIfMissing(true)) {
      try (final RocksDB rocksDB = RocksDB.open(options, "./rocksdb-data")) {
        // key-value
        byte[] key = "Hello".getBytes();
        rocksDB.put(key, "World".getBytes());

        System.out.println(new String(rocksDB.get(key)));

        rocksDB.put("SecondKey".getBytes(), "SecondValue".getBytes());

        // List
        List<byte[]> keys = Arrays.asList(key, "SecondKey".getBytes(),
            "missKey".getBytes());
        List<byte[]> values = rocksDB.multiGetAsList(keys);
        for (int i = 0; i < keys.size(); i++) {
          System.out.println("multiGet " + new String(keys.get(i)) + ":" +
              (values.get(i) != null ? new String(values.get(i)) : null));
        }

        // [key - value]
        RocksIterator iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
          System.out.println("iterator key:" + new String(iter.key()) + ", " +
              "iter value:" + new String(iter.value()));
        }

        // key
        rocksDB.delete(key);
        System.out.println("after remove key:" + new String(key));

        iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
          System.out.println("iterator key:" + new String(iter.key()) + ", " +
              "iter value:" + new String(iter.value()));
        }
      }
    } catch (RocksDBException e) {
      e.printStackTrace();
    }
  }

  // (table)
  public void testCertainColumnFamily() {
    System.out.println("\ntestCertainColumnFamily begin...");
    try (final ColumnFamilyOptions cfOpts =
             new ColumnFamilyOptions().optimizeUniversalStyleCompaction()) {
      String cfName = "my-first-columnfamily";
      // list of column family descriptors, first entry must always be
      // default column family
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
          new ColumnFamilyDescriptor(cfName.getBytes(), cfOpts)
      );

      List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
      try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).
          setCreateMissingColumnFamilies(true);
           final RocksDB rocksDB = RocksDB.open(dbOptions, "./rocksdb-data-cf" +
               "/", cfDescriptors, cfHandles)) {
        ColumnFamilyHandle cfHandle = cfHandles.stream().filter(x -> {
          try {
            return (new String(x.getName())).equals(cfName);
          } catch (RocksDBException e) {
            return false;
          }
        }).collect(Collectors.toList()).get(0);

        // key/value
        String key = "FirstKey";
        rocksDB.put(cfHandles.get(0), key.getBytes(), "FirstValue".getBytes());
        // key
        byte[] getValue = rocksDB.get(cfHandles.get(0), key.getBytes());
        System.out.println("get Value : " + new String(getValue));
        // key/value
        rocksDB.put(cfHandles.get(1), "SecondKey".getBytes(),
            "SecondValue".getBytes());

        List<byte[]> keys = Arrays.asList(key.getBytes(),
            "SecondKey".getBytes());
        List<ColumnFamilyHandle> cfHandleList = Arrays.asList(cfHandle,
            cfHandle);
        // key
        List<byte[]> values = rocksDB.multiGetAsList(cfHandleList, keys);
        for (int i = 0; i < keys.size(); i++) {
          System.out.println("multiGet:" + new String(keys.get(i)) + "--" +
              (values.get(i) == null ? null : new String(values.get(i))));
        }
        //rocksDB.compactRange();
        //rocksDB.compactFiles();
        List<LiveFileMetaData> liveFileMetaDataList =
            rocksDB.getLiveFilesMetaData();
        for (LiveFileMetaData m : liveFileMetaDataList) {
          System.out.println("Live File Metadata");
          System.out.println("\tFile :" + m.fileName());
          System.out.println("\ttable :" + new String(m.columnFamilyName()));
          System.out.println("\tKey Range :" + new String(m.smallestKey()) +
              " " + "<->" + new String(m.largestKey()));
        }
        // key
        rocksDB.delete(cfHandle, key.getBytes());

        // key
        RocksIterator iter = rocksDB.newIterator(cfHandle);
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
          System.out.println("iterator:" + new String(iter.key()) + ":" +
              new String(iter.value()));
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
  //
  public void ReadRocksDBInstance(String dbPathArg, RocksDB rocksDB,
                                  FileWriter file,
                                  RocksDBCheckpointDiffer differ) {
    System.out.println("Reading RocksDB instance at : " + dbPathArg);
    boolean createdDB = false;
    //
    try (final Options options =
             new Options().setParanoidChecks(true)
                 .setCreateIfMissing(true)
                 .setCompressionType(CompressionType.NO_COMPRESSION)
                 .setForceConsistencyChecks(false)) {

      if (rocksDB == null) {
        rocksDB = RocksDB.openReadOnly(options, dbPathArg);
        createdDB = true;
      }

      List<LiveFileMetaData> liveFileMetaDataList =
          rocksDB.getLiveFilesMetaData();
      for (LiveFileMetaData m : liveFileMetaDataList) {
        System.out.println("Live File Metadata");
        System.out.println("\tFile :" + m.fileName());
        System.out.println("\tLevel :" + m.level());
        System.out.println("\ttable :" + new String(m.columnFamilyName()));
        System.out.println("\tKey Range :" + new String(m.smallestKey()) + " " +
            "<->" + new String(m.largestKey()));
        if (differ.debugEnabled(DEBUG_DAG_LIVE_NODES)) {
          differ.printMutableGraphFromAGivenNode(m.fileName(), m.level(),
              differ.getCompactionFwdDAG());
        }
      }

      if(differ.debugEnabled(DEBUG_READ_ALL_DB_KEYS)) {
        RocksIterator iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
          System.out.println("iterator key:" + new String(iter.key()) + ", " +
              "iter value:" + new String(iter.value()));
          if (file != null) {
            file.write("iterator key:" + new String(iter.key()) + ", iter " +
                "value:" + new String(iter.value()));
            file.write("\n");
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (createdDB){
        rocksDB.close();
      }
    }
  }

  private FileWriter createFile(String fileName)
      throws IOException {
    File file = new File(fileName);
    file.createNewFile();
    return new FileWriter(fileName);
  }
}