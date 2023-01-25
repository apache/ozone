/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.snapshot;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.IntegerCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test persistent list backed by RocksDB.
 */
public class TestRocksDbPersistentList {
  private ColumnFamilyHandle columnFamily;
  private RocksDB rocksDB;
  private File file;
  private CodecRegistry codecRegistry;

  @BeforeEach
  public void init() throws RocksDBException, IOException {
    Options options = new Options().setCreateIfMissing(true);
    file = new File("./test-persistent-list");
    if (!file.mkdirs() && !file.exists()) {
      throw new IllegalArgumentException("Unable to create directory " +
          file);
    }

    rocksDB = RocksDB.open(options,
        Paths.get(file.toString(), "rocks.db").toFile().getAbsolutePath());

    codecRegistry = new CodecRegistry();
    codecRegistry.addCodec(Integer.class, new IntegerCodec());

    columnFamily = rocksDB.createColumnFamily(
        new ColumnFamilyDescriptor(
            codecRegistry.asRawData("testList"),
            new ColumnFamilyOptions()));
  }

  @AfterEach
  public void teardown() throws RocksDBException {
    deleteDirectory(file);

    if (columnFamily != null && rocksDB != null) {
      rocksDB.dropColumnFamily(columnFamily);
    }
    if (columnFamily != null) {
      columnFamily.close();
    }
    if (rocksDB != null) {
      rocksDB.close();
    }
  }

  @ValueSource(ints = {0, 3})
  @ParameterizedTest
  public void testRocksDBPersistentList(int index) {
    PersistentList<String> persistentList = new RocksDbPersistentList<>(
        rocksDB,
        columnFamily,
        codecRegistry,
        String.class
    );

    List<String> testList = Arrays.asList("e1", "e2", "e3", "e1", "e2");
    testList.forEach(persistentList::add);

    Iterator<String> iterator = index == 0 ? persistentList.iterator() :
        persistentList.iterator(index);

    while (iterator.hasNext()) {
      assertEquals(iterator.next(), testList.get(index++));
    }
  }

  private boolean deleteDirectory(File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (File content : allContents) {
        if (!deleteDirectory(content)) {
          return false;
        }
      }
    }
    return directoryToBeDeleted.delete();
  }
}
