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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Test persistent set backed by RocksDB.
 */
public class TestRocksDbPersistentSet {
  private ColumnFamilyHandle columnFamily;
  private ManagedRocksDB db;
  private File file;
  private CodecRegistry codecRegistry;

  @BeforeEach
  public void init() throws RocksDBException, IOException {
    ManagedOptions options = new ManagedOptions();
    options.setCreateIfMissing(true);
    file = new File("./test-persistent-set");
    if (!file.mkdirs() && !file.exists()) {
      throw new IllegalArgumentException("Unable to create directory " +
          file);
    }

    db = ManagedRocksDB.open(options,
        Paths.get(file.toString(), "rocks.db").toFile().getAbsolutePath());

    codecRegistry = new CodecRegistry();

    columnFamily = db.get().createColumnFamily(
        new ColumnFamilyDescriptor(
            codecRegistry.asRawData("testSet"),
            new ManagedColumnFamilyOptions()));
  }

  @AfterEach
  public void teardown() throws RocksDBException {
    deleteDirectory(file);

    if (columnFamily != null && db != null) {
      db.get().dropColumnFamily(columnFamily);
    }
    if (columnFamily != null) {
      columnFamily.close();
    }
    if (db != null) {
      db.close();
    }
  }

  @Test
  public void testRocksDBPersistentSetForString() {
    PersistentSet<String> persistentSet = new RocksDbPersistentSet<>(
        db,
        columnFamily,
        codecRegistry,
        String.class
    );

    List<String> testList = Arrays.asList("e1", "e1", "e2", "e2", "e3");
    Set<String> testSet = new HashSet<>(testList);

    testList.forEach(persistentSet::add);

    Iterator<String> iterator = persistentSet.iterator();
    Iterator<String> setIterator = testSet.iterator();

    while (iterator.hasNext()) {
      assertEquals(iterator.next(), setIterator.next());
    }
    assertFalse(setIterator.hasNext());
  }

  @Test
  public void testRocksDBPersistentSetForLong() {
    PersistentSet<Long> persistentSet = new RocksDbPersistentSet<>(
        db,
        columnFamily,
        codecRegistry,
        Long.class
    );

    List<Long> testList = Arrays.asList(1L, 1L, 2L, 2L, 3L, 4L, 5L, 5L);
    Set<Long> testSet = new HashSet<>(testList);

    testList.forEach(persistentSet::add);

    Iterator<Long> iterator = persistentSet.iterator();
    Iterator<Long> setIterator = testSet.iterator();

    while (iterator.hasNext()) {
      assertEquals(iterator.next(), setIterator.next());
    }
    assertFalse(setIterator.hasNext());
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
