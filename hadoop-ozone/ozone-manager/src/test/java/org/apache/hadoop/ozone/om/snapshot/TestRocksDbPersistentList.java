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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.util.ClosableIterator;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import static org.apache.hadoop.hdds.utils.db.DBStoreBuilder.DEFAULT_COLUMN_FAMILY_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test persistent list backed by RocksDB.
 */
public class TestRocksDbPersistentList {
  private static File file;
  private static ManagedRocksDB db;
  private static ManagedDBOptions dbOptions;
  private static ManagedColumnFamilyOptions columnFamilyOptions;

  @BeforeAll
  public static void staticInit() throws RocksDBException {
    dbOptions = new ManagedDBOptions();
    dbOptions.setCreateIfMissing(true);
    columnFamilyOptions = new ManagedColumnFamilyOptions();

    file = new File("./test-persistent-list");
    if (!file.mkdirs() && !file.exists()) {
      throw new IllegalArgumentException("Unable to create directory " +
          file);
    }

    String absolutePath = Paths.get(file.toString(), "rocks.db").toFile()
        .getAbsolutePath();

    List<ColumnFamilyDescriptor> columnFamilyDescriptors =
        Collections.singletonList(new ColumnFamilyDescriptor(
            StringUtils.string2Bytes(DEFAULT_COLUMN_FAMILY_NAME),
            columnFamilyOptions));

    List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    db = ManagedRocksDB.open(dbOptions, absolutePath, columnFamilyDescriptors,
        columnFamilyHandles);
  }

  @AfterAll
  public static void staticTearDown() {
    if (dbOptions != null) {
      dbOptions.close();
    }
    if (columnFamilyOptions != null) {
      columnFamilyOptions.close();
    }
    if (db != null) {
      db.close();
    }

    GenericTestUtils.deleteDirectory(file);
  }

  @Test
  public void testRocksDBPersistentList() throws IOException, RocksDBException {
    ColumnFamilyHandle columnFamily = null;
    try {
      final CodecRegistry codecRegistry = CodecRegistry.newBuilder().build();

      columnFamily = db.get().createColumnFamily(new ColumnFamilyDescriptor(
          codecRegistry.asRawData("testSet"), columnFamilyOptions));


      PersistentList<String> persistentList = new RocksDbPersistentList<>(
          db,
          columnFamily,
          codecRegistry,
          String.class
      );

      List<String> testList = Arrays.asList("e1", "e2", "e3", "e1", "e2");
      testList.forEach(persistentList::add);

      try (ClosableIterator<String> iterator = persistentList.iterator()) {
        int index = 0;

        while (iterator.hasNext()) {
          assertEquals(iterator.next(), testList.get(index++));
        }
        assertEquals(testList.size(), index);
      }
    } finally {
      if (columnFamily != null) {
        db.get().dropColumnFamily(columnFamily);
        columnFamily.close();
      }
    }
  }
}
