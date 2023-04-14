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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import static org.apache.hadoop.hdds.utils.db.DBStoreBuilder.DEFAULT_COLUMN_FAMILY_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test persistent map backed by RocksDB.
 */
public class TestRocksDbPersistentMap {
  private static File file;
  private static ManagedRocksDB db;
  private static ManagedDBOptions dbOptions;
  private static ManagedColumnFamilyOptions columnFamilyOptions;

  @BeforeAll
  public static void staticInit() throws RocksDBException {
    dbOptions = new ManagedDBOptions();
    dbOptions.setCreateIfMissing(true);
    columnFamilyOptions = new ManagedColumnFamilyOptions();

    file = new File("./test-persistent-map");
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

  @AfterEach
  public void teardown() throws RocksDBException {
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
  public void testRocksDBPersistentMap() throws IOException, RocksDBException {
    ColumnFamilyHandle columnFamily = null;
    try {
      CodecRegistry codecRegistry = new CodecRegistry();
      columnFamily = db.get().createColumnFamily(new ColumnFamilyDescriptor(
          codecRegistry.asRawData("testMap"), columnFamilyOptions));

      PersistentMap<String, String> persistentMap = new RocksDbPersistentMap<>(
          db,
          columnFamily,
          codecRegistry,
          String.class,
          String.class
      );

      List<String> keys = Arrays.asList("Key1", "Key2", "Key3", "Key1", "Key2");
      List<String> values =
          Arrays.asList("value1", "value2", "Value3", "Value1", "Value2");

      Map<String, String> expectedMap = new HashMap<>();

      for (int i = 0; i < keys.size(); i++) {
        String key = keys.get(i);
        String value = values.get(i);

        persistentMap.put(key, value);
        expectedMap.put(key, value);
      }

      for (Map.Entry<String, String> entry : expectedMap.entrySet()) {
        assertEquals(entry.getValue(), persistentMap.get(entry.getKey()));
      }
    } finally {
      if (columnFamily != null) {
        db.get().dropColumnFamily(columnFamily);
        columnFamily.close();
      }
    }
  }
}
