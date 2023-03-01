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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

/**
 * Test persistent map backed by RocksDB.
 */
public class TestRocksDbPersistentMap {

  private ColumnFamilyHandle columnFamily;
  private ManagedRocksDB db;
  private File file;
  private CodecRegistry codecRegistry;

  @BeforeEach
  public void init() throws RocksDBException, IOException {
    ManagedOptions options = new ManagedOptions();
    options.setCreateIfMissing(true);

    file = new File("./test-persistent-map");
    if (!file.mkdirs() && !file.exists()) {
      throw new IllegalArgumentException("Unable to create directory " +
          file);
    }

    db = ManagedRocksDB.open(options,
        Paths.get(file.toString(), "rocks.db").toFile().getAbsolutePath());

    codecRegistry = new CodecRegistry();

    columnFamily = db.get().createColumnFamily(
        new ColumnFamilyDescriptor(
            codecRegistry.asRawData("testList"),
            new ManagedColumnFamilyOptions()));
  }

  @AfterEach
  public void clean() throws RocksDBException {
    deleteDirectory(file);

    if (columnFamily != null && db != null) {
      db.get()
          .dropColumnFamily(columnFamily);
    }
    if (columnFamily != null) {
      columnFamily.close();
    }
    if (db != null) {
      db.close();
    }
  }

  @Test
  public void testRocksDBPersistentMap() {
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
