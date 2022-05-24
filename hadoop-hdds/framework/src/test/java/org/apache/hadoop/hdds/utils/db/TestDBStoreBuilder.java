/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.utils.db;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyOptions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * Tests RDBStore creation.
 */
public class TestDBStoreBuilder {

  @BeforeEach
  public void setUp(@TempDir Path tempDir) throws Exception {
    System.setProperty(DBConfigFromFile.CONFIG_DIR, tempDir.toString());
  }

  @Test
  public void builderWithoutAnyParams() {
    OzoneConfiguration conf = new OzoneConfiguration();
    Assertions.assertThrows(IOException.class,
        () -> DBStoreBuilder.newBuilder(conf).build());
  }

  @Test
  public void builderWithOneParamV1() {
    OzoneConfiguration conf = new OzoneConfiguration();
    Assertions.assertThrows(IOException.class,
        () -> DBStoreBuilder.newBuilder(conf).setName("Test.db").build());
  }

  @Test
  public void builderWithOneParamV2(@TempDir Path tempDir) {
    OzoneConfiguration conf = new OzoneConfiguration();
    Assertions.assertThrows(IOException.class,
        () -> DBStoreBuilder.newBuilder(conf).setPath(tempDir).build());
  }

  @Test
  public void builderWithOpenClose(@TempDir Path tempDir) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DBStore dbStore = DBStoreBuilder.newBuilder(conf)
        .setName("Test.db")
        .setPath(tempDir)
        .build();
    // Nothing to do just open and Close.
    dbStore.close();
  }

  @Test
  public void builderWithDoubleTableName(@TempDir Path tempDir)
      throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    // Registering a new table with the same name should replace the previous
    // one.
    DBStore dbStore = DBStoreBuilder.newBuilder(conf)
        .setName("Test.db")
        .setPath(tempDir)
        .addTable("FIRST")
        .addTable("FIRST", new ColumnFamilyOptions())
        .build();
    // Building should succeed without error.

    try (Table<byte[], byte[]> firstTable = dbStore.getTable("FIRST")) {
      byte[] key =
          RandomStringUtils.random(9).getBytes(StandardCharsets.UTF_8);
      byte[] value =
          RandomStringUtils.random(9).getBytes(StandardCharsets.UTF_8);
      firstTable.put(key, value);
      byte[] temp = firstTable.get(key);
      Assertions.assertArrayEquals(value, temp);
    }

    dbStore.close();
  }

  @Test
  public void builderWithDataWrites(@TempDir Path tempDir) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    try (DBStore dbStore = DBStoreBuilder.newBuilder(conf)
        .setName("Test.db")
        .setPath(tempDir)
        .addTable("First")
        .addTable("Second")
        .build()) {
      try (Table<byte[], byte[]> firstTable = dbStore.getTable("First")) {
        byte[] key =
            RandomStringUtils.random(9).getBytes(StandardCharsets.UTF_8);
        byte[] value =
            RandomStringUtils.random(9).getBytes(StandardCharsets.UTF_8);
        firstTable.put(key, value);
        byte[] temp = firstTable.get(key);
        Assertions.assertArrayEquals(value, temp);
      }

      try (Table secondTable = dbStore.getTable("Second")) {
        Assertions.assertTrue(secondTable.isEmpty());
      }
    }
  }

  @Test
  public void builderWithDiskProfileWrites(@TempDir Path tempDir)
      throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    try (DBStore dbStore = DBStoreBuilder.newBuilder(conf)
        .setName("Test.db")
        .setPath(tempDir)
        .addTable("First")
        .addTable("Second")
        .setProfile(DBProfile.DISK)
        .build()) {
      try (Table<byte[], byte[]> firstTable = dbStore.getTable("First")) {
        byte[] key =
            RandomStringUtils.random(9).getBytes(StandardCharsets.UTF_8);
        byte[] value =
            RandomStringUtils.random(9).getBytes(StandardCharsets.UTF_8);
        firstTable.put(key, value);
        byte[] temp = firstTable.get(key);
        Assertions.assertArrayEquals(value, temp);
      }

      try (Table secondTable = dbStore.getTable("Second")) {
        Assertions.assertTrue(secondTable.isEmpty());
      }
    }
  }


}