/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.utils.db;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
    assertThrows(IOException.class,
        () -> DBStoreBuilder.newBuilder(conf).build());
  }

  @Test
  public void builderWithOneParamV1() {
    OzoneConfiguration conf = new OzoneConfiguration();
    assertThrows(IOException.class,
        () -> DBStoreBuilder.newBuilder(conf).setName("Test.db").build());
  }

  @Test
  public void builderWithOneParamV2(@TempDir Path tempDir) {
    OzoneConfiguration conf = new OzoneConfiguration();
    assertThrows(IOException.class,
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
        .addTable("FIRST", new ManagedColumnFamilyOptions())
        .build();
    // Building should succeed without error.

    final Table<byte[], byte[]> firstTable = dbStore.getTable("FIRST");
    byte[] key = RandomStringUtils.secure().next(9).getBytes(StandardCharsets.UTF_8);
    byte[] value = RandomStringUtils.secure().next(9).getBytes(StandardCharsets.UTF_8);
    firstTable.put(key, value);
    byte[] temp = firstTable.get(key);
    assertArrayEquals(value, temp);

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
      final Table<byte[], byte[]> firstTable = dbStore.getTable("First");
      byte[] key = RandomStringUtils.secure().next(9).getBytes(StandardCharsets.UTF_8);
      byte[] value = RandomStringUtils.secure().next(9).getBytes(StandardCharsets.UTF_8);
      firstTable.put(key, value);
      byte[] temp = firstTable.get(key);
      assertArrayEquals(value, temp);


      final Table<byte[], byte[]> secondTable = dbStore.getTable("Second");
      assertTrue(secondTable.isEmpty());
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
      Table<byte[], byte[]> firstTable = dbStore.getTable("First");
      byte[] key = RandomStringUtils.secure().next(9).getBytes(StandardCharsets.UTF_8);
      byte[] value = RandomStringUtils.secure().next(9).getBytes(StandardCharsets.UTF_8);
      firstTable.put(key, value);
      byte[] temp = firstTable.get(key);
      assertArrayEquals(value, temp);

      Table<byte[], byte[]> secondTable = dbStore.getTable("Second");
      assertTrue(secondTable.isEmpty());
    }
  }

  @Test
  public void builderWithColumnFamilyOptions(@TempDir Path tempDir)
      throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempDir.toString());
    File newFolder = new File(tempDir.toString() + "/newFolder");

    if (!newFolder.exists()) {
      assertTrue(newFolder.mkdirs());
    }

    String sampleTableName = "sampleTable";
    final DBColumnFamilyDefinition<String, Long> sampleTable =
        new DBColumnFamilyDefinition<>(sampleTableName,
            StringCodec.get(), LongCodec.get());
    final DBDefinition sampleDB = new DBDefinition.WithMap(
        DBColumnFamilyDefinition.newUnmodifiableMap(sampleTable)) {
      {
        ManagedColumnFamilyOptions cfOptions = new ManagedColumnFamilyOptions();
        // reverse the default option for check
        cfOptions.setForceConsistencyChecks(
            !cfOptions.forceConsistencyChecks());
        sampleTable.setCfOptions(cfOptions);
      }

      @Override
      public String getName() {
        return "sampleDB";
      }

      @Override
      public String getLocationConfigKey() {
        return null;
      }

      @Override
      public File getDBLocation(ConfigurationSource conf) {
        return null;
      }
    };

    try (RDBStore rdbStore = DBStoreBuilder.newBuilder(conf, sampleDB, "SampleStore", newFolder.toPath()).build()) {
      Collection<RocksDatabase.ColumnFamily> cfFamilies =
          rdbStore.getColumnFamilies();

      // we also have the default column family, so there are 2
      assertEquals(2, cfFamilies.size());

      boolean checked = false;
      for (RocksDatabase.ColumnFamily cfFamily : cfFamilies) {
        if (Arrays.equals(cfFamily.getHandle().getName(),
            sampleTableName.getBytes(StandardCharsets.UTF_8))) {
          // get the default value
          boolean defaultValue = new ManagedColumnFamilyOptions()
              .forceConsistencyChecks();

          // the value should be different from the default value
          assertNotEquals(cfFamily.getHandle().getDescriptor()
              .getOptions().forceConsistencyChecks(), defaultValue);
          checked = true;
        }
      }
      assertTrue(checked);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testIfAutoCompactionDisabled(boolean disableAutoCompaction,
          @TempDir Path tempDir)
          throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempDir.toString());
    File newFolder = new File(tempDir.toString(), "newFolder");

    if (!newFolder.exists()) {
      assertTrue(newFolder.mkdirs());
    }

    String sampleTableName = "sampleTable";
    final DBColumnFamilyDefinition<String, Long> sampleTable =
        new DBColumnFamilyDefinition<>(sampleTableName,
            StringCodec.get(), LongCodec.get());
    final DBDefinition sampleDB = new DBDefinition.WithMap(
        DBColumnFamilyDefinition.newUnmodifiableMap(sampleTable)) {
      @Override
      public String getName() {
        return "sampleDB";
      }

      @Override
      public String getLocationConfigKey() {
        return null;
      }

      @Override
      public File getDBLocation(ConfigurationSource conf) {
        return null;
      }
    };

    try (RDBStore rdbStore = DBStoreBuilder.newBuilder(conf, sampleDB, "SampleStore", newFolder.toPath())
        .disableDefaultCFAutoCompaction(disableAutoCompaction)
        .build()) {
      Collection<RocksDatabase.ColumnFamily> cfFamilies =
              rdbStore.getColumnFamilies();

      // we also have the default column family, so there are 2
      assertEquals(2, cfFamilies.size());

      for (RocksDatabase.ColumnFamily cfFamily : cfFamilies) {
        // the value should be different from the default value
        assertEquals(cfFamily.getHandle().getDescriptor()
                .getOptions().disableAutoCompactions(),
                disableAutoCompaction);
      }
    }
  }
}
