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

import static org.apache.hadoop.hdds.utils.db.DBConfigFromFile.getOptionsFileNameFromDB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;

/**
 * DBConf tests.
 */
public class TestDBConfigFromFile {
  private static final String DB_FILE = "test.db";
  private static final String INI_FILE = getOptionsFileNameFromDB(DB_FILE);

  @BeforeEach
  public void setUp(@TempDir Path tempDir) throws Exception {
    System.setProperty(DBConfigFromFile.CONFIG_DIR, tempDir.toString());
    ClassLoader classLoader = getClass().getClassLoader();
    File testData = new File(classLoader.getResource(INI_FILE).getFile());
    File dest = Paths.get(
        System.getProperty(DBConfigFromFile.CONFIG_DIR), INI_FILE).toFile();
    FileUtils.copyFile(testData, dest);
  }

  @AfterEach
  public void tearDown() throws Exception {
  }

  @Test
  public void readFromFile() throws IOException {
    final List<String> families =
        Arrays.asList(StringUtils.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY),
            "First", "Second", "Third",
            "Fourth", "Fifth",
            "Sixth");
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
        new ArrayList<>();
    for (String family : families) {
      columnFamilyDescriptors.add(
          new ColumnFamilyDescriptor(family.getBytes(StandardCharsets.UTF_8),
              new ColumnFamilyOptions()));
    }

    final DBOptions options = DBConfigFromFile.readFromFile(DB_FILE,
        columnFamilyDescriptors);

    // Some Random Values Defined in the test.db.ini, we verify that we are
    // able to get values that are defined in the test.db.ini.
    assertNotNull(options);
    assertEquals(551615L, options.maxManifestFileSize());
    assertEquals(1000L, options.keepLogFileNum());
    assertEquals(1048576, options.writableFileMaxBufferSize());
  }

  @Test
  public void readFromFileInvalidConfig() throws IOException {
    final List<String> families =
        Arrays.asList(StringUtils.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY),
            "First", "Second", "Third",
            "Fourth", "Fifth",
            "Sixth");
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
        new ArrayList<>();
    for (String family : families) {
      columnFamilyDescriptors.add(
          new ColumnFamilyDescriptor(family.getBytes(StandardCharsets.UTF_8),
              new ColumnFamilyOptions()));
    }

    final DBOptions options = DBConfigFromFile.readFromFile("badfile.db.ini",
        columnFamilyDescriptors);

    // This has to return a Null, since we have config defined for badfile.db
    assertNull(options);
  }
}
