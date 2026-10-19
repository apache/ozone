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

package org.apache.hadoop.hdds.utils.db.managed;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Tests for {@link ManagedRocksDB#openWithLatestOptions}.
 */
public class TestManagedRocksDB {
  static {
    ManagedRocksObjectUtils.loadRocksDBLibrary();
  }

  private static final byte[] KEY = "key".getBytes(UTF_8);
  private static final byte[] VALUE = "value".getBytes(UTF_8);

  @Test
  public void testOpenWithOptionsFromNewerRocksDBVersion(@TempDir Path dir) throws Exception {
    createDB(dir);
    // Simulate an OPTIONS file written by a newer RocksDB, e.g. after a downgrade.
    addUnknownOptionToLatestOptionsFile(dir, true);

    List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
    List<ColumnFamilyHandle> handles = new ArrayList<>();
    try (ManagedConfigOptions configOptions = new ManagedConfigOptions();
         ManagedDBOptions dbOptions = new ManagedDBOptions();
         ManagedRocksDB db = ManagedRocksDB.openWithLatestOptions(
             configOptions, dbOptions, dir.toString(), descriptors, handles)) {
      try {
        assertArrayEquals(VALUE, db.get().get(KEY));
      } finally {
        handles.forEach(ColumnFamilyHandle::close);
      }
    } finally {
      descriptors.forEach(d -> d.getOptions().close());
    }
  }

  @Test
  public void testOpenWithUnknownOptionFromSameRocksDBVersion(@TempDir Path dir) throws Exception {
    createDB(dir);
    // An unknown option in an OPTIONS file from the same RocksDB version still means corruption.
    addUnknownOptionToLatestOptionsFile(dir, false);

    List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
    try (ManagedConfigOptions configOptions = new ManagedConfigOptions();
         ManagedDBOptions dbOptions = new ManagedDBOptions()) {
      assertThrows(RocksDBException.class, () -> ManagedRocksDB.openWithLatestOptions(
          configOptions, dbOptions, dir.toString(), descriptors, new ArrayList<>()));
    } finally {
      descriptors.forEach(d -> d.getOptions().close());
    }
  }

  private static void createDB(Path dir) throws RocksDBException {
    List<ColumnFamilyHandle> handles = new ArrayList<>();
    try (ManagedColumnFamilyOptions cfOptions = new ManagedColumnFamilyOptions();
         ManagedDBOptions dbOptions = new ManagedDBOptions()) {
      dbOptions.setCreateIfMissing(true);
      try (ManagedRocksDB db = ManagedRocksDB.open(dbOptions, dir.toString(),
          Collections.singletonList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions)), handles)) {
        try {
          db.get().put(KEY, VALUE);
        } finally {
          handles.forEach(ColumnFamilyHandle::close);
        }
      }
    }
  }

  /**
   * Adds an unknown option to the default column family in the latest OPTIONS file.
   * If {@code newerVersion}, also marks the file as written by RocksDB 99.0.0.
   */
  private static void addUnknownOptionToLatestOptionsFile(Path dir, boolean newerVersion) throws IOException {
    Path optionsFile;
    try (Stream<Path> files = Files.list(dir)) {
      optionsFile = files.filter(p -> p.getFileName().toString().matches("OPTIONS-[0-9]+"))
          .max(Path::compareTo)
          .orElseThrow(() -> new IOException("No OPTIONS file in " + dir));
    }
    List<String> lines = new ArrayList<>();
    for (String line : Files.readAllLines(optionsFile, UTF_8)) {
      if (newerVersion && line.trim().startsWith("rocksdb_version=")) {
        line = "  rocksdb_version=99.0.0";
      }
      lines.add(line);
      if (line.startsWith("[CFOptions \"default\"]")) {
        lines.add("  option_from_a_newer_rocksdb=1");
      }
    }
    Files.write(optionsFile, lines, UTF_8);
  }
}
