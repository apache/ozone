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

import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_PROPERTY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;
import org.apache.hadoop.hdds.utils.db.LatestVersionedKWayMergeIterator.MergedKeyValue;
import org.apache.hadoop.hdds.utils.db.TestRawSstFileRecords.SourceRecord;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * End-to-end test over real SST files produced by RocksDB flush.
 * <p>
 * {@link org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileWriter} is not used here because
 * it stores sequence number 0 on every key and rejects duplicate user keys per file. Flushing a
 * real RocksDB after each logical source batch yields SST files with global sequence numbers.
 * Memtable flushes retain only the latest value per user key within each SST; competing versions
 * appear across separate flushed files, matching production snapshot-diff inputs.
 */
@EnabledIfSystemProperty(named = ROCKS_TOOLS_NATIVE_PROPERTY, matches = "true")
class TestLatestVersionedKWayMergeIteratorOverSst {

  @TempDir
  private Path tempDir;

  @BeforeAll
  static void loadNativeLibrary() throws NativeLibraryNotLoadedException {
    ManagedRawSSTFileReader.loadLibrary();
  }

  @Test
  void testWorkedExampleOverRealSstFiles() throws Exception {
    // Mirrors the unit-test "three-file worked example": cross-file k-way merge with competing
    // versions, k1 latest-value-only, k2 delete-then-recreate across files.
    Path dbDir = tempDir.resolve("worked-example-db");
    Files.createDirectories(dbDir);
    Set<String> knownSstFiles = new HashSet<>();
    List<Path> sstFiles = new ArrayList<>(3);

    try (ManagedDBOptions dbOptions = new ManagedDBOptions();
         ManagedColumnFamilyOptions cfOptions = new ManagedColumnFamilyOptions();
         FlushOptions flushOptions = new FlushOptions()) {
      dbOptions.setCreateIfMissing(true);
      List<ColumnFamilyDescriptor> columnFamilyDescriptors = Collections.singletonList(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
      List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      try (ManagedRocksDB db = ManagedRocksDB.open(
          dbOptions, dbDir.toString(), columnFamilyDescriptors, columnFamilyHandles);
           ColumnFamilyHandle cf = columnFamilyHandles.get(0)) {

        // Source A: latest k1 wins within the memtable before flush, plus k2.
        rocksPut(db, cf, "k1", "v5");
        rocksPut(db, cf, "k1", "v10");
        rocksPut(db, cf, "k2", "v20");
        sstFiles.add(flushAndCopySst(db, dbDir, cf, flushOptions, knownSstFiles, "a"));

        // Source B: competing k1 version and a k2 tombstone.
        rocksPut(db, cf, "k1", "v3");
        rocksPut(db, cf, "k1", "v15");
        rocksDelete(db, cf, "k2");
        sstFiles.add(flushAndCopySst(db, dbDir, cf, flushOptions, knownSstFiles, "b"));

        // Source C: winning k1/k2 values.
        rocksPut(db, cf, "k1", "v1");
        rocksPut(db, cf, "k1", "v30");
        rocksPut(db, cf, "k2", "v25");
        sstFiles.add(flushAndCopySst(db, dbDir, cf, flushOptions, knownSstFiles, "c"));
      }
    }

    List<List<SourceRecord>> perSource = TestRawSstFileRecords.readFiles(sstFiles);
    long k1VersionsAcrossFiles = perSource.stream()
        .flatMap(List::stream)
        .filter(record -> Arrays.equals(record.getUserKey(), keyBytes("k1")))
        .count();
    assertEquals(3, k1VersionsAcrossFiles,
        "each flushed SST should contribute one surviving k1 version");
    long distinctK1Sequences = perSource.stream()
        .flatMap(List::stream)
        .filter(record -> Arrays.equals(record.getUserKey(), keyBytes("k1")))
        .mapToLong(SourceRecord::getSequence)
        .distinct()
        .count();
    assertEquals(3, distinctK1Sequences,
        "k1 versions across SST files should carry distinct RocksDB sequence numbers");

    List<MergedKeyValue> actual = mergeSstFiles(sstFiles.toArray(new Path[0]));
    assertEquals(3, actual.size(), "expected k1 winner plus k2 tombstone and recreate value");

    MergedKeyValue k1Winner = actual.get(0);
    assertArrayEquals(keyBytes("k1"), k1Winner.getUserKey());
    assertEquals(LatestVersionedKWayMergeIterator.ROCKS_TYPE_VALUE, k1Winner.getValueType());
    assertArrayEquals(valueBytes("v30"), k1Winner.getValue());

    MergedKeyValue k2Tombstone = actual.get(1);
    assertArrayEquals(keyBytes("k2"), k2Tombstone.getUserKey());
    assertNotEquals(LatestVersionedKWayMergeIterator.ROCKS_TYPE_VALUE, k2Tombstone.getValueType());

    MergedKeyValue k2Value = actual.get(2);
    assertArrayEquals(keyBytes("k2"), k2Value.getUserKey());
    assertEquals(LatestVersionedKWayMergeIterator.ROCKS_TYPE_VALUE, k2Value.getValueType());
    assertArrayEquals(valueBytes("v25"), k2Value.getValue());
    assertTrue(k2Value.getSequence() > k2Tombstone.getSequence(),
        "recreate value must be newer than the tombstone");
  }

  private Path flushAndCopySst(ManagedRocksDB db, Path dbDir, ColumnFamilyHandle cf,
      FlushOptions flushOptions, Set<String> knownSstFiles, String label)
      throws RocksDBException, IOException {
    db.get().flush(flushOptions, cf);
    Path newSst = findNewSstFile(dbDir, knownSstFiles);
    Path dest = tempDir.resolve(label + "-" + newSst.getFileName());
    Files.copy(newSst, dest);
    return dest;
  }

  private static Path findNewSstFile(Path dbDir, Set<String> knownSstFiles) throws IOException {
    try (Stream<Path> sstPaths = Files.list(dbDir)) {
      List<Path> newFiles = sstPaths
          .filter(path -> path.getFileName().toString().endsWith(".sst"))
          .filter(path -> knownSstFiles.add(path.getFileName().toString()))
          .sorted()
          .collect(Collectors.toList());
      if (newFiles.size() != 1) {
        throw new IllegalStateException(
            "Expected exactly one new SST file under " + dbDir + ", found " + newFiles);
      }
      return newFiles.get(0);
    }
  }

  private List<MergedKeyValue> mergeSstFiles(Path... sstFiles) throws Exception {
    List<MergedKeyValue> results = new ArrayList<>();
    try (LatestVersionedKWayMergeIterator iterator =
             LatestVersionedKWayMergeIterator.overRawSstFiles(Arrays.asList(sstFiles))) {
      while (iterator.hasNext()) {
        results.add(iterator.next());
      }
    }
    return results;
  }

  private static void rocksPut(ManagedRocksDB db, ColumnFamilyHandle cf, String key, String value)
      throws RocksDBException {
    db.get().put(cf, keyBytes(key), valueBytes(value));
  }

  private static void rocksDelete(ManagedRocksDB db, ColumnFamilyHandle cf, String key)
      throws RocksDBException {
    db.get().delete(cf, keyBytes(key));
  }

  private static byte[] keyBytes(String key) {
    return key.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] valueBytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

}
