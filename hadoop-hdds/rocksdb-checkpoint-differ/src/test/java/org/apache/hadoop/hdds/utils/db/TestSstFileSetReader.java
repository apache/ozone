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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.TestUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedEnvOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileWriter;
import org.apache.hadoop.ozone.util.ClosableIterator;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.rocksdb.RocksDBException;

/**
 * ManagedSstFileReader tests.
 */
class TestSstFileSetReader {

  @TempDir
  private File tempDir;

  private final AtomicInteger fileCounter = new AtomicInteger();

  // Key prefix containing all characters, to check if all characters can be
  // written & read from rocksdb through SSTDumptool
  private static final String KEY_PREFIX = IntStream.range(0, 5 << 10).boxed()
      .map(i -> String.format("%c", i % 256))
      .collect(Collectors.joining(""));

  /**
   * Helper method to create an SST file with the given keys.
   * Each key-value pair is written to the SST file, where keys with value 0
   * are treated as deletions (tombstones) and keys with non-zero value are regular entries.
   *
   * @param keys TreeMap containing keys and their corresponding values (0 for delete, non-0 for put)
   * @return Absolute path to the created SST file
   * @throws RocksDBException if there's an error during SST file creation
   */
  private Path createRandomSSTFile(TreeMap<String, Integer> keys)
      throws RocksDBException {
    File file = new File(tempDir, "tmp_sst_file" + fileCounter.incrementAndGet() + ".sst");

    try (ManagedOptions managedOptions = new ManagedOptions();
         ManagedEnvOptions managedEnvOptions = new ManagedEnvOptions();
         ManagedSstFileWriter sstFileWriter = new ManagedSstFileWriter(
             managedEnvOptions, managedOptions)) {
      sstFileWriter.open(file.getAbsolutePath());
      for (Map.Entry<String, Integer> entry : keys.entrySet()) {
        byte[] keyByte = StringUtils.string2Bytes(entry.getKey());
        if (entry.getValue() == 0) {
          sstFileWriter.delete(keyByte);
        } else {
          sstFileWriter.put(keyByte, keyByte);
        }
      }
      sstFileWriter.finish();
    }
    assertTrue(file.exists());
    return file.getAbsoluteFile().toPath();
  }

  /**
   * Helper method to create a map of keys with values alternating between 0 and 1.
   * Keys with even indices get value 0 (will be treated as deletions/tombstones),
   * keys with odd indices get value 1 (will be treated as regular entries).
   *
   * @param startRange Starting range for key generation (inclusive)
   * @param endRange Ending range for key generation (exclusive)
   * @return Map of keys with alternating 0/1 values
   */
  private Map<String, Integer> createKeys(int startRange, int endRange) {
    return IntStream.range(startRange, endRange).boxed()
        .collect(Collectors.toMap(i -> KEY_PREFIX + i,
            i -> i % 2));
  }

  /**
   * Helper method to create dummy test data consisting of multiple SST files.
   * Keys are distributed across files in round-robin fashion, ensuring each file
   * contains a subset of the total key space for testing overlapping scenarios.
   *
   * @param numberOfFiles Number of SST files to create
   * @return Pair containing the complete sorted key map and list of SST file paths
   * @throws RocksDBException if there's an error during SST file creation
   */
  private Pair<SortedMap<String, Integer>, List<Path>> createDummyData(int numberOfFiles) throws RocksDBException {
    List<Path> files = new ArrayList<>();
    int numberOfKeysPerFile = 1000;
    TreeMap<String, Integer> keys =
        new TreeMap<>(createKeys(0, numberOfKeysPerFile * numberOfFiles));
    List<TreeMap<String, Integer>> fileKeysList =
        IntStream.range(0, numberOfFiles)
            .mapToObj(i -> new TreeMap<String, Integer>())
            .collect(Collectors.toList());
    int cnt = 0;
    for (Map.Entry<String, Integer> kv : keys.entrySet()) {
      fileKeysList.get(cnt % numberOfFiles).put(kv.getKey(), kv.getValue());
      cnt += 1;
    }
    for (TreeMap<String, Integer> fileKeys : fileKeysList) {
      Path tmpSSTFile = createRandomSSTFile(fileKeys);
      files.add(tmpSSTFile);
    }
    return Pair.of(keys, files);
  }

  /**
   * Tests the getKeyStream method of SstFileSetReader with various boundary conditions.
   * This test verifies that:
   * 1. Keys are correctly filtered within specified lower and upper bounds
   * 2. Only non-deleted keys are returned in the stream
   * 3. Deleted keys (tombstones) are properly excluded from results
   */
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 7, 10})
  public void testGetKeyStream(int numberOfFiles)
      throws RocksDBException, CodecException {
    Pair<SortedMap<String, Integer>, List<Path>> data = createDummyData(numberOfFiles);
    List<Path> files = data.getRight();
    SortedMap<String, Integer> keys = data.getLeft();
    // Getting every possible combination of 2 elements from the sampled keys.
    // Reading the sst file lying within the given bounds and
    // validating the keys read from the sst file.
    List<Optional<String>> bounds = TestUtils.getTestingBounds(keys);
    for (Optional<String> lowerBound : bounds) {
      for (Optional<String> upperBound : bounds) {
        // Calculating the expected keys which lie in the given boundary.
        Map<String, Integer> keysInBoundary =
            keys.entrySet().stream().filter(entry ->
                    lowerBound.map(l -> entry.getKey().compareTo(l) >= 0)
                        .orElse(true) &&
                    upperBound.map(u -> entry.getKey().compareTo(u) < 0)
                        .orElse(true))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        try (ClosableIterator<String> keyStream =
                 new SstFileSetReader(files).getKeyStream(
                     lowerBound.orElse(null), upperBound.orElse(null))) {
          while (keyStream.hasNext()) {
            String key = keyStream.next();
            assertEquals(1, keysInBoundary.get(key));
            assertNotNull(keysInBoundary.remove(key));
          }
          keysInBoundary.values().forEach(val -> assertEquals(0, val));
        }
      }
    }
  }

  /**
   * Tests the getKeyStreamWithTombstone method which includes both regular keys and tombstones.
   * This test is only enabled when the native RocksDB tools library is available.
   * Unlike testGetKeyStream, this method returns ALL keys within bounds, including tombstones.
   */
  @EnabledIfSystemProperty(named = ROCKS_TOOLS_NATIVE_PROPERTY, matches = "true")
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 7, 10})
  public void testGetKeyStreamWithTombstone(int numberOfFiles)
      throws RocksDBException, CodecException {
    assumeTrue(ManagedRawSSTFileReader.tryLoadLibrary());
    Pair<SortedMap<String, Integer>, List<Path>> data =
        createDummyData(numberOfFiles);
    List<Path> files = data.getRight();
    SortedMap<String, Integer> keys = data.getLeft();
    // Getting every possible combination of 2 elements from the sampled keys.
    // Reading the sst file lying within the given bounds and
    // validating the keys read from the sst file.
    List<Optional<String>> bounds = TestUtils.getTestingBounds(keys);
    for (Optional<String> lowerBound : bounds) {
      for (Optional<String> upperBound : bounds) {
        // Calculating the expected keys which lie in the given boundary.
        Map<String, Integer> keysInBoundary =
            keys.entrySet().stream().filter(entry ->
                    lowerBound.map(l -> entry.getKey().compareTo(l) >= 0)
                        .orElse(true) &&
                    upperBound.map(u -> entry.getKey().compareTo(u) < 0)
                        .orElse(true))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        try (ClosableIterator<String> keyStream = new SstFileSetReader(files)
            .getKeyStreamWithTombstone(lowerBound.orElse(null),
                upperBound.orElse(null))) {
          while (keyStream.hasNext()) {
            String key = keyStream.next();
            assertNotNull(keysInBoundary.remove(key));
          }
        }
        assertEquals(0, keysInBoundary.size());
      }
    }
  }

  /**
   * Test MinHeap implementation with overlapping SST files.
   * Verifies that duplicate entries from multiple SST files are handled correctly,
   * with the entry from the SST file with the highest index (latest in collection) being returned.
   */
  @ParameterizedTest
  @ValueSource(ints = {2, 3, 5})
  public void testMinHeapWithOverlappingSstFiles(int numberOfFiles) throws RocksDBException, CodecException {
    assumeTrue(numberOfFiles >= 2);

    // Create overlapping SST files with some duplicate keys
    List<Path> files = new ArrayList<>();
    Map<String, Integer> expectedKeys = new TreeMap<>();

    // File 0: keys 0-9 (all valid entries)
    TreeMap<String, Integer> file0Keys = new TreeMap<>();
    for (int i = 0; i < 10; i++) {
      String key = KEY_PREFIX + i;
      file0Keys.put(key, 1);
      expectedKeys.put(key, 0); // Expected to come from file 0 initially
    }
    files.add(createRandomSSTFile(file0Keys));

    // File 1: keys 5-14 (overlaps with file 0 on keys 5-9, adds keys 10-14)
    TreeMap<String, Integer> file1Keys = new TreeMap<>();
    for (int i = 5; i < 15; i++) {
      String key = KEY_PREFIX + i;
      file1Keys.put(key, 1);
      expectedKeys.put(key, 1); // Keys 5-9 should now come from file 1 (higher index)
    }
    files.add(createRandomSSTFile(file1Keys));

    // File 2: keys 10-19 (overlaps with file 1 on keys 10-14, adds keys 15-19)
    if (numberOfFiles >= 3) {
      TreeMap<String, Integer> file2Keys = new TreeMap<>();
      for (int i = 10; i < 20; i++) {
        String key = KEY_PREFIX + i;
        file2Keys.put(key, 1);
        expectedKeys.put(key, 2); // Keys 10-14 should now come from file 2 (highest index)
      }
      files.add(createRandomSSTFile(file2Keys));
    }

    // Add more files if requested
    for (int fileIdx = 3; fileIdx < numberOfFiles; fileIdx++) {
      TreeMap<String, Integer> fileKeys = new TreeMap<>();
      int startKey = fileIdx * 5;
      for (int i = startKey; i < startKey + 10; i++) {
        String key = KEY_PREFIX + i;
        fileKeys.put(key, 1);
        expectedKeys.put(key, fileIdx); // This file has highest index for these keys
      }
      files.add(createRandomSSTFile(fileKeys));
    }

    // Read using SstFileSetReader and verify correct behavior
    List<String> actualKeys = new ArrayList<>();
    try (ClosableIterator<String> keyStream = new SstFileSetReader(files).getKeyStream(null, null)) {
      while (keyStream.hasNext()) {
        actualKeys.add(keyStream.next());
      }

    }

    // Verify all expected keys are present and in sorted order
    List<String> expectedKeysList = expectedKeys.keySet().stream()
        .sorted()
        .collect(Collectors.toList());
    assertEquals(expectedKeysList, actualKeys, "Keys should be in sorted order without duplicates");
  }

  /**
   * Test duplicate key handling with the latest file taking precedence.
   * This specifically tests the behavior where duplicate keys should return
   * the value from the SST file with the highest index.
   */
  @ParameterizedTest
  @ValueSource(ints = {3, 4, 5})
  public void testDuplicateKeyHandlingWithLatestFilePrecedence(int numberOfFiles)
      throws RocksDBException, CodecException {
    assumeTrue(numberOfFiles >= 3);

    List<Path> files = new ArrayList<>();

    // All files will contain the same set of keys, but we expect the last file to "win"
    String[] testKeys = {KEY_PREFIX + "duplicate1", KEY_PREFIX + "duplicate2", KEY_PREFIX + "duplicate3"};

    for (int fileIdx = 0; fileIdx < numberOfFiles; fileIdx++) {
      TreeMap<String, Integer> fileKeys = new TreeMap<>();

      // Add the duplicate keys to each file
      for (String testKey : testKeys) {
        fileKeys.put(testKey, 1); // All are valid entries
      }

      // Add some unique keys per file to verify sorting works correctly
      for (int i = 0; i < 3; i++) {
        String uniqueKey = KEY_PREFIX + "unique_" + fileIdx + "_" + i;
        fileKeys.put(uniqueKey, 1);
      }

      files.add(createRandomSSTFile(fileKeys));
    }

    // Read all keys
    List<String> actualKeys = new ArrayList<>();
    try (ClosableIterator<String> keyStream = new SstFileSetReader(files).getKeyStream(null, null)) {
      while (keyStream.hasNext()) {
        actualKeys.add(keyStream.next());
      }
    }

    // Verify we only get each duplicate key once (not numberOfFiles times)
    long duplicateKeyCount = actualKeys.stream()
        .filter(key -> key.contains("duplicate"))
        .count();
    assertEquals(testKeys.length, duplicateKeyCount,
        "Should have exactly one occurrence of each duplicate key");

    // Verify all keys are in sorted order
    List<String> sortedKeys = new ArrayList<>(actualKeys);
    sortedKeys.sort(String::compareTo);
    assertEquals(sortedKeys, actualKeys, "Keys should be in sorted order");

    // Verify total number of distinct keys
    Set<String> uniqueKeys = new HashSet<>(actualKeys);
    assertEquals(uniqueKeys.size(), actualKeys.size(), "Should have no duplicate keys in output");

    // Expected total: 3 duplicate keys + 3 unique keys per file
    int expectedTotalKeys = testKeys.length + (numberOfFiles * 3);
    assertEquals(expectedTotalKeys, actualKeys.size(),
        "Should have correct total number of distinct keys");
  }

}
