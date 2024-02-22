/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ozone.rocksdb.util;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;
import org.apache.hadoop.hdds.utils.TestUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedEnvOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRawSSTFileReader;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileWriter;
import org.apache.ozone.test.tag.Native;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_LIBRARY_NAME;

/**
 * ManagedSstFileReader tests.
 */
class TestSstFileSetReader {

  // Key prefix containing all characters, to check if all characters can be
  // written & read from rocksdb through SSTDumptool
  private static final String KEY_PREFIX = IntStream.range(0, 256).boxed()
      .map(i -> String.format("%c", i))
      .collect(Collectors.joining(""));

  private String createRandomSSTFile(TreeMap<String, Integer> keys)
      throws IOException, RocksDBException {
    File file = File.createTempFile("tmp_sst_file", ".sst");
    file.deleteOnExit();

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
    return file.getAbsolutePath();
  }

  private Map<String, Integer> createKeys(int startRange, int endRange) {
    return IntStream.range(startRange, endRange).boxed()
        .collect(Collectors.toMap(i -> KEY_PREFIX + i,
            i -> i % 2));
  }

  private Pair<SortedMap<String, Integer>, List<String>> createDummyData(
      int numberOfFiles) throws RocksDBException, IOException {
    List<String> files = new ArrayList<>();
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
      String tmpSSTFile = createRandomSSTFile(fileKeys);
      files.add(tmpSSTFile);
    }
    return Pair.of(keys, files);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 7, 10})
  public void testGetKeyStream(int numberOfFiles)
      throws RocksDBException, IOException {
    Pair<SortedMap<String, Integer>, List<String>> data =
        createDummyData(numberOfFiles);
    List<String> files = data.getRight();
    SortedMap<String, Integer> keys = data.getLeft();
    // Getting every possible combination of 2 elements from the sampled keys.
    // Reading the sst file lying within the given bounds and
    // validating the keys read from the sst file.
    List<Optional<String>> bounds = TestUtils.getTestingBounds(keys);
    for (Optional<String> lowerBound : bounds) {
      for (Optional<String> upperBound : bounds) {
        // Calculating the expected keys which lie in the given boundary.
        Map<String, Integer> keysInBoundary =
            keys.entrySet().stream().filter(entry -> lowerBound
                    .map(l -> entry.getKey().compareTo(l) >= 0)
                    .orElse(true)  &&
                    upperBound.map(u -> entry.getKey().compareTo(u) < 0)
                        .orElse(true))
                .collect(Collectors.toMap(Map.Entry::getKey,
                    Map.Entry::getValue));
        try (Stream<String> keyStream =
                 new SstFileSetReader(files).getKeyStream(
                     lowerBound.orElse(null), upperBound.orElse(null))) {
          keyStream.forEach(key -> {
            Assertions.assertEquals(keysInBoundary.get(key), 1);
            Assertions.assertNotNull(keysInBoundary.remove(key));
          });
          keysInBoundary.values()
              .forEach(val -> Assertions.assertEquals(0, val));
        }
      }
    }
  }

  @Native(ROCKS_TOOLS_NATIVE_LIBRARY_NAME)
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 7, 10})
  public void testGetKeyStreamWithTombstone(int numberOfFiles)
      throws RocksDBException, IOException, NativeLibraryNotLoadedException {
    Assumptions.assumeTrue(ManagedRawSSTFileReader.loadLibrary());
    Pair<SortedMap<String, Integer>, List<String>> data =
        createDummyData(numberOfFiles);
    List<String> files = data.getRight();
    SortedMap<String, Integer> keys = data.getLeft();
    // Getting every possible combination of 2 elements from the sampled keys.
    // Reading the sst file lying within the given bounds and
    // validating the keys read from the sst file.
    List<Optional<String>> bounds = TestUtils.getTestingBounds(keys);
    for (Optional<String> lowerBound : bounds) {
      for (Optional<String> upperBound : bounds) {
        // Calculating the expected keys which lie in the given boundary.
        Map<String, Integer> keysInBoundary =
            keys.entrySet().stream().filter(entry -> lowerBound
                    .map(l -> entry.getKey().compareTo(l) >= 0)
                    .orElse(true)  &&
                    upperBound.map(u -> entry.getKey().compareTo(u) < 0)
                        .orElse(true))
                .collect(Collectors.toMap(Map.Entry::getKey,
                    Map.Entry::getValue));
        try (Stream<String> keyStream = new SstFileSetReader(files)
            .getKeyStreamWithTombstone(lowerBound.orElse(null),
                upperBound.orElse(null))) {
          keyStream.forEach(
              key -> {
                Assertions.assertNotNull(keysInBoundary.remove(key));
              });
        }
        Assertions.assertEquals(0, keysInBoundary.size());
      }
    }
  }
}
