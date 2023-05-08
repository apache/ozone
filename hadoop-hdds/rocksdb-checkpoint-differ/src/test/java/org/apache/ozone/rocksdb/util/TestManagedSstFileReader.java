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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;
import org.apache.hadoop.hdds.utils.db.managed.ManagedEnvOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSSTDumpTool;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileWriter;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * ManagedSstFileReader tests.
 */
public class TestManagedSstFileReader {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestManagedSstFileReader.class);

  private String createRandomSSTFile(Map<String, Integer> records)
      throws IOException, RocksDBException {
    Map<String, Integer> keys = records instanceof TreeMap ?
        records : new TreeMap<>(records);
    File file = File.createTempFile("tmp_sst_file", ".sst");
    file.deleteOnExit();
    try (ManagedSstFileWriter sstFileWriter = new ManagedSstFileWriter(
        new ManagedEnvOptions(), new ManagedOptions())) {
      sstFileWriter.open(file.getAbsolutePath());
      for (Map.Entry<String, Integer> entry : keys.entrySet()) {
        if (entry.getValue() == 0) {
          sstFileWriter.delete(entry.getKey().getBytes(StandardCharsets.UTF_8));
        } else {
          sstFileWriter.put(entry.getKey().getBytes(StandardCharsets.UTF_8),
              entry.getKey().getBytes(StandardCharsets.UTF_8));
        }
      }
      sstFileWriter.finish();
    }
    return file.getAbsolutePath();
  }

  private Map<String, Integer> createKeys(int startRange, int endRange) {
    return IntStream.range(startRange, endRange).boxed()
        .collect(Collectors.toMap(i -> "key" + i,
            i -> i % 2));
  }

  private Pair<Map<String, Integer>, List<String>> createDummyData(
      int numberOfFiles) throws RocksDBException, IOException {
    List<String> files = new ArrayList<>();
    int numberOfKeysPerFile = 1000;
    Map<String, Integer> keys = new HashMap<>();
    int cnt = 0;
    for (int i = 0; i < numberOfFiles; i++) {
      Map<String, Integer> fileKeys = createKeys(cnt,
          cnt + numberOfKeysPerFile);
      cnt += fileKeys.size();
      String tmpSSTFile = createRandomSSTFile(fileKeys);
      files.add(tmpSSTFile);
      keys.putAll(fileKeys);
    }
    return Pair.of(keys, files);
  }


  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 7, 10})
  public void testGetKeyStream(int numberOfFiles)
      throws RocksDBException, IOException, NativeLibraryNotLoadedException {
    Pair<Map<String, Integer>, List<String>> data =
        createDummyData(numberOfFiles);
    List<String> files = data.getRight();
    Map<String, Integer> keys = data.getLeft();
    new ManagedSstFileReader(files).getKeyStream().forEach(
        key -> {
          Assertions.assertEquals(keys.get(key), 1);
          keys.remove(key);
        });
    keys.values().forEach(val -> Assertions.assertEquals(0, val));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 7, 10})
  public void testGetKeyStreamWithTombstone(int numberOfFiles)
      throws RocksDBException, IOException, NativeLibraryNotLoadedException {
    Pair<Map<String, Integer>, List<String>> data =
        createDummyData(numberOfFiles);
    List<String> files = data.getRight();
    Map<String, Integer> keys = data.getLeft();
    ExecutorService executorService = new ThreadPoolExecutor(0,
        1, 60, TimeUnit.SECONDS,
        new SynchronousQueue<>(), new ThreadFactoryBuilder()
        .setNameFormat("snapshot-diff-manager-sst-dump-tool-TID-%d")
        .build(), new ThreadPoolExecutor.DiscardPolicy());
    ManagedSSTDumpTool sstDumpTool =
        new ManagedSSTDumpTool(executorService, 256);
    new ManagedSstFileReader(files).getKeyStreamWithTombstone(sstDumpTool)
        .forEach(keys::remove);
    Assertions.assertEquals(0, keys.size());
    executorService.shutdown();
    try {
      executorService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Failed to shutdown Report Manager", e);
      Thread.currentThread().interrupt();
    }
  }
}
