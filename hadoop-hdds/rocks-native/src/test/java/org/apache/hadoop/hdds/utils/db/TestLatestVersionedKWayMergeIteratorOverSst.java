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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;
import org.apache.hadoop.hdds.utils.db.ExpectedLatestVersionMergeOutput.SourceRecord;
import org.apache.hadoop.hdds.utils.db.LatestVersionedKWayMergeIterator.MergedKeyValue;
import org.apache.hadoop.hdds.utils.db.managed.ManagedEnvOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileWriter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end test over real SST files written with {@link ManagedSstFileWriter}.
 * Expected output is computed independently by reading each SST file and
 * applying {@link ExpectedLatestVersionMergeOutput}.
 */
@EnabledIfSystemProperty(named = ROCKS_TOOLS_NATIVE_PROPERTY, matches = "true")
class TestLatestVersionedKWayMergeIteratorOverSst {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestLatestVersionedKWayMergeIteratorOverSst.class);

  @TempDir
  private Path tempDir;

  @BeforeAll
  static void loadNativeLibrary() throws NativeLibraryNotLoadedException {
    ManagedRawSSTFileReader.loadLibrary();
  }

  @Test
  void testOverRealSstFilesMatchesExpectedOutput() throws Exception {
    // Mirrors the worked example shape, but RocksDB assigns real sequence numbers.
    Path sstA = writeSst("a.sst",
        op("k1", "v10"),
        op("k1", "v5"),
        op("k2", "v20"));
    Path sstB = writeSst("b.sst",
        op("k1", "v3"),
        op("k1", "v15"),
        del("k2"));
    Path sstC = writeSst("c.sst",
        op("k1", "v1"),
        op("k1", "v30"),
        op("k2", "v25"));

    List<List<SourceRecord>> perSource =
        TestRawSstFileRecords.readFiles(Arrays.asList(sstA, sstB, sstC));
    List<SourceRecord> expected = ExpectedLatestVersionMergeOutput.fromSourceRecords(perSource);
    List<MergedKeyValue> actual = mergeSstFiles(sstA, sstB, sstC);

    logRawInputs(perSource);
    logComparison(expected, actual);
    assertResultsEqual(expected, actual);
  }

  @Test
  void testDeleteRecreateOverRealSstFiles() throws Exception {
    Path sstA = writeSst("delete.sst",
        op("k1", "v1"),
        del("k1"));
    Path sstB = writeSst("recreate.sst",
        op("k1", "v-new"));

    List<List<SourceRecord>> perSource =
        TestRawSstFileRecords.readFiles(Arrays.asList(sstA, sstB));
    List<SourceRecord> expected = ExpectedLatestVersionMergeOutput.fromSourceRecords(perSource);
    List<MergedKeyValue> actual = mergeSstFiles(sstA, sstB);

    logRawInputs(perSource);
    logComparison(expected, actual);
    assertResultsEqual(expected, actual);
    assertEquals(2, actual.size(), "delete-recreate should emit tombstone then value");
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

  private Path writeSst(String fileName, SstOp... ops) throws Exception {
    File file = Files.createFile(tempDir.resolve(fileName)).toFile();
    try (ManagedEnvOptions envOptions = new ManagedEnvOptions();
         ManagedOptions options = new ManagedOptions();
         ManagedSstFileWriter writer = new ManagedSstFileWriter(envOptions, options)) {
      writer.open(file.getAbsolutePath());
      for (SstOp op : ops) {
        byte[] key = op.key.getBytes(StandardCharsets.UTF_8);
        if (op.delete) {
          writer.delete(key);
        } else {
          writer.put(key, op.value.getBytes(StandardCharsets.UTF_8));
        }
      }
      writer.finish();
    }
    return file.toPath();
  }

  private static SstOp op(String key, String value) {
    return new SstOp(key, value, false);
  }

  private static SstOp del(String key) {
    return new SstOp(key, null, true);
  }

  private static void assertResultsEqual(List<SourceRecord> expected, List<MergedKeyValue> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      SourceRecord exp = expected.get(i);
      MergedKeyValue act = actual.get(i);
      assertArrayEquals(exp.getUserKey(), act.getUserKey());
      assertEquals(exp.getSequence(), act.getSequence());
      assertEquals(exp.getType(), act.getValueType());
      assertArrayEquals(exp.getValue(), act.getValue());
    }
  }

  private static void logRawInputs(List<List<SourceRecord>> perSource) {
    for (int i = 0; i < perSource.size(); i++) {
      for (SourceRecord record : perSource.get(i)) {
        LOG.info("raw source={} key={} seq={} type={}",
            i, new String(record.getUserKey(), StandardCharsets.UTF_8),
            record.getSequence(), record.getType());
      }
    }
  }

  private static void logComparison(List<SourceRecord> expected, List<MergedKeyValue> actual) {
    for (int i = 0; i < expected.size(); i++) {
      LOG.info("compare[{}] expected seq={} type={} actual seq={} type={}",
          i, expected.get(i).getSequence(), expected.get(i).getType(),
          actual.get(i).getSequence(), actual.get(i).getValueType());
    }
  }

  private static final class SstOp {
    private final String key;
    private final String value;
    private final boolean delete;

    private SstOp(String key, String value, boolean delete) {
      this.key = key;
      this.value = value;
      this.delete = delete;
    }
  }
}
