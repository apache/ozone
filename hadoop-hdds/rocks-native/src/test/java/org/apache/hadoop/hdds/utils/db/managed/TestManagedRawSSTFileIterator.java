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

package org.apache.hadoop.hdds.utils.db.managed;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;
import org.apache.hadoop.hdds.utils.TestUtils;
import org.apache.ozone.test.tag.Native;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_LIBRARY_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for ManagedRawSSTFileReaderIterator.
 */
@Native(ROCKS_TOOLS_NATIVE_LIBRARY_NAME)
class TestManagedRawSSTFileIterator {

  @TempDir
  private Path tempDir;

  private File createSSTFileWithKeys(
      TreeMap<Pair<String, Integer>, String> keys) throws Exception {
    File file = Files.createFile(tempDir.resolve("tmp_sst_file.sst")).toFile();
    try (ManagedEnvOptions envOptions = new ManagedEnvOptions();
         ManagedOptions managedOptions = new ManagedOptions();
         ManagedSstFileWriter sstFileWriter = new ManagedSstFileWriter(envOptions, managedOptions)) {
      sstFileWriter.open(file.getAbsolutePath());
      for (Map.Entry<Pair<String, Integer>, String> entry : keys.entrySet()) {
        if (entry.getKey().getValue() == 0) {
          sstFileWriter.delete(entry.getKey().getKey().getBytes(StandardCharsets.UTF_8));
        } else {
          sstFileWriter.put(entry.getKey().getKey().getBytes(StandardCharsets.UTF_8),
              entry.getValue().getBytes(StandardCharsets.UTF_8));
        }
      }
      sstFileWriter.finish();
    }
    return file;
  }

  private static Stream<? extends Arguments> keyValueFormatArgs() {
    return Stream.of(Arguments.of(Named.of("Key starting with a single quote", "'key%1$d=>"),
            Named.of("Value starting with a number ending with a single quote", "%1$dvalue'")),
        Arguments.of(Named.of("Key ending with a number", "key%1$d"),
            Named.of("Value starting & ending with a number", "%1$dvalue%1$d")),
        Arguments.of(Named.of("Key starting with a single quote & ending with a number", "'key%1$d"),
            Named.of("Value starting & ending with a number & elosed within quotes", "%1$d'value%1$d'")),
        Arguments.of(Named.of("Key starting with a single quote & ending with a number", "'key%1$d"),
            Named.of("Value starting & ending with a number & elosed within quotes", "%1$d'value%1$d'")),
        Arguments.of(Named.of("Key ending with a number", "key%1$d"),
            Named.of("Value starting & ending with a number & containing null character & new line character",
                "%1$dvalue\n\0%1$d")),
        Arguments.of(Named.of("Key ending with a number & containing a null character", "key\0%1$d"),
            Named.of("Value starting & ending with a number & elosed within quotes", "%1$dvalue\r%1$d")));
  }

  @BeforeAll
  public static void init() throws NativeLibraryNotLoadedException {
    ManagedRawSSTFileReader.loadLibrary();
  }


  @ParameterizedTest
  @MethodSource("keyValueFormatArgs")
  public void testSSTDumpIteratorWithKeyFormat(String keyFormat, String valueFormat) throws Exception {
    TreeMap<Pair<String, Integer>, String> keys = IntStream.range(0, 100).boxed().collect(Collectors.toMap(
        i -> Pair.of(String.format(keyFormat, i), i % 2),
        i -> i % 2 == 0 ? "" : String.format(valueFormat, i),
        (v1, v2) -> v2,
        TreeMap::new));
    File file = createSSTFileWithKeys(keys);
    try (ManagedOptions options = new ManagedOptions();
         ManagedRawSSTFileReader<ManagedRawSSTFileIterator.KeyValue> reader = new ManagedRawSSTFileReader<>(
             options, file.getAbsolutePath(), 2 * 1024 * 1024)) {
      List<Optional<String>> testBounds = TestUtils.getTestingBounds(keys.keySet().stream()
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1, TreeMap::new)));
      for (Optional<String> keyStart : testBounds) {
        for (Optional<String> keyEnd : testBounds) {
          Map<Pair<String, Integer>, String> expectedKeys = keys.entrySet().stream()
              .filter(e -> keyStart.map(s -> e.getKey().getKey().compareTo(s) >= 0).orElse(true))
              .filter(e -> keyEnd.map(s -> e.getKey().getKey().compareTo(s) < 0).orElse(true))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
          Optional<ManagedSlice> lowerBound = keyStart.map(s -> new ManagedSlice(StringUtils.string2Bytes(s)));
          Optional<ManagedSlice> upperBound = keyEnd.map(s -> new ManagedSlice(StringUtils.string2Bytes(s)));
          try (ManagedRawSSTFileIterator<ManagedRawSSTFileIterator.KeyValue> iterator
                   = reader.newIterator(Function.identity(), lowerBound.orElse(null), upperBound.orElse(null))) {
            while (iterator.hasNext()) {
              ManagedRawSSTFileIterator.KeyValue r = iterator.next();
              String key = StringUtils.bytes2String(r.getKey());
              Pair<String, Integer> recordKey = Pair.of(key, r.getType());
              assertThat(expectedKeys).containsKey(recordKey);
              assertEquals(Optional.ofNullable(expectedKeys.get(recordKey)).orElse(""),
                  StringUtils.bytes2String(r.getValue()));
              expectedKeys.remove(recordKey);
            }
            assertEquals(0, expectedKeys.size());
          } finally {
            lowerBound.ifPresent(ManagedSlice::close);
            upperBound.ifPresent(ManagedSlice::close);
          }
        }
      }
    }
  }
}
