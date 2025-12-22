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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.ManagedRawSstFileIterator.KeyValue;
import org.apache.hadoop.hdds.utils.db.managed.ManagedEnvOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSlice;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileWriter;
import org.apache.hadoop.hdds.utils.db.managed.ManagedTypeUtil;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test for ManagedRawSSTFileReaderIterator.
 */
class TestManagedRawSstFileIterator {

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

  private static Stream<Arguments> keyValueFormatArgs() {
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
            Named.of("Value starting & ending with a number & elosed within quotes", "%1$dvalue\r%1$d")),
        Arguments.of(Named.of("Key with prefix length 5k of random alphaNumeric string",
                new StringBuilder(RandomStringUtils.secure().nextAlphanumeric(5 << 10))
                    .append("key%1$d").toString()),
            Named.of("Value with prefix length 5k of random alphaNumeric string",
                new StringBuilder(RandomStringUtils.secure().nextAlphanumeric(5 << 10))
                    .append("%1$dvalue%1$d").toString())))
        .flatMap(i -> Arrays.stream(IteratorType.values()).map(type -> Arguments.of(i.get()[0], i.get()[1], type)));
  }

  @ParameterizedTest
  @MethodSource("keyValueFormatArgs")
  public void testSSTDumpIteratorWithKeyFormat(String keyFormat, String valueFormat, IteratorType type)
      throws Exception {
    TreeMap<Pair<String, Integer>, String> keys = IntStream.range(0, 100).boxed().collect(Collectors.toMap(
        i -> Pair.of(String.format(keyFormat, i), i % 2),
        i -> i % 2 == 0 ? "" : String.format(valueFormat, i),
        (v1, v2) -> v2, TreeMap::new));
    File file = createSSTFileWithKeys(keys);
    try (ManagedOptions options = new ManagedOptions()) {
      List<Optional<String>> testBounds = TestUtils.getTestingBounds(keys.keySet().stream()
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1, TreeMap::new)));
      for (Optional<String> keyStart : testBounds) {
        for (Optional<String> keyEnd : testBounds) {
          Map<Pair<String, Integer>, String> expectedKeys = keys.entrySet().stream()
              .filter(e -> keyStart.map(s -> e.getKey().getKey().compareTo(s) >= 0).orElse(true))
              .filter(e -> keyEnd.map(s -> e.getKey().getKey().compareTo(s) < 0).orElse(true))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1, TreeMap::new));

          Optional<ManagedSlice> lowerBound = keyStart.map(s ->
              new ManagedSlice(ManagedTypeUtil.getInternalKey(StringUtils.string2Bytes(s), options)));
          Optional<ManagedSlice> upperBound = keyEnd.map(s ->
              new ManagedSlice(ManagedTypeUtil.getInternalKey(StringUtils.string2Bytes(s), options)));
          try (ManagedRawSstFileIterator<KeyValue> iterator =
                   new ManagedRawSstFileIterator<>(file.getAbsolutePath(), options, lowerBound,
                       upperBound, type, Function.identity())) {
            Iterator<Map.Entry<Pair<String, Integer>, String>> expectedKeyItr = expectedKeys.entrySet().iterator();
            while (iterator.hasNext()) {
              KeyValue r = iterator.next();
              assertTrue(expectedKeyItr.hasNext());
              Map.Entry<Pair<String, Integer>, String> expectedKey = expectedKeyItr.next();
              String key = r.getKey() == null ? null : StringCodec.get().fromCodecBuffer(r.getKey());
              assertEquals(type.readKey() ? expectedKey.getKey().getKey() : null, key);
              assertEquals(type.readValue() ? expectedKey.getValue() : null,
                  type.readValue() ? StringCodec.get().fromCodecBuffer(r.getValue()) : r.getValue());
              expectedKeyItr.remove();
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
