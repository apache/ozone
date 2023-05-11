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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Test for ManagedSSTDumpIterator.
 */
public class TestManagedSSTDumpIterator {

  private void testSSTDumpIteratorWithKeys(
      TreeMap<Pair<String, Integer>, String> keys) throws Exception {
    File file = File.createTempFile("tmp_sst_file", ".sst");
    file.deleteOnExit();
    try (ManagedEnvOptions envOptions = new ManagedEnvOptions();
         ManagedOptions managedOptions = new ManagedOptions();
         ManagedSstFileWriter sstFileWriter = new ManagedSstFileWriter(
             envOptions, managedOptions)) {
      sstFileWriter.open(file.getAbsolutePath());
      for (Map.Entry<Pair<String, Integer>, String> entry : keys.entrySet()) {
        if (entry.getKey().getValue() == 0) {
          sstFileWriter.delete(entry.getKey().getKey()
              .getBytes(StandardCharsets.UTF_8));
        } else {
          sstFileWriter.put(entry.getKey().getKey()
                  .getBytes(StandardCharsets.UTF_8),
              entry.getValue().getBytes(StandardCharsets.UTF_8));
        }
      }
      sstFileWriter.finish();
      sstFileWriter.close();
      ExecutorService executorService =
          new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
              new ArrayBlockingQueue<>(1),
              new ThreadPoolExecutor.CallerRunsPolicy());
      ManagedSSTDumpTool tool = new ManagedSSTDumpTool(executorService, 8192);
      try (ManagedSSTDumpIterator<ManagedSSTDumpIterator.KeyValue> iterator =
              new ManagedSSTDumpIterator<ManagedSSTDumpIterator.KeyValue>(
              tool, file.getAbsolutePath(), new ManagedOptions()) {

            @Override
            protected KeyValue getTransformedValue(Optional<KeyValue> value) {
              return value.orElse(null);
            }
          }) {
        while (iterator.hasNext()) {
          ManagedSSTDumpIterator.KeyValue r = iterator.next();
          Pair<String, Integer> recordKey = Pair.of(new String(r.getKey(),
              StandardCharsets.UTF_8), r.getType());
          Assertions.assertTrue(keys.containsKey(recordKey));
          Assertions.assertEquals(
              Optional.ofNullable(keys.get(recordKey)).orElse(""),
              new String(r.getValue(), StandardCharsets.UTF_8));
          keys.remove(recordKey);
        }
        Assertions.assertEquals(0, keys.size());
      }
      executorService.shutdown();
    }
  }

  @Native("Managed Rocks Tools")
  @ParameterizedTest
  @ArgumentsSource(KeyValueFormatArgumentProvider.class)
  public void testSSTDumpIteratorWithKeyFormat(String keyFormat,
              String valueFormat) throws Exception {
    TreeMap<Pair<String, Integer>, String> keys =
        IntStream.range(0, 100).boxed().collect(
            Collectors.toMap(
                i -> Pair.of(String.format(keyFormat, i), i % 2),
                i -> i % 2 == 0 ? "" : String.format(valueFormat, i),
                (v1, v2) -> v2,
                TreeMap::new));
    testSSTDumpIteratorWithKeys(keys);
  }
}

class KeyValueFormatArgumentProvider implements ArgumentsProvider {
  @Override
  public Stream<? extends Arguments> provideArguments(
      ExtensionContext context) {
    return Stream.of(
        Arguments.of("'key%1$d=>", "%1$dvalue'"),
        Arguments.of("key%1$d", "%1$dvalue%1$d"),
        Arguments.of("'key%1$d", "%1$d'value%1$d'"),
        Arguments.of("'key%1$d", "%1$d'value%1$d'"),
        Arguments.of("key%1$d", "%1$dvalue\n\0%1$d"),
        Arguments.of("key\0%1$d", "%1$dvalue\r%1$d")
    );
  }
}
