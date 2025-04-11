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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.ratis.util.function.CheckedFunction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class for ParallelTableOperator.
 */
public class TestSplitTableIteratorOp {

  private static ThrottledThreadpoolExecutor executor;
  private static final Logger LOG = LoggerFactory.getLogger(TestSplitTableIteratorOp.class);
  private Map<Integer, Integer> visited;

  @BeforeAll
  public static void init() {
    executor = new ThrottledThreadpoolExecutor(10);
  }

  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(executor);
  }

  @BeforeEach
  public void setup() {
    visited = new ConcurrentHashMap<>();
  }

  private static Stream<Arguments> getParallelTableOperatorArguments() {
    return Stream.of(
        Arguments.of(1, null, null, 1),
        Arguments.of(1, 100, 200, 1),
        Arguments.of(5, null, null, 8),
        Arguments.of(5, 100, 200, 8),
        Arguments.of(8, null, null, 5),
        Arguments.of(8, 100, 200, 5)
    );
  }

  private static Stream<Arguments> getParallelTableOperatorArgumentsBoundsException() {
    return Stream.of(
        Arguments.of(1, null, null),
        Arguments.of(1, 100, 200),
        Arguments.of(5, null, null),
        Arguments.of(5, 100, 200),
        Arguments.of(8, null, null),
        Arguments.of(8, 100, 200)
    );
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private void testParallelTableOperator(Table<Integer, Integer> intTable,
      SplitTableIteratorOp<Table<Integer, Integer>, Integer, Integer> parallelTableOperator,
      int maxKeyIdx, int keyJumps, Integer start, Integer end, List<Integer> bounds,
      CheckedFunction<Table.KeyValue<Integer, Integer>, Void, IOException> opr, Supplier<Exception> expectedException)
      throws IOException, ExecutionException, InterruptedException {
    List<Integer> seekedKeys = Collections.synchronizedList(new ArrayList<>());
    when(intTable.iterator()).thenAnswer(i -> {
      Table.KeyValueIterator<Integer, Integer> itr = (Table.KeyValueIterator<Integer, Integer>) spy(i.callRealMethod());
      doAnswer(j -> {
        seekedKeys.add(null);
        return j.callRealMethod();
      }).when(itr).seekToFirst();
      doAnswer(j -> {
        seekedKeys.add(j.getArgument(0));
        return j.callRealMethod();
      }).when(itr).seek(any(Integer.class));
      return itr;
    });
    Map<Integer, Integer> expectedKeys = new HashMap<>();
    for (int i = 0; i < maxKeyIdx; i += keyJumps) {
      intTable.put(i, i);
      if ((start == null || start <= i) && (end == null || end > i)) {
        expectedKeys.put(i, 1);
      }
    }

    List<Integer> expectedSeekedKeys = bounds.subList(0, bounds.size() - 1);
    if (expectedException != null) {
      Exception e = assertThrows(ExecutionException.class,
          () -> parallelTableOperator.performTaskOnTableVals(start, end, opr, LOG, 1));
      Exception expectedEx = expectedException.get();
      assertEquals(expectedEx.getClass(), e.getCause().getClass());
      assertEquals(expectedEx.getMessage(), e.getCause().getMessage());
    } else {
      parallelTableOperator.performTaskOnTableVals(start, end, opr, LOG, 1);
      assertEquals(expectedKeys, visited);
      seekedKeys.sort((a, b) -> {
        if (Objects.equals(a, b)) {
          return 0;
        }
        if (a == null) {
          return -1;
        }
        if (b == null) {
          return 1;
        }
        return Integer.compare(a, b);
      });
      assertEquals(expectedSeekedKeys, seekedKeys);
    }
  }

  @ParameterizedTest
  @MethodSource("getParallelTableOperatorArguments")
  public void testParallelTableOperator(int keyJumps, Integer start, Integer end, int boundJumps)
      throws IOException, ExecutionException, InterruptedException {
    int maxKeyIdx = 1001;
    List<Integer> bounds = new ArrayList<>();
    bounds.add(start);
    for (int i = (start == null ? 0 : start) + boundJumps;
         i < (end == null ? (maxKeyIdx - 1) : end); i += boundJumps) {
      bounds.add(i);
    }
    bounds.add(end);
    Table<Integer, Integer> intTable = spy(new InMemoryTestTable<>(IntegerCodec.get()));
    CheckedFunction<Table.KeyValue<Integer, Integer>, Void, IOException> opr = kv -> {
      visited.compute(kv.getKey(), (k, v) -> v == null ? 1 : v + 1);
      return null;
    };

    SplitTableIteratorOp<Table<Integer, Integer>, Integer, Integer> splitTableIteratorOp =
        new SplitTableIteratorOp<Table<Integer, Integer>, Integer, Integer>(executor, intTable, IntegerCodec.get()) {
          @Override
          protected List<Integer> getBounds(Integer startKey, Integer endKey) {
            return bounds;
          }
        };
    testParallelTableOperator(intTable, splitTableIteratorOp, maxKeyIdx, keyJumps, start, end, bounds, opr, null);
  }


  @ParameterizedTest
  @MethodSource("getParallelTableOperatorArgumentsBoundsException")
  public void testParallelTableOperatorWithBoundsException(int keyJumps, Integer start, Integer end)
      throws IOException, ExecutionException, InterruptedException {
    int maxKeyIdx = 1001;
    List<Integer> bounds = new ArrayList<>();
    bounds.add(start);
    bounds.add(end);
    Table<Integer, Integer> intTable = spy(new InMemoryTestTable<>(IntegerCodec.get()));

    CheckedFunction<Table.KeyValue<Integer, Integer>, Void, IOException> opr = kv -> {
      visited.compute(kv.getKey(), (k, v) -> v == null ? 1 : v + 1);
      return null;
    };

    SplitTableIteratorOp<Table<Integer, Integer>, Integer, Integer> splitTableIteratorOp =
        new SplitTableIteratorOp<Table<Integer, Integer>, Integer, Integer>(executor, intTable, IntegerCodec.get()) {
          @Override
          protected List<Integer> getBounds(Integer startKey, Integer endKey) throws IOException {
            throw new IOException("Exception while getting bounds");
          }
        };
    testParallelTableOperator(intTable, splitTableIteratorOp, maxKeyIdx, keyJumps, start, end, bounds, opr, null);
  }

  @ParameterizedTest
  @MethodSource("getParallelTableOperatorArguments")
  public void testParallelTableOperatorWithException(int keyJumps, Integer start, Integer end, int boundJumps)
      throws IOException, ExecutionException, InterruptedException {
    int maxKeyIdx = 1001;
    List<Integer> bounds = new ArrayList<>();
    bounds.add(start);
    for (int i = (start == null ? 0 : start) + boundJumps;
         i < (end == null ? (maxKeyIdx - 1) : end); i += boundJumps) {
      bounds.add(i);
    }
    bounds.add(end);
    AtomicInteger minException = new AtomicInteger(Integer.MAX_VALUE);
    Table<Integer, Integer> intTable = spy(new InMemoryTestTable<>(IntegerCodec.get()));
    CheckedFunction<Table.KeyValue<Integer, Integer>, Void, IOException> opr = kv -> {
      Integer key = kv.getKey();
      visited.compute(kv.getKey(), (k, v) -> v == null ? 1 : v + 1);
      if (key != 0 && key % (3 * keyJumps) == 0) {
        minException.updateAndGet((prev) -> Math.min(prev, key));
        throw new IOException("Exception while getting bounds for key " + key);
      }
      return null;
    };

    SplitTableIteratorOp<Table<Integer, Integer>, Integer, Integer> splitTableIteratorOp =
        new SplitTableIteratorOp<Table<Integer, Integer>, Integer, Integer>(executor, intTable, IntegerCodec.get()) {
          @Override
          protected List<Integer> getBounds(Integer startKey, Integer endKey) {
            return bounds;
          }
        };
    testParallelTableOperator(intTable, splitTableIteratorOp, maxKeyIdx, keyJumps, start, end, bounds, opr,
        () -> new IOException("Exception while getting bounds for key " + minException.get()));
  }
}
