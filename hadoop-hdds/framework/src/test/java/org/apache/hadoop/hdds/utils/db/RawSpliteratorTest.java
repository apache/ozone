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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Spliterator;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Unit test class for verifying the behavior of the {@code RawSpliterator}.
 * This class contains tests to evaluate the functionality of its methods,
 * such as {@code tryAdvance}, {@code trySplit}, and error handling during
 * method execution.
 */
class RawSpliteratorTest {

  @Test
  void testTryAdvanceWithValidElement() throws IOException {
    // Mock dependencies
    TableIterator<String, ReferenceCountedRDBStoreAbstractIterator.AutoCloseableRawKeyValue<String>> rawIteratorMock =
        mock(TableIterator.class);
    ReferenceCountedRDBStoreAbstractIterator.AutoCloseableRawKeyValue<String> rawKeyValueMock =
        mock(ReferenceCountedRDBStoreAbstractIterator.AutoCloseableRawKeyValue.class);

    when(rawIteratorMock.hasNext()).thenReturn(true);
    when(rawIteratorMock.next()).thenReturn(rawKeyValueMock);

    RawSpliterator<String, String, String> rawSpliterator = new MockRawSpliterator(1) {
      @Override
      TableIterator<String, ReferenceCountedRDBStoreAbstractIterator.AutoCloseableRawKeyValue<String>> getRawIterator(String prefix,
          String startKey, int maxParallelism) {
        return rawIteratorMock;
      }
    };

    when(rawKeyValueMock.getKey()).thenReturn("key");
    when(rawKeyValueMock.getValue()).thenReturn("value");

    Consumer<Table.KeyValue<String, String>> action = keyValue -> {
      try {
        assertEquals("key", keyValue.getKey());
        assertEquals("value", keyValue.getValue());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };

    boolean result = rawSpliterator.tryAdvance(action);

    assertTrue(result);
    verify(rawIteratorMock).hasNext();
    verify(rawIteratorMock).next();
    verify(rawKeyValueMock).close();
  }

  @Test
  void testTryAdvanceWithNoElement() throws IOException {
    TableIterator<String, ReferenceCountedRDBStoreAbstractIterator.AutoCloseableRawKeyValue<String>> rawIteratorMock =
        mock(TableIterator.class);

    when(rawIteratorMock.hasNext()).thenReturn(false);

    RawSpliterator<String, String, String> rawSpliterator = new MockRawSpliterator(1) {
      @Override
      TableIterator<String, ReferenceCountedRDBStoreAbstractIterator.AutoCloseableRawKeyValue<String>> getRawIterator(String prefix,
          String startKey, int maxParallelism) {
        return rawIteratorMock;
      }
    };

    Consumer<Table.KeyValue<String, String>> action = keyValue -> fail("Action should not be called");

    boolean result = rawSpliterator.tryAdvance(action);

    assertFalse(result);
    verify(rawIteratorMock).hasNext();
    verify(rawIteratorMock, never()).next();
  }

  @Test
  void testTryAdvanceWhenConvertThrowsIOException() throws IOException {
    TableIterator<String, ReferenceCountedRDBStoreAbstractIterator.AutoCloseableRawKeyValue<String>> rawIteratorMock =
        mock(TableIterator.class);
    ReferenceCountedRDBStoreAbstractIterator.AutoCloseableRawKeyValue<String> rawKeyValueMock =
        mock(ReferenceCountedRDBStoreAbstractIterator.AutoCloseableRawKeyValue.class);

    when(rawIteratorMock.hasNext()).thenReturn(true);
    when(rawIteratorMock.next()).thenReturn(rawKeyValueMock);

    RawSpliterator<String, String, String> rawSpliterator = new RawSpliterator<String, String, String>(null, null, 1,
        true) {

      @Override
      Table.KeyValue<String, String> convert(RawKeyValue<String> kv) throws IOException {
        throw new IOException("Mocked exception");
      }

      @Override
      TableIterator<String, ReferenceCountedRDBStoreAbstractIterator.AutoCloseableRawKeyValue<String>> getRawIterator(String prefix,
          String startKey, int maxParallelism) {
        return rawIteratorMock;
      }
    };
    rawSpliterator.initializeIterator();

    Consumer<Table.KeyValue<String, String>> action = keyValue -> {
    };

    Exception exception = assertThrows(IllegalStateException.class, () -> rawSpliterator.tryAdvance(action));

    assertEquals("Failed next()", exception.getMessage());
    assertNotNull(exception.getCause());
    assertEquals(IOException.class, exception.getCause().getClass());
    assertEquals("Mocked exception", exception.getCause().getMessage());
    verify(rawIteratorMock).hasNext();
    verify(rawIteratorMock).next();
  }

  @Test
  void testTrySplits() throws IOException {
    TableIterator<String, ReferenceCountedRDBStoreAbstractIterator.AutoCloseableRawKeyValue<String>> rawIteratorMock =
        mock(TableIterator.class);

    RawSpliterator<String, String, String> rawSpliterator = new MockRawSpliterator(2) {
      @Override
      TableIterator<String, ReferenceCountedRDBStoreAbstractIterator.AutoCloseableRawKeyValue<String>> getRawIterator(String prefix,
          String startKey, int maxParallelism) {
        return rawIteratorMock;
      }
    };
    Spliterator<Table.KeyValue<String, String>> split1 = rawSpliterator.trySplit();
    Spliterator<Table.KeyValue<String, String>> split2 = rawSpliterator.trySplit();
    assertNotNull(split1);
    assertNull(split2);
    rawSpliterator.close();
    Spliterator<Table.KeyValue<String, String>> split3 = split1.trySplit();
    assertNotNull(split3);
  }

  private abstract static class MockRawSpliterator extends RawSpliterator<String, String, String> {

    MockRawSpliterator(int maxParallelism) throws IOException {
      super(null, null, maxParallelism, true);
      super.initializeIterator();
    }

    @Override
    Table.KeyValue<String, String> convert(RawKeyValue<String> kv) {
      return Table.newKeyValue(kv.getKey(), kv.getValue());
    }
  }
}
