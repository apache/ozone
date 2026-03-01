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

import static org.apache.hadoop.hdds.utils.db.IteratorType.KEY_AND_VALUE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.Table.CloseableKeyValue;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

/**
 */
public class TestRDBStorePoolBackedCodecBufferIterator
    extends TestRDBStoreAbstractIterator<RDBStorePoolBackedCodecBufferIterator> {

  @BeforeAll
  public static void init() {
    ManagedRocksObjectUtils.loadRocksDBLibrary();
  }

  @BeforeEach
  public void setupTest() {
    CodecBuffer.enableLeakDetection();
  }

  @Override
  RDBStorePoolBackedCodecBufferIterator newIterator() throws RocksDatabaseException {
    return new RDBStorePoolBackedCodecBufferIterator(getItrInitializer(), null, null, KEY_AND_VALUE, 1);
  }

  RDBStorePoolBackedCodecBufferIterator newIterator(int poolSize) throws RocksDatabaseException {
    return new RDBStorePoolBackedCodecBufferIterator(getItrInitializer(), null, null, KEY_AND_VALUE,
        poolSize);
  }

  @Override
  RDBStorePoolBackedCodecBufferIterator newIterator(byte[] prefix) throws RocksDatabaseException {
    return new RDBStorePoolBackedCodecBufferIterator(getItrInitializer(), getRocksTableMock(), prefix, KEY_AND_VALUE,
        1);
  }

  Answer<Integer> newAnswerInt(String name, int b) {
    return newAnswer(name, (byte) b);
  }

  Answer<Integer> newAnswer(String name, byte... b) {
    return invocation -> {
      System.out.printf("answer %s: %s%n", name, StringUtils.bytes2Hex(b));
      Object[] args = invocation.getArguments();
      final ByteBuffer buffer = (ByteBuffer) args[0];
      return writeToBuffer(buffer, b);
    };
  }

  private int writeToBuffer(ByteBuffer buffer, byte... bytesToWrite) {
    buffer.clear();
    buffer.put(bytesToWrite);
    buffer.flip();
    return bytesToWrite.length;
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 5, 10})
  public void testRDBStoreCodecBufferIterGetsFailBeyondMaxBuffers(int maxBuffers)
      throws InterruptedException, TimeoutException, RocksDatabaseException {
    List<CloseableKeyValue<CodecBuffer, CodecBuffer>> vals = new ArrayList<>();
    when(getRocksDBIteratorMock().isValid()).thenReturn(true);
    AtomicInteger counter = new AtomicInteger(0);

    when(getRocksDBIteratorMock().key(any()))
        .thenAnswer(i -> writeToBuffer(i.getArgument(0), (byte)counter.getAndIncrement()));
    when(getRocksDBIteratorMock().value(any()))
        .thenAnswer(i -> writeToBuffer(i.getArgument(0), (byte)counter.getAndIncrement()));
    try (RDBStorePoolBackedCodecBufferIterator iterator = newIterator(maxBuffers)) {
      for (int i = 0; i < maxBuffers; i++) {
        vals.add(iterator.next());
      }
      assertEquals(Math.max(maxBuffers, 0), vals.size());
      ExecutorService executor = Executors.newSingleThreadExecutor();
      AtomicReference<CompletableFuture<Boolean>> nextThread = new AtomicReference<>(CompletableFuture.supplyAsync(
          () -> {
            CloseableKeyValue<CodecBuffer, CodecBuffer> v = iterator.next();
            vals.add(v);
            return true;
          },
          executor));
      if (maxBuffers < 1) {
        // Number of max buffers is always going to be at least 1.
        GenericTestUtils.waitFor(() -> nextThread.get().isDone() && nextThread.get().join(), 10, 100);
        nextThread.set(CompletableFuture.supplyAsync(
            () -> {
              CloseableKeyValue<CodecBuffer, CodecBuffer> v = iterator.next();
              vals.add(v);
              return true;
            },
            executor));
      }
      assertEquals(Math.max(1, maxBuffers), vals.size());
      for (int i = 0; i < vals.size(); i++) {
        assertEquals(2 * i, vals.get(i).getKey().getArray()[0]);
        assertEquals(2 * i + 1, vals.get(i).getValue().getArray()[0]);
      }
      assertFalse(nextThread.get().isDone());
      int size = vals.size();
      vals.get(0).close();
      GenericTestUtils.waitFor(() -> nextThread.get().isDone() && nextThread.get().join(), 10, 100);
      assertEquals(size + 1, vals.size());
      assertEquals(2 * size, vals.get(size).getKey().getArray()[0]);
      assertEquals(2 * size + 1, vals.get(size).getValue().getArray()[0]);
      for (CloseableKeyValue<CodecBuffer, CodecBuffer> v : vals) {
        v.close();
      }
      executor.shutdown();
    }
  }

  @Test
  public void testForEachRemaining() throws Exception {
    when(getRocksDBIteratorMock().isValid())
        .thenReturn(true, true, true, true, true, true, false);
    when(getRocksDBIteratorMock().key(any()))
        .then(newAnswerInt("key1", 0x00))
        .then(newAnswerInt("key2", 0x01))
        .then(newAnswerInt("key3", 0x02))
        .thenThrow(new NoSuchElementException());
    when(getRocksDBIteratorMock().value(any()))
        .then(newAnswerInt("val1", 0x7f))
        .then(newAnswerInt("val2", 0x7e))
        .then(newAnswerInt("val3", 0x7d))
        .thenThrow(new NoSuchElementException());

    List<KeyValue<byte[], byte[]>> remaining = new ArrayList<>();
    try (RDBStorePoolBackedCodecBufferIterator i = newIterator()) {
      i.forEachRemaining(kvSupplier -> {
        try {
          remaining.add(Table.newKeyValue(
              kvSupplier.getKey().getArray(), kvSupplier.getValue().getArray()));
        } finally {
          kvSupplier.close();
        }
      });

      System.out.println("remaining: " + remaining);
      assertArrayEquals(new byte[]{0x00}, remaining.get(0).getKey());
      assertArrayEquals(new byte[]{0x7f}, remaining.get(0).getValue());
      assertArrayEquals(new byte[]{0x01}, remaining.get(1).getKey());
      assertArrayEquals(new byte[]{0x7e}, remaining.get(1).getValue());
      assertArrayEquals(new byte[]{0x02}, remaining.get(2).getKey());
      assertArrayEquals(new byte[]{0x7d}, remaining.get(2).getValue());
    }

    CodecTestUtil.gc();
  }

  @Test
  public void testHasNextDoesNotDependsOnIsvalid() throws Exception {
    when(getRocksDBIteratorMock().isValid()).thenReturn(true, true, false);

    try (RDBStorePoolBackedCodecBufferIterator i = newIterator()) {
      assertTrue(i.hasNext());
      try (CloseableKeyValue<CodecBuffer, CodecBuffer> val = i.next()) {
        assertFalse(i.hasNext());
        assertThrows(NoSuchElementException.class, i::next);
        assertFalse(i.hasNext());
      }
    }

    CodecTestUtil.gc();
  }

  @Test
  public void testNextCallsIsValidThenGetsTheValueAndStepsToNext()
      throws Exception {
    when(getRocksDBIteratorMock().isValid()).thenReturn(true);
    InOrder verifier = inOrder(getRocksDBIteratorMock());
    try (RDBStorePoolBackedCodecBufferIterator i = newIterator()) {
      i.next().close();
    }

    verifier.verify(getRocksDBIteratorMock()).isValid();
    verifier.verify(getRocksDBIteratorMock()).key(any());
    verifier.verify(getRocksDBIteratorMock()).value(any());
    verifier.verify(getRocksDBIteratorMock()).next();

    CodecTestUtil.gc();
  }

  @Test
  public void testConstructorSeeksToFirstElement() throws Exception {
    newIterator().close();

    verify(getRocksDBIteratorMock(), times(1)).seekToFirst();

    CodecTestUtil.gc();
  }

  @Test
  public void testSeekToFirstSeeks() throws Exception {
    try (RDBStorePoolBackedCodecBufferIterator i = newIterator()) {
      i.seekToFirst();
    }
    verify(getRocksDBIteratorMock(), times(2)).seekToFirst();

    CodecTestUtil.gc();
  }

  @Test
  public void testSeekToLastSeeks() throws Exception {
    try (RDBStorePoolBackedCodecBufferIterator i = newIterator()) {
      i.seekToLast();
    }

    verify(getRocksDBIteratorMock(), times(1)).seekToLast();

    CodecTestUtil.gc();
  }

  @Test
  public void testSeekWithInvalidValue() throws RocksDatabaseException {
    when(getRocksDBIteratorMock().isValid()).thenReturn(false);

    try (RDBStorePoolBackedCodecBufferIterator i = newIterator();
         CodecBuffer target = CodecBuffer.wrap(new byte[] {0x55});
         CloseableKeyValue<CodecBuffer, CodecBuffer> valSupplier = i.seek(target)) {
      assertFalse(i.hasNext());
      InOrder verifier = inOrder(getRocksDBIteratorMock());
      verify(getRocksDBIteratorMock(), times(1)).seekToFirst(); //at construct time
      verify(getRocksDBIteratorMock(), never()).seekToLast();
      verifier.verify(getRocksDBIteratorMock(), times(1))
          .seek(any(ByteBuffer.class));
      verifier.verify(getRocksDBIteratorMock(), times(2)).isValid();
      verifier.verify(getRocksDBIteratorMock(), never()).key(any());
      verifier.verify(getRocksDBIteratorMock(), never()).value(any());
      assertNull(valSupplier);
    }
  }

  @Test
  public void testSeekReturnsTheActualKey() throws Exception {
    when(getRocksDBIteratorMock().isValid()).thenReturn(true);
    when(getRocksDBIteratorMock().key(any()))
        .then(newAnswerInt("key1", 0x00));
    when(getRocksDBIteratorMock().value(any()))
        .then(newAnswerInt("val1", 0x7f));

    try (RDBStorePoolBackedCodecBufferIterator i = newIterator();
         CodecBuffer target = CodecBuffer.wrap(new byte[] {0x55});
         CloseableKeyValue<CodecBuffer, CodecBuffer> valSupplier = i.seek(target)) {
      assertTrue(i.hasNext());
      InOrder verifier = inOrder(getRocksDBIteratorMock());
      verify(getRocksDBIteratorMock(), times(1)).seekToFirst(); //at construct time
      verify(getRocksDBIteratorMock(), never()).seekToLast();
      verifier.verify(getRocksDBIteratorMock(), times(1))
          .seek(any(ByteBuffer.class));
      verifier.verify(getRocksDBIteratorMock(), times(1)).isValid();
      verifier.verify(getRocksDBIteratorMock(), times(1)).key(any());
      verifier.verify(getRocksDBIteratorMock(), times(1)).value(any());
      assertArrayEquals(new byte[] {0x00}, valSupplier.getKey().getArray());
      assertArrayEquals(new byte[] {0x7f}, valSupplier.getValue().getArray());
    }

    CodecTestUtil.gc();
  }

  @Test
  public void testGettingTheKeyIfIteratorIsValid() throws Exception {
    when(getRocksDBIteratorMock().isValid()).thenReturn(true);
    when(getRocksDBIteratorMock().key(any()))
        .then(newAnswerInt("key1", 0x00));

    byte[] key = null;
    try (RDBStorePoolBackedCodecBufferIterator i = newIterator()) {
      if (i.hasNext()) {
        try (CloseableKeyValue<CodecBuffer, CodecBuffer> kv = i.next()) {
          key = kv.getKey().getArray();
        }
      }
    }

    InOrder verifier = inOrder(getRocksDBIteratorMock());

    verifier.verify(getRocksDBIteratorMock(), times(2)).isValid();
    verifier.verify(getRocksDBIteratorMock(), times(1)).key(any());
    assertArrayEquals(new byte[]{0x00}, key);

    CodecTestUtil.gc();
  }

  @Test
  public void testGettingTheValueIfIteratorIsValid() throws Exception {
    when(getRocksDBIteratorMock().isValid()).thenReturn(true);
    when(getRocksDBIteratorMock().key(any()))
        .then(newAnswerInt("key1", 0x00));
    when(getRocksDBIteratorMock().value(any()))
        .then(newAnswerInt("val1", 0x7f));

    byte[] key = null;
    byte[] value = null;
    try (RDBStorePoolBackedCodecBufferIterator i = newIterator()) {
      if (i.hasNext()) {
        try (CloseableKeyValue<CodecBuffer, CodecBuffer> entry = i.next()) {
          key = entry.getKey().getArray();
          value = entry.getValue().getArray();
        }
      }
    }

    InOrder verifier = inOrder(getRocksDBIteratorMock());

    verifier.verify(getRocksDBIteratorMock(), times(2)).isValid();
    verifier.verify(getRocksDBIteratorMock(), times(1)).key(any());
    assertArrayEquals(new byte[]{0x00}, key);
    assertArrayEquals(new byte[]{0x7f}, value);

    CodecTestUtil.gc();
  }

  @Test
  public void testRemovingFromDBActuallyDeletesFromTable() throws Exception {
    final byte[] testKey = new byte[10];
    ThreadLocalRandom.current().nextBytes(testKey);
    when(getRocksDBIteratorMock().isValid()).thenReturn(true);
    when(getRocksDBIteratorMock().key(any()))
        .then(newAnswer("key1", testKey));

    try (RDBStorePoolBackedCodecBufferIterator i = newIterator(null)) {
      try (CloseableKeyValue kv = i.next()) {
        i.removeFromDB();
      }

    }

    InOrder verifier = inOrder(getRocksDBIteratorMock(), getRocksTableMock());

    verifier.verify(getRocksDBIteratorMock(), times(1)).isValid();
    verifier.verify(getRocksTableMock(), times(1))
        .delete(ByteBuffer.wrap(testKey));
    CodecTestUtil.gc();
  }

  @Test
  public void testRemoveFromDBWithoutDBTableSet() throws Exception {
    try (RDBStorePoolBackedCodecBufferIterator i = newIterator()) {
      assertThrows(UnsupportedOperationException.class,
          i::removeFromDB);
    }

    CodecTestUtil.gc();
  }

  @Test
  public void testCloseCloses() throws Exception {
    newIterator().close();

    verify(getRocksDBIteratorMock(), times(1)).close();

    CodecTestUtil.gc();
  }

  @Test
  public void testNullPrefixedIterator() throws Exception {
    try (RDBStorePoolBackedCodecBufferIterator i = newIterator()) {
      verify(getRocksDBIteratorMock(), times(1)).seekToFirst();
      clearInvocations(getRocksDBIteratorMock());
      when(getRocksDBIteratorMock().isValid()).thenReturn(true);
      i.seekToFirst();
      verify(getRocksDBIteratorMock(), times(0)).isValid();
      verify(getRocksDBIteratorMock(), times(0)).key(any());
      verify(getRocksDBIteratorMock(), times(1)).seekToFirst();
      clearInvocations(getRocksDBIteratorMock());
      i.hasNext();
      verify(getRocksDBIteratorMock(), times(1)).isValid();
      clearInvocations(getRocksDBIteratorMock());

      assertTrue(i.hasNext());
      // hasNext shouldn't make isValid() redundant calls.
      verify(getRocksDBIteratorMock(), times(1)).isValid();
      verify(getRocksDBIteratorMock(), times(0)).key(any());

      i.seekToLast();
      verify(getRocksDBIteratorMock(), times(1)).seekToLast();
    }

    CodecTestUtil.gc();
  }
}
