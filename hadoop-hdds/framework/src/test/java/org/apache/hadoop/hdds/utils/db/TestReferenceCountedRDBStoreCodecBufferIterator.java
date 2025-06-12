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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
import org.apache.hadoop.hdds.utils.db.iterator.CloseableRawKeyValue;
import org.apache.hadoop.hdds.utils.db.iterator.ReferenceCountedRDBStoreCodecBufferIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;
import org.rocksdb.RocksIterator;

/**
 * This test is similar to {@link TestReferenceCountedRDBStoreByteArrayIterator}
 * except that this test is for {@link ReferenceCountedRDBStoreCodecBufferIterator}.
 */
public class TestReferenceCountedRDBStoreCodecBufferIterator {

  private RocksIterator rocksIteratorMock;
  private ManagedRocksIterator managedRocksIterator;
  private RDBTable rdbTableMock;

  @BeforeEach
  public void setup() {
    CodecBuffer.enableLeakDetection();
    rocksIteratorMock = mock(RocksIterator.class);
    managedRocksIterator = newManagedRocksIterator();
    rdbTableMock = mock(RDBTable.class);
    Logger.getLogger(ManagedRocksObjectUtils.class).setLevel(Level.DEBUG);
  }

  ManagedRocksIterator newManagedRocksIterator() {
    return new ManagedRocksIterator(rocksIteratorMock);
  }

  ReferenceCountedRDBStoreCodecBufferIterator newIterator() {
    return newIterator(1);
  }

  ReferenceCountedRDBStoreCodecBufferIterator newIterator(int maxNumberOfBuffers) {
    return new ReferenceCountedRDBStoreCodecBufferIterator(managedRocksIterator, null, null, maxNumberOfBuffers);
  }

  ReferenceCountedRDBStoreCodecBufferIterator newIterator(CodecBuffer prefix) {
    return new ReferenceCountedRDBStoreCodecBufferIterator(
        managedRocksIterator, rdbTableMock, prefix, 1);
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
      throws InterruptedException, TimeoutException {
    List<CloseableRawKeyValue<CodecBuffer>> vals = new ArrayList<>();
    when(rocksIteratorMock.isValid()).thenReturn(true);
    AtomicInteger counter = new AtomicInteger(0);

    when(rocksIteratorMock.key(any()))
        .thenAnswer(i -> writeToBuffer(i.getArgument(0), (byte)counter.getAndIncrement()));
    when(rocksIteratorMock.value(any()))
        .thenAnswer(i -> writeToBuffer(i.getArgument(0), (byte)counter.getAndIncrement()));
    try (ReferenceCountedRDBStoreCodecBufferIterator iterator = newIterator(maxBuffers)) {
      for (int i = 0; i < maxBuffers; i++) {
        vals.add(iterator.next());
      }
      assertEquals(Math.max(maxBuffers, 0), vals.size());
      ExecutorService executor = Executors.newSingleThreadExecutor();
      AtomicReference<CompletableFuture<Boolean>> nextThread = new AtomicReference<>(CompletableFuture.supplyAsync(
          () -> {
            CloseableRawKeyValue<CodecBuffer> v = iterator.next();
            vals.add(v);
            return true;
          },
          executor));

      if (maxBuffers < 1) {
        // Number of max buffers is always going to be at least 1. We need at least 1 buffers one for getting the next
        // value and one for returning the current value.
        GenericTestUtils.waitFor(() -> nextThread.get().isDone() && nextThread.get().join(), 10, 100);
        System.out.println(Thread.currentThread().getName());
        nextThread.set(CompletableFuture.supplyAsync(
            () -> {
              CloseableRawKeyValue<CodecBuffer> v = iterator.next();
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
      for (CloseableRawKeyValue<CodecBuffer> v : vals) {
        v.close();
      }
      executor.shutdown();
    }
  }

  @Test
  public void testForEachRemaining() throws Exception {
    when(rocksIteratorMock.isValid())
        .thenReturn(true, true, true, false);
    when(rocksIteratorMock.key(any()))
        .then(newAnswerInt("key1", 0x00))
        .then(newAnswerInt("key2", 0x01))
        .then(newAnswerInt("key3", 0x02))
        .thenThrow(new NoSuchElementException());
    when(rocksIteratorMock.value(any()))
        .then(newAnswerInt("val1", 0x7f))
        .then(newAnswerInt("val2", 0x7e))
        .then(newAnswerInt("val3", 0x7d))
        .thenThrow(new NoSuchElementException());

    List<Table.KeyValue<byte[], byte[]>> remaining = new ArrayList<>();
    try (ReferenceCountedRDBStoreCodecBufferIterator i = newIterator()) {
      i.forEachRemaining(kvSupplier -> {
        try {
          remaining.add(RawKeyValue.create(
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
    when(rocksIteratorMock.isValid()).thenReturn(true, false);

    try (ReferenceCountedRDBStoreCodecBufferIterator i = newIterator()) {
      assertTrue(i.hasNext());
      assertTrue(i.hasNext());
      i.next();
      assertFalse(i.hasNext());
      assertThrows(NoSuchElementException.class, i::next);
      assertFalse(i.hasNext());
    }

    CodecTestUtil.gc();
  }

  @Test
  public void testNextCallsIsValidThenGetsTheValueAndStepsToNext()
      throws Exception {
    when(rocksIteratorMock.isValid()).thenReturn(true);
    InOrder verifier = inOrder(rocksIteratorMock);
    try (ReferenceCountedRDBStoreCodecBufferIterator i = newIterator()) {
      i.next();
    }

    verifier.verify(rocksIteratorMock).isValid();
    verifier.verify(rocksIteratorMock).key(any());
    verifier.verify(rocksIteratorMock).value(any());
    verifier.verify(rocksIteratorMock).next();

    CodecTestUtil.gc();
  }

  @Test
  public void testConstructorSeeksToFirstElement() throws Exception {
    newIterator().close();

    verify(rocksIteratorMock, times(1)).seekToFirst();

    CodecTestUtil.gc();
  }

  @Test
  public void testSeekToFirstSeeks() throws Exception {
    try (ReferenceCountedRDBStoreCodecBufferIterator i = newIterator()) {
      i.seekToFirst();
    }
    verify(rocksIteratorMock, times(2)).seekToFirst();

    CodecTestUtil.gc();
  }

  @Test
  public void testSeekToLastSeeks() throws Exception {
    try (ReferenceCountedRDBStoreCodecBufferIterator i = newIterator()) {
      i.seekToLast();
    }

    verify(rocksIteratorMock, times(1)).seekToLast();

    CodecTestUtil.gc();
  }

  @Test
  public void testSeekWithInvalidValue() {
    when(rocksIteratorMock.isValid()).thenReturn(false);

    try (ReferenceCountedRDBStoreCodecBufferIterator i = newIterator();
         CodecBuffer target = CodecBuffer.wrap(new byte[] {0x55});
         CloseableRawKeyValue<CodecBuffer> valSupplier = i.seek(target)) {
      assertFalse(i.hasNext());
      InOrder verifier = inOrder(rocksIteratorMock);
      verify(rocksIteratorMock, times(1)).seekToFirst(); //at construct time
      verify(rocksIteratorMock, never()).seekToLast();
      verifier.verify(rocksIteratorMock, times(1))
          .seek(any(ByteBuffer.class));
      verifier.verify(rocksIteratorMock, times(1)).isValid();
      verifier.verify(rocksIteratorMock, never()).key(any());
      verifier.verify(rocksIteratorMock, never()).value(any());
      assertNull(valSupplier);
    }
  }

  @Test
  public void testSeekReturnsTheActualKey() throws Exception {
    when(rocksIteratorMock.isValid()).thenReturn(true);
    when(rocksIteratorMock.key(any()))
        .then(newAnswerInt("key1", 0x00));
    when(rocksIteratorMock.value(any()))
        .then(newAnswerInt("val1", 0x7f));

    try (ReferenceCountedRDBStoreCodecBufferIterator i = newIterator();
         CodecBuffer target = CodecBuffer.wrap(new byte[] {0x55});
         CloseableRawKeyValue<CodecBuffer> valSupplier = i.seek(target)) {
      assertTrue(i.hasNext());
      InOrder verifier = inOrder(rocksIteratorMock);
      verify(rocksIteratorMock, times(1)).seekToFirst(); //at construct time
      verify(rocksIteratorMock, never()).seekToLast();
      verifier.verify(rocksIteratorMock, times(1))
          .seek(any(ByteBuffer.class));
      verifier.verify(rocksIteratorMock, times(1)).isValid();
      verifier.verify(rocksIteratorMock, times(1)).key(any());
      verifier.verify(rocksIteratorMock, times(1)).value(any());
      assertArrayEquals(new byte[] {0x00}, valSupplier.getKey().getArray());
      assertArrayEquals(new byte[] {0x7f}, valSupplier.getValue().getArray());
    }

    CodecTestUtil.gc();
  }

  @Test
  public void testGettingTheKeyIfIteratorIsValid() throws Exception {
    when(rocksIteratorMock.isValid()).thenReturn(true);
    when(rocksIteratorMock.key(any()))
        .then(newAnswerInt("key1", 0x00));

    byte[] key = null;
    try (ReferenceCountedRDBStoreCodecBufferIterator i = newIterator()) {
      if (i.hasNext()) {
        try (CloseableRawKeyValue<CodecBuffer> kv = i.next()) {
          key = kv.getKey().getArray();
        }
      }
    }

    InOrder verifier = inOrder(rocksIteratorMock);

    verifier.verify(rocksIteratorMock, times(1)).isValid();
    verifier.verify(rocksIteratorMock, times(1)).key(any());
    assertArrayEquals(new byte[]{0x00}, key);

    CodecTestUtil.gc();
  }

  @Test
  public void testGettingTheValueIfIteratorIsValid() throws Exception {
    when(rocksIteratorMock.isValid()).thenReturn(true);
    when(rocksIteratorMock.key(any()))
        .then(newAnswerInt("key1", 0x00));
    when(rocksIteratorMock.value(any()))
        .then(newAnswerInt("val1", 0x7f));

    byte[] key = null;
    byte[] value = null;
    try (ReferenceCountedRDBStoreCodecBufferIterator i = newIterator()) {
      if (i.hasNext()) {
        try (CloseableRawKeyValue<CodecBuffer> entry = i.next()) {
          key = entry.getKey().getArray();
          value = entry.getValue().getArray();
        }
      }
    }

    InOrder verifier = inOrder(rocksIteratorMock);

    verifier.verify(rocksIteratorMock, times(1)).isValid();
    verifier.verify(rocksIteratorMock, times(1)).key(any());
    assertArrayEquals(new byte[]{0x00}, key);
    assertArrayEquals(new byte[]{0x7f}, value);

    CodecTestUtil.gc();
  }

  @Test
  public void testRemovingFromDBActuallyDeletesFromTable() throws Exception {
    final byte[] testKey = new byte[10];
    ThreadLocalRandom.current().nextBytes(testKey);
    when(rocksIteratorMock.isValid()).thenReturn(true);
    when(rocksIteratorMock.key(any()))
        .then(newAnswer("key1", testKey));

    try (ReferenceCountedRDBStoreCodecBufferIterator i = newIterator(null)) {
      i.next();
      i.removeFromDB();
    }

    InOrder verifier = inOrder(rocksIteratorMock, rdbTableMock);

    verifier.verify(rocksIteratorMock, times(1)).isValid();
    verifier.verify(rdbTableMock, times(1))
        .delete(ByteBuffer.wrap(testKey));
    CodecTestUtil.gc();
  }

  @Test
  public void testRemoveFromDBWithoutDBTableSet() throws Exception {
    try (ReferenceCountedRDBStoreCodecBufferIterator i = newIterator()) {
      assertThrows(UnsupportedOperationException.class,
          i::removeFromDB);
    }

    CodecTestUtil.gc();
  }

  @Test
  public void testCloseCloses() throws Exception {
    newIterator().close();

    verify(rocksIteratorMock, times(1)).close();

    CodecTestUtil.gc();
  }

  @Test
  public void testNullPrefixedIterator() throws Exception {

    try (ReferenceCountedRDBStoreCodecBufferIterator i = newIterator()) {
      verify(rocksIteratorMock, times(1)).seekToFirst();
      clearInvocations(rocksIteratorMock);
      when(rocksIteratorMock.isValid()).thenReturn(true);
      i.seekToFirst();
      verify(rocksIteratorMock, times(0)).isValid();
      verify(rocksIteratorMock, times(0)).key(any());
      verify(rocksIteratorMock, times(1)).seekToFirst();
      clearInvocations(rocksIteratorMock);
      i.hasNext();
      verify(rocksIteratorMock, times(1)).isValid();
      verify(rocksIteratorMock, times(1)).key(any());
      clearInvocations(rocksIteratorMock);

      assertTrue(i.hasNext());
      // hasNext shouldn't make isValid() redundant calls.
      verify(rocksIteratorMock, times(0)).isValid();
      verify(rocksIteratorMock, times(0)).key(any());

      i.seekToLast();
      verify(rocksIteratorMock, times(1)).seekToLast();
    }

    CodecTestUtil.gc();
  }

  @Test
  public void testNormalPrefixedIterator() throws Exception {
    final byte[] prefixBytes = "sample".getBytes(StandardCharsets.UTF_8);
    try (ReferenceCountedRDBStoreCodecBufferIterator i = newIterator(
        CodecBuffer.wrap(prefixBytes))) {
      final ByteBuffer prefix = ByteBuffer.wrap(prefixBytes);
      verify(rocksIteratorMock, times(1)).seek(prefix);
      clearInvocations(rocksIteratorMock);
      when(rocksIteratorMock.isValid()).thenReturn(true);
      when(rocksIteratorMock.key(any()))
          .then(newAnswer("key1", prefixBytes));
      i.seekToFirst();
      verify(rocksIteratorMock, times(0)).isValid();
      verify(rocksIteratorMock, times(0)).key(any());
      verify(rocksIteratorMock, times(1)).seek(prefix);
      clearInvocations(rocksIteratorMock);
      i.hasNext();
      verify(rocksIteratorMock, times(1)).isValid();
      verify(rocksIteratorMock, times(1)).key(any());
      verify(rocksIteratorMock, times(0)).seek(prefix);
      clearInvocations(rocksIteratorMock);

      assertTrue(i.hasNext());
      // Ensure redundant native call is made since key and value already have been fetched as part of seek
      verify(rocksIteratorMock, times(0)).isValid();
      verify(rocksIteratorMock, times(0)).key(any());

      Exception e =
          assertThrows(Exception.class, () -> i.seekToLast(), "Prefixed iterator does not support seekToLast");
      assertInstanceOf(UnsupportedOperationException.class, e);
    }

    CodecTestUtil.gc();
  }
}
