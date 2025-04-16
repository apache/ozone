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

import java.io.IOException;
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
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;
import org.rocksdb.RocksIterator;

/**
 * This test is similar to {@link TestRDBStoreByteArrayIterator}
 * except that this test is for {@link RDBStoreCodecBufferIterator}.
 */
public class TestRDBStoreCodecBufferIterator {

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

  RDBStoreCodecBufferIterator newIterator() {
    return newIterator(1);
  }

  RDBStoreCodecBufferIterator newIterator(int numberOfThreads) {
    return new RDBStoreCodecBufferIterator(managedRocksIterator, null, null, numberOfThreads);
  }

  RDBStoreCodecBufferIterator newIterator(CodecBuffer prefix) {
    return new RDBStoreCodecBufferIterator(
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
    List<UncheckedAutoCloseableSupplier<RawKeyValue<CodecBuffer>>> vals = new ArrayList<>();
    when(rocksIteratorMock.isValid()).thenReturn(true);
    AtomicInteger counter = new AtomicInteger(0);

    when(rocksIteratorMock.key(any()))
        .thenAnswer(i -> writeToBuffer(i.getArgument(0), (byte)counter.getAndIncrement()));
    when(rocksIteratorMock.value(any()))
        .thenAnswer(i -> writeToBuffer(i.getArgument(0), (byte)counter.getAndIncrement()));
    try (RDBStoreCodecBufferIterator iterator = newIterator(maxBuffers)) {
      for (int i = 0; i < maxBuffers - 1; i++) {
        vals.add(iterator.next());
      }
      assertEquals(Math.max(maxBuffers - 1, 0), vals.size());
      ExecutorService executor = Executors.newSingleThreadExecutor();
      AtomicReference<CompletableFuture<Boolean>> nextThread = new AtomicReference<>(CompletableFuture.supplyAsync(
          () -> {
            UncheckedAutoCloseableSupplier<RawKeyValue<CodecBuffer>> v = iterator.next();
            vals.add(v);
            return true;
          },
          executor));

      if (maxBuffers <= 1) {
        // Number of max buffers is always going to be 2. We need atleast 2 buffers one for getting the next value
        // and one for returning the current value.
        GenericTestUtils.waitFor(() -> nextThread.get().isDone() && nextThread.get().join(), 10, 100);
        System.out.println(Thread.currentThread().getName());
        nextThread.set(CompletableFuture.supplyAsync(
            () -> {
              UncheckedAutoCloseableSupplier<RawKeyValue<CodecBuffer>> v = iterator.next();
              vals.add(v);
              return true;
            },
            executor));
      }
      assertEquals(Math.max(maxBuffers - 1, 1), vals.size());
      for (int i = 0; i < vals.size(); i++) {
        assertEquals(2 * i, vals.get(i).get().getKey().getArray()[0]);
        assertEquals(2 * i + 1, vals.get(i).get().getValue().getArray()[0]);
      }
      assertFalse(nextThread.get().isDone());
      int size = vals.size();
      vals.get(0).close();
      GenericTestUtils.waitFor(() -> nextThread.get().isDone() && nextThread.get().join(), 10, 100);
      assertEquals(size + 1, vals.size());
      assertEquals(2 * size, vals.get(size).get().getKey().getArray()[0]);
      assertEquals(2 * size + 1, vals.get(size).get().getValue().getArray()[0]);
      for (UncheckedAutoCloseableSupplier<RawKeyValue<CodecBuffer>> v : vals) {
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
    try (RDBStoreCodecBufferIterator i = newIterator()) {
      i.forEachRemaining(kvSupplier -> {
        try {
          Table.KeyValue<CodecBuffer, CodecBuffer> kv = kvSupplier.get();
          remaining.add(RawKeyValue.create(
              kv.getKey().getArray(), kv.getValue().getArray()));
        } catch (IOException e) {
          throw new RuntimeException(e);
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

    try (RDBStoreCodecBufferIterator i = newIterator()) {
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
    try (RDBStoreCodecBufferIterator i = newIterator()) {
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
    try (RDBStoreCodecBufferIterator i = newIterator()) {
      i.seekToFirst();
    }
    verify(rocksIteratorMock, times(2)).seekToFirst();

    CodecTestUtil.gc();
  }

  @Test
  public void testSeekToLastSeeks() throws Exception {
    try (RDBStoreCodecBufferIterator i = newIterator()) {
      i.seekToLast();
    }

    verify(rocksIteratorMock, times(1)).seekToLast();

    CodecTestUtil.gc();
  }

  @Test
  public void testSeekWithInvalidValue() {
    when(rocksIteratorMock.isValid()).thenReturn(false);

    try (RDBStoreCodecBufferIterator i = newIterator();
         CodecBuffer target = CodecBuffer.wrap(new byte[] {0x55});
         UncheckedAutoCloseableSupplier<RawKeyValue<CodecBuffer>> valSupplier = i.seek(target)) {
      InOrder verifier = inOrder(rocksIteratorMock);
      verify(rocksIteratorMock, times(1)).seekToFirst(); //at construct time
      verify(rocksIteratorMock, never()).seekToLast();
      verifier.verify(rocksIteratorMock, times(1))
          .seek(any(ByteBuffer.class));
      verifier.verify(rocksIteratorMock, times(1)).isValid();
      verifier.verify(rocksIteratorMock, never()).key(any());
      verifier.verify(rocksIteratorMock, never()).value(any());
      assertNull(valSupplier.get());
    }
  }

  @Test
  public void testSeekReturnsTheActualKey() throws Exception {
    when(rocksIteratorMock.isValid()).thenReturn(true);
    when(rocksIteratorMock.key(any()))
        .then(newAnswerInt("key1", 0x00));
    when(rocksIteratorMock.value(any()))
        .then(newAnswerInt("val1", 0x7f));

    try (RDBStoreCodecBufferIterator i = newIterator();
         CodecBuffer target = CodecBuffer.wrap(new byte[] {0x55});
         UncheckedAutoCloseableSupplier<RawKeyValue<CodecBuffer>> valSupplier = i.seek(target)) {
      InOrder verifier = inOrder(rocksIteratorMock);

      verify(rocksIteratorMock, times(1)).seekToFirst(); //at construct time
      verify(rocksIteratorMock, never()).seekToLast();
      verifier.verify(rocksIteratorMock, times(1))
          .seek(any(ByteBuffer.class));
      verifier.verify(rocksIteratorMock, times(1)).isValid();
      verifier.verify(rocksIteratorMock, times(1)).key(any());
      verifier.verify(rocksIteratorMock, times(1)).value(any());
      assertArrayEquals(new byte[] {0x00}, valSupplier.get().getKey().getArray());
      assertArrayEquals(new byte[] {0x7f}, valSupplier.get().getValue().getArray());
    }

    CodecTestUtil.gc();
  }

  @Test
  public void testGettingTheKeyIfIteratorIsValid() throws Exception {
    when(rocksIteratorMock.isValid()).thenReturn(true);
    when(rocksIteratorMock.key(any()))
        .then(newAnswerInt("key1", 0x00));

    byte[] key = null;
    try (RDBStoreCodecBufferIterator i = newIterator()) {
      if (i.hasNext()) {
        try (UncheckedAutoCloseableSupplier<RawKeyValue<CodecBuffer>> kv = i.next()) {
          key = kv.get().getKey().getArray();
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
    try (RDBStoreCodecBufferIterator i = newIterator()) {
      if (i.hasNext()) {
        try (UncheckedAutoCloseableSupplier<RawKeyValue<CodecBuffer>> entry = i.next()) {
          key = entry.get().getKey().getArray();
          value = entry.get().getValue().getArray();
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

    try (RDBStoreCodecBufferIterator i = newIterator(null)) {
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
    try (RDBStoreCodecBufferIterator i = newIterator()) {
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

    try (RDBStoreCodecBufferIterator i = newIterator()) {
      verify(rocksIteratorMock, times(1)).seekToFirst();
      clearInvocations(rocksIteratorMock);
      when(rocksIteratorMock.isValid()).thenReturn(true);
      i.seekToFirst();
      verify(rocksIteratorMock, times(1)).isValid();
      verify(rocksIteratorMock, times(1)).key(any());
      verify(rocksIteratorMock, times(1)).seekToFirst();
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
    try (RDBStoreCodecBufferIterator i = newIterator(
        CodecBuffer.wrap(prefixBytes))) {
      final ByteBuffer prefix = ByteBuffer.wrap(prefixBytes);
      verify(rocksIteratorMock, times(1)).seek(prefix);
      clearInvocations(rocksIteratorMock);
      when(rocksIteratorMock.isValid()).thenReturn(true);
      when(rocksIteratorMock.key(any()))
          .then(newAnswer("key1", prefixBytes));
      i.seekToFirst();
      verify(rocksIteratorMock, times(1)).isValid();
      verify(rocksIteratorMock, times(1)).key(any());
      verify(rocksIteratorMock, times(1)).seek(prefix);
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
