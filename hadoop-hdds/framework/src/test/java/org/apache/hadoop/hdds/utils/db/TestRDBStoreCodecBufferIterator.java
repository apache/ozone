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
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

/**
 * This test is similar to {@link TestRDBStoreByteArrayIterator}
 * except that this test is for {@link RDBStoreCodecBufferIterator}.
 */
public class TestRDBStoreCodecBufferIterator extends TestRDBStoreAbstractIterator<RDBStoreCodecBufferIterator> {

  @BeforeAll
  public static void init() {
    ManagedRocksObjectUtils.loadRocksDBLibrary();
  }

  @BeforeEach
  public void setupTest() {
    CodecBuffer.enableLeakDetection();
  }

  ManagedRocksIterator newManagedRocksIterator() {
    return new ManagedRocksIterator(getRocksDBIteratorMock());
  }

  @Override
  RDBStoreCodecBufferIterator newIterator() throws RocksDatabaseException {
    return new RDBStoreCodecBufferIterator(getItrInitializer(), null, null, KEY_AND_VALUE);
  }

  @Override
  RDBStoreCodecBufferIterator newIterator(byte[] prefix) throws RocksDatabaseException {
    return new RDBStoreCodecBufferIterator(getItrInitializer(), getRocksTableMock(), prefix, KEY_AND_VALUE);
  }

  Answer<Integer> newAnswerInt(String name, int b) {
    return newAnswer(name, (byte) b);
  }

  Answer<Integer> newAnswer(String name, byte... b) {
    return invocation -> {
      System.out.printf("answer %s: %s%n", name, StringUtils.bytes2Hex(b));
      Object[] args = invocation.getArguments();
      final ByteBuffer buffer = (ByteBuffer) args[0];
      buffer.clear();
      buffer.put(b);
      buffer.flip();
      return b.length;
    };
  }

  @Test
  public void testForEachRemaining() throws Exception {
    when(getRocksDBIteratorMock().isValid())
        .thenReturn(true, true, true, true, true, true, false);
    when(getRocksDBIteratorMock().key(any()))
        .then(newAnswerInt("key2", 0x00))
        .then(newAnswerInt("key3", 0x01))
        .then(newAnswerInt("key4", 0x02))
        .thenThrow(new NoSuchElementException());
    when(getRocksDBIteratorMock().value(any()))
        .then(newAnswerInt("val2", 0x7f))
        .then(newAnswerInt("val3", 0x7e))
        .then(newAnswerInt("val4", 0x7d))
        .thenThrow(new NoSuchElementException());

    List<Table.KeyValue<byte[], byte[]>> remaining = new ArrayList<>();
    try (RDBStoreCodecBufferIterator i = newIterator()) {
      i.forEachRemaining(kv ->
          remaining.add(Table.newKeyValue(kv.getKey().getArray(), kv.getValue().getArray())));

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
  public void testHasNextDependsOnIsvalid() throws Exception {
    when(getRocksDBIteratorMock().isValid()).thenReturn(true, false);

    try (RDBStoreCodecBufferIterator i = newIterator()) {
      assertTrue(i.hasNext());
      assertFalse(i.hasNext());
    }

    CodecTestUtil.gc();
  }

  @Test
  public void testNextCallsIsValidThenGetsTheValueAndStepsToNext()
      throws Exception {
    when(getRocksDBIteratorMock().isValid()).thenReturn(true);
    InOrder verifier = inOrder(getRocksDBIteratorMock());
    try (RDBStoreCodecBufferIterator i = newIterator()) {
      i.next();
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
    try (RDBStoreCodecBufferIterator i = newIterator()) {
      i.seekToFirst();
    }
    verify(getRocksDBIteratorMock(), times(2)).seekToFirst();

    CodecTestUtil.gc();
  }

  @Test
  public void testSeekToLastSeeks() throws Exception {
    try (RDBStoreCodecBufferIterator i = newIterator()) {
      i.seekToLast();
    }

    verify(getRocksDBIteratorMock(), times(1)).seekToLast();

    CodecTestUtil.gc();
  }

  @Test
  public void testSeekReturnsTheActualKey() throws Exception {
    when(getRocksDBIteratorMock().isValid()).thenReturn(true);
    when(getRocksDBIteratorMock().key(any()))
        .then(newAnswerInt("key1", 0x00));
    when(getRocksDBIteratorMock().value(any()))
        .then(newAnswerInt("val1", 0x7f));

    try (RDBStoreCodecBufferIterator i = newIterator();
         CodecBuffer target = CodecBuffer.wrap(new byte[]{0x55})) {
      final Table.KeyValue<CodecBuffer, CodecBuffer> val = i.seek(target);

      InOrder verifier = inOrder(getRocksDBIteratorMock());

      verify(getRocksDBIteratorMock(), times(1)).seekToFirst(); //at construct time
      verify(getRocksDBIteratorMock(), never()).seekToLast();
      verifier.verify(getRocksDBIteratorMock(), times(1))
          .seek(any(ByteBuffer.class));
      verifier.verify(getRocksDBIteratorMock(), times(1)).isValid();
      verifier.verify(getRocksDBIteratorMock(), times(1)).key(any());
      verifier.verify(getRocksDBIteratorMock(), times(1)).value(any());
      assertArrayEquals(new byte[]{0x00}, val.getKey().getArray());
      assertArrayEquals(new byte[]{0x7f}, val.getValue().getArray());
    }

    CodecTestUtil.gc();
  }

  @Test
  public void testGettingTheKeyIfIteratorIsValid() throws Exception {
    when(getRocksDBIteratorMock().isValid()).thenReturn(true);
    when(getRocksDBIteratorMock().key(any()))
        .then(newAnswerInt("key1", 0x00));

    byte[] key = null;
    try (RDBStoreCodecBufferIterator i = newIterator()) {
      if (i.hasNext()) {
        key = i.next().getKey().getArray();
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
    try (RDBStoreCodecBufferIterator i = newIterator()) {
      if (i.hasNext()) {
        Table.KeyValue<CodecBuffer, CodecBuffer> entry = i.next();
        key = entry.getKey().getArray();
        value = entry.getValue().getArray();
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

    try (RDBStoreCodecBufferIterator i = newIterator(null)) {
      i.next();
      i.removeFromDB();
    }

    InOrder verifier = inOrder(getRocksDBIteratorMock(), getRocksTableMock());

    verifier.verify(getRocksDBIteratorMock(), times(1)).isValid();
    verifier.verify(getRocksTableMock(), times(1))
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

    verify(getRocksDBIteratorMock(), times(1)).close();

    CodecTestUtil.gc();
  }

  @Test
  public void testNullPrefixedIterator() throws Exception {
    try (RDBStoreCodecBufferIterator i = newIterator()) {
      verify(getRocksDBIteratorMock(), times(1)).seekToFirst();
      clearInvocations(getRocksDBIteratorMock());

      i.seekToFirst();
      verify(getRocksDBIteratorMock(), times(1)).seekToFirst();
      clearInvocations(getRocksDBIteratorMock());

      when(getRocksDBIteratorMock().isValid()).thenReturn(true);
      assertTrue(i.hasNext());
      verify(getRocksDBIteratorMock(), times(1)).isValid();
      verify(getRocksDBIteratorMock(), times(0)).key(any());

      i.seekToLast();
      verify(getRocksDBIteratorMock(), times(1)).seekToLast();
    }

    CodecTestUtil.gc();
  }
}
