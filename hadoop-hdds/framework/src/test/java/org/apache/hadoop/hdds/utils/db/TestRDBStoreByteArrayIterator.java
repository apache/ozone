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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.rocksdb.RocksIterator;

/**
 * This test prescribe expected behaviour
 * from {@link RDBStoreByteArrayIterator} which wraps
 * RocksDB's own iterator. Ozone internally in TypedTableIterator uses, the
 * RDBStoreIterator to provide iteration over table elements in a typed manner.
 * The tests are to ensure we access RocksDB via the iterator properly.
 */
public class TestRDBStoreByteArrayIterator {

  private RocksIterator rocksDBIteratorMock;
  private ManagedRocksIterator managedRocksIterator;
  private RDBTable rocksTableMock;

  @BeforeEach
  public void setup() {
    rocksDBIteratorMock = mock(RocksIterator.class);
    managedRocksIterator = new ManagedRocksIterator(rocksDBIteratorMock);
    rocksTableMock = mock(RDBTable.class);
    Logger.getLogger(ManagedRocksObjectUtils.class).setLevel(Level.DEBUG);
  }

  RDBStoreByteArrayIterator newIterator() {
    return new RDBStoreByteArrayIterator(managedRocksIterator, null, null);
  }

  RDBStoreByteArrayIterator newIterator(byte[] prefix) {
    return new RDBStoreByteArrayIterator(
        managedRocksIterator, rocksTableMock, prefix);
  }

  @Test
  public void testForeachRemainingCallsConsumerWithAllElements() throws IOException {
    when(rocksDBIteratorMock.isValid())
        .thenReturn(true, true, true, false);
    when(rocksDBIteratorMock.key())
        .thenReturn(new byte[]{0x00}, new byte[]{0x01},
            new byte[]{0x02})
        .thenThrow(new NoSuchElementException());
    when(rocksDBIteratorMock.value())
        .thenReturn(new byte[]{0x7f}, new byte[]{0x7e}, new byte[]{0x7d})
        .thenThrow(new NoSuchElementException());

    final Consumer<UncheckedAutoCloseableSupplier<RawKeyValue<byte[]>>> consumerStub
        = mock(Consumer.class);

    try (RDBStoreByteArrayIterator iter = newIterator()) {
      iter.forEachRemaining(consumerStub);
      ArgumentCaptor<UncheckedAutoCloseableSupplier<RawKeyValue<byte[]>>> capture =
          forClass(UncheckedAutoCloseableSupplier.class);
      verify(consumerStub, times(3)).accept(capture.capture());
      assertArrayEquals(
          new byte[]{0x00}, capture.getAllValues().get(0).get().getKey());
      assertArrayEquals(
          new byte[]{0x7f}, capture.getAllValues().get(0).get().getValue());
      assertArrayEquals(
          new byte[]{0x01}, capture.getAllValues().get(1).get().getKey());
      assertArrayEquals(
          new byte[]{0x7e}, capture.getAllValues().get(1).get().getValue());
      assertArrayEquals(
          new byte[]{0x02}, capture.getAllValues().get(2).get().getKey());
      assertArrayEquals(
          new byte[]{0x7d}, capture.getAllValues().get(2).get().getValue());
    }
  }

  @Test
  public void testHasNextDoesNotDependsOnIsvalid() {
    when(rocksDBIteratorMock.isValid()).thenReturn(true, false);

    try (RDBStoreByteArrayIterator iter = newIterator()) {
      assertTrue(iter.hasNext());
      assertTrue(iter.hasNext());
      iter.next();
      assertFalse(iter.hasNext());
      assertThrows(NoSuchElementException.class, iter::next);
      assertFalse(iter.hasNext());
    }
  }

  @Test
  public void testNextCallsIsValidThenGetsTheValueAndStepsToNext() {
    when(rocksDBIteratorMock.isValid()).thenReturn(true);
    RDBStoreByteArrayIterator iter = newIterator();

    InOrder verifier = inOrder(rocksDBIteratorMock);

    iter.next();

    verifier.verify(rocksDBIteratorMock).isValid();
    verifier.verify(rocksDBIteratorMock).key();
    verifier.verify(rocksDBIteratorMock).value();
    verifier.verify(rocksDBIteratorMock).next();
  }

  @Test
  public void testConstructorSeeksToFirstElement() {
    newIterator();

    verify(rocksDBIteratorMock, times(1)).seekToFirst();
  }

  @Test
  public void testSeekToFirstSeeks() {
    RDBStoreByteArrayIterator iter = newIterator();

    iter.seekToFirst();

    verify(rocksDBIteratorMock, times(2)).seekToFirst();
  }

  @Test
  public void testSeekToLastSeeks() {
    RDBStoreByteArrayIterator iter = newIterator();

    iter.seekToLast();

    verify(rocksDBIteratorMock, times(1)).seekToLast();
  }

  @Test
  public void testSeekWithInvalidValue() {
    when(rocksDBIteratorMock.isValid()).thenReturn(false);

    try (RDBStoreByteArrayIterator iter = newIterator()) {
      final UncheckedAutoCloseableSupplier<RawKeyValue<byte[]>> val = iter.seek(new byte[] {0x55});
      InOrder verifier = inOrder(rocksDBIteratorMock);

      verify(rocksDBIteratorMock, times(1)).seekToFirst(); //at construct time
      verify(rocksDBIteratorMock, never()).seekToLast();
      verifier.verify(rocksDBIteratorMock, times(1)).seek(any(byte[].class));
      verifier.verify(rocksDBIteratorMock, times(1)).isValid();
      verifier.verify(rocksDBIteratorMock, never()).key();
      verifier.verify(rocksDBIteratorMock, never()).value();
      assertNull(val.get());
    }
  }

  @Test
  public void testSeekReturnsTheActualKey() throws Exception {
    when(rocksDBIteratorMock.isValid()).thenReturn(true);
    when(rocksDBIteratorMock.key()).thenReturn(new byte[]{0x00});
    when(rocksDBIteratorMock.value()).thenReturn(new byte[]{0x7f});

    RDBStoreByteArrayIterator iter = newIterator();
    final UncheckedAutoCloseableSupplier<RawKeyValue<byte[]>> val = iter.seek(new byte[]{0x55});

    InOrder verifier = inOrder(rocksDBIteratorMock);

    verify(rocksDBIteratorMock, times(1)).seekToFirst(); //at construct time
    verify(rocksDBIteratorMock, never()).seekToLast();
    verifier.verify(rocksDBIteratorMock, times(1)).seek(any(byte[].class));
    verifier.verify(rocksDBIteratorMock, times(1)).isValid();
    verifier.verify(rocksDBIteratorMock, times(1)).key();
    verifier.verify(rocksDBIteratorMock, times(1)).value();
    assertArrayEquals(new byte[]{0x00}, val.get().getKey());
    assertArrayEquals(new byte[]{0x7f}, val.get().getValue());
  }

  @Test
  public void testGettingTheKeyIfIteratorIsValid() throws Exception {
    when(rocksDBIteratorMock.isValid()).thenReturn(true);
    when(rocksDBIteratorMock.key()).thenReturn(new byte[]{0x00});

    RDBStoreByteArrayIterator iter = newIterator();
    byte[] key = null;
    if (iter.hasNext()) {
      try (UncheckedAutoCloseableSupplier<RawKeyValue<byte[]>> entry = iter.next()) {
        key = entry.get().getKey();
      }
    }

    InOrder verifier = inOrder(rocksDBIteratorMock);

    verifier.verify(rocksDBIteratorMock, times(1)).isValid();
    verifier.verify(rocksDBIteratorMock, times(1)).key();
    assertArrayEquals(new byte[]{0x00}, key);
  }

  @Test
  public void testGettingTheValueIfIteratorIsValid() throws Exception {
    when(rocksDBIteratorMock.isValid()).thenReturn(true);
    when(rocksDBIteratorMock.key()).thenReturn(new byte[]{0x00});
    when(rocksDBIteratorMock.value()).thenReturn(new byte[]{0x7f});

    try (RDBStoreByteArrayIterator iter = newIterator()) {
      UncheckedAutoCloseableSupplier<RawKeyValue<byte[]>> entry;
      byte[] key = null;
      byte[] value = null;
      if (iter.hasNext()) {
        entry = iter.next();
        key = entry.get().getKey();
        value = entry.get().getValue();
      }
      InOrder verifier = inOrder(rocksDBIteratorMock);

      verifier.verify(rocksDBIteratorMock, times(1)).isValid();
      verifier.verify(rocksDBIteratorMock, times(1)).key();
      assertArrayEquals(new byte[]{0x00}, key);
      assertArrayEquals(new byte[]{0x7f}, value);
    }
  }

  @Test
  public void testRemovingFromDBActuallyDeletesFromTable() throws Exception {
    byte[] testKey = new byte[]{0x00};
    when(rocksDBIteratorMock.isValid()).thenReturn(true);
    when(rocksDBIteratorMock.key()).thenReturn(testKey);

    RDBStoreByteArrayIterator iter = newIterator(null);
    iter.removeFromDB();

    InOrder verifier = inOrder(rocksDBIteratorMock, rocksTableMock);

    verifier.verify(rocksDBIteratorMock, times(1)).isValid();
    verifier.verify(rocksTableMock, times(1)).delete(testKey);
  }

  @Test
  public void testRemoveFromDBWithoutDBTableSet() {
    RDBStoreByteArrayIterator iter = newIterator();
    assertThrows(UnsupportedOperationException.class,
        iter::removeFromDB);
  }

  @Test
  public void testCloseCloses() throws Exception {
    RDBStoreByteArrayIterator iter = newIterator();
    iter.close();

    verify(rocksDBIteratorMock, times(1)).close();
  }

  @Test
  public void testNullPrefixedIterator() throws IOException {
    RDBStoreByteArrayIterator iter = newIterator(null);
    verify(rocksDBIteratorMock, times(1)).seekToFirst();
    clearInvocations(rocksDBIteratorMock);
    when(rocksDBIteratorMock.isValid()).thenReturn(true);
    iter.seekToFirst();
    verify(rocksDBIteratorMock, times(1)).isValid();
    verify(rocksDBIteratorMock, times(1)).key();
    verify(rocksDBIteratorMock, times(1)).seekToFirst();
    clearInvocations(rocksDBIteratorMock);
    assertTrue(iter.hasNext());
    verify(rocksDBIteratorMock, times(0)).isValid();
    verify(rocksDBIteratorMock, times(0)).key();

    iter.seekToLast();
    verify(rocksDBIteratorMock, times(1)).seekToLast();

    iter.close();
  }

  @Test
  public void testNormalPrefixedIterator() throws IOException {
    byte[] testPrefix = "sample".getBytes(StandardCharsets.UTF_8);
    RDBStoreByteArrayIterator iter = newIterator(testPrefix);
    verify(rocksDBIteratorMock, times(1)).seek(testPrefix);
    clearInvocations(rocksDBIteratorMock);
    when(rocksDBIteratorMock.isValid()).thenReturn(true);
    when(rocksDBIteratorMock.key()).thenReturn(testPrefix);
    iter.seekToFirst();
    verify(rocksDBIteratorMock, times(1)).isValid();
    verify(rocksDBIteratorMock, times(1)).key();
    verify(rocksDBIteratorMock, times(1)).seek(testPrefix);
    clearInvocations(rocksDBIteratorMock);
    assertTrue(iter.hasNext());
    // hasNext shouldn't make isValid() redundant calls.
    verify(rocksDBIteratorMock, times(0)).isValid();
    verify(rocksDBIteratorMock, times(0)).key();
    Exception e =
        assertThrows(Exception.class, () -> iter.seekToLast(), "Prefixed iterator does not support seekToLast");
    assertInstanceOf(UnsupportedOperationException.class, e);

    iter.close();
  }
}
