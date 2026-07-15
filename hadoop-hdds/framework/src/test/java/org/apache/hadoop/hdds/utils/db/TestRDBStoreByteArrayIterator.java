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
import static org.apache.hadoop.hdds.utils.db.IteratorType.KEY_ONLY;
import static org.apache.hadoop.hdds.utils.db.IteratorType.NEITHER;
import static org.apache.hadoop.hdds.utils.db.IteratorType.VALUE_ONLY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
    return new RDBStoreByteArrayIterator(managedRocksIterator, null, null, KEY_AND_VALUE);
  }

  RDBStoreByteArrayIterator newIterator(byte[] prefix) {
    return new RDBStoreByteArrayIterator(managedRocksIterator, rocksTableMock, prefix, KEY_AND_VALUE);
  }

  @Test
  public void testForeachRemainingCallsConsumerWithAllElements() {
    when(rocksDBIteratorMock.isValid())
        .thenReturn(true, true, true, true, true, true, true, false);
    when(rocksDBIteratorMock.key())
        .thenReturn(new byte[]{0x00}, new byte[]{0x00}, new byte[]{0x01},
            new byte[]{0x02})
        .thenThrow(new NoSuchElementException());
    when(rocksDBIteratorMock.value())
        .thenReturn(new byte[]{0x7f}, new byte[]{0x7f}, new byte[]{0x7e},
            new byte[]{0x7d})
        .thenThrow(new NoSuchElementException());

    final Consumer<Table.KeyValue<byte[], byte[]>> consumerStub
        = mock(Consumer.class);

    RDBStoreByteArrayIterator iter = newIterator();
    iter.forEachRemaining(consumerStub);

    final ArgumentCaptor<Table.KeyValue<byte[], byte[]>> capture = forClass(Table.KeyValue.class);
    verify(consumerStub, times(3)).accept(capture.capture());
    assertArrayEquals(
        new byte[]{0x00}, capture.getAllValues().get(0).getKey());
    assertArrayEquals(
        new byte[]{0x7f}, capture.getAllValues().get(0).getValue());
    assertArrayEquals(
        new byte[]{0x01}, capture.getAllValues().get(1).getKey());
    assertArrayEquals(
        new byte[]{0x7e}, capture.getAllValues().get(1).getValue());
    assertArrayEquals(
        new byte[]{0x02}, capture.getAllValues().get(2).getKey());
    assertArrayEquals(
        new byte[]{0x7d}, capture.getAllValues().get(2).getValue());
  }

  @Test
  public void testHasNextDependsOnIsvalid() {
    when(rocksDBIteratorMock.isValid()).thenReturn(true, true, false);

    RDBStoreByteArrayIterator iter = newIterator();

    assertTrue(iter.hasNext());
    assertFalse(iter.hasNext());
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
  public void testSeekReturnsTheActualKey() throws Exception {
    when(rocksDBIteratorMock.isValid()).thenReturn(true);
    when(rocksDBIteratorMock.key()).thenReturn(new byte[]{0x00});
    when(rocksDBIteratorMock.value()).thenReturn(new byte[]{0x7f});

    RDBStoreByteArrayIterator iter = newIterator();
    final Table.KeyValue<byte[], byte[]> val = iter.seek(new byte[]{0x55});

    InOrder verifier = inOrder(rocksDBIteratorMock);

    verify(rocksDBIteratorMock, times(1)).seekToFirst(); //at construct time
    verify(rocksDBIteratorMock, never()).seekToLast();
    verifier.verify(rocksDBIteratorMock, times(1)).seek(any(byte[].class));
    verifier.verify(rocksDBIteratorMock, times(1)).isValid();
    verifier.verify(rocksDBIteratorMock, times(1)).key();
    verifier.verify(rocksDBIteratorMock, times(1)).value();
    assertArrayEquals(new byte[]{0x00}, val.getKey());
    assertArrayEquals(new byte[]{0x7f}, val.getValue());
  }

  @Test
  public void testGettingTheKeyIfIteratorIsValid() throws Exception {
    when(rocksDBIteratorMock.isValid()).thenReturn(true);
    when(rocksDBIteratorMock.key()).thenReturn(new byte[]{0x00});

    RDBStoreByteArrayIterator iter = newIterator();
    byte[] key = null;
    if (iter.hasNext()) {
      final Table.KeyValue<byte[], byte[]> entry = iter.next();
      key = entry.getKey();
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

    RDBStoreByteArrayIterator iter = newIterator();
    Table.KeyValue<byte[], byte[]> entry;
    byte[] key = null;
    byte[] value = null;
    if (iter.hasNext()) {
      entry = iter.next();
      key = entry.getKey();
      value = entry.getValue();
    }

    InOrder verifier = inOrder(rocksDBIteratorMock);

    verifier.verify(rocksDBIteratorMock, times(1)).isValid();
    verifier.verify(rocksDBIteratorMock, times(1)).key();
    assertArrayEquals(new byte[]{0x00}, key);
    assertArrayEquals(new byte[]{0x7f}, value);
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

    iter.seekToFirst();
    verify(rocksDBIteratorMock, times(1)).seekToFirst();
    clearInvocations(rocksDBIteratorMock);

    when(rocksDBIteratorMock.isValid()).thenReturn(true);
    assertTrue(iter.hasNext());
    verify(rocksDBIteratorMock, times(1)).isValid();
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

    iter.seekToFirst();
    verify(rocksDBIteratorMock, times(1)).seek(testPrefix);
    clearInvocations(rocksDBIteratorMock);

    when(rocksDBIteratorMock.isValid()).thenReturn(true);
    when(rocksDBIteratorMock.key()).thenReturn(testPrefix);
    assertTrue(iter.hasNext());
    verify(rocksDBIteratorMock, times(1)).isValid();
    verify(rocksDBIteratorMock, times(1)).key();
    Exception e =
        assertThrows(Exception.class, () -> iter.seekToLast(), "Prefixed iterator does not support seekToLast");
    assertInstanceOf(UnsupportedOperationException.class, e);

    iter.close();
  }

  @Test
  public void testIteratorType() {
    assertFalse(NEITHER.readKey());
    assertFalse(NEITHER.readValue());

    assertTrue(KEY_ONLY.readKey());
    assertFalse(KEY_ONLY.readValue());

    assertFalse(VALUE_ONLY.readKey());
    assertTrue(VALUE_ONLY.readValue());

    assertTrue(KEY_AND_VALUE.readKey());
    assertTrue(KEY_AND_VALUE.readValue());
  }
}
