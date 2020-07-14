package org.apache.hadoop.hdds.utils.db;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.rocksdb.RocksIterator;

import java.util.NoSuchElementException;
import java.util.function.Consumer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollectionOf;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestRDBStoreIterator {

  private RocksIterator rocksDBIteratorMock;
  private RDBTable rocksTableMock;

  @Before
  public void setup() {
    rocksDBIteratorMock = mock(RocksIterator.class);
    rocksTableMock = mock(RDBTable.class);
  }

  @Test
  public void testForeachRemainingCallsConsumerWithAllElements() {
    when(rocksDBIteratorMock.isValid())
        .thenReturn(true, true, true, true, true, true, false);
    when(rocksDBIteratorMock.key())
        .thenReturn(new byte[]{0x00}, new byte[]{0x01}, new byte[]{0x02})
        .thenThrow(new NoSuchElementException());
    when(rocksDBIteratorMock.value())
        .thenReturn(new byte[]{0x7f}, new byte[]{0x7e}, new byte[]{0x7d})
        .thenThrow(new NoSuchElementException());


    Consumer<ByteArrayKeyValue> consumerStub = mock(Consumer.class);

    RDBStoreIterator iter = new RDBStoreIterator(rocksDBIteratorMock);
    iter.forEachRemaining(consumerStub);

    ArgumentCaptor<ByteArrayKeyValue> capture =
        ArgumentCaptor.forClass(ByteArrayKeyValue.class);
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
  public void testHasNextDependsOnIsvalid(){
    when(rocksDBIteratorMock.isValid()).thenReturn(true, false);

    RDBStoreIterator iter = new RDBStoreIterator(rocksDBIteratorMock);

    assertTrue(iter.hasNext());
    assertFalse(iter.hasNext());
  }

  @Test
  public void testNextCallsIsValidThenGetsTheValueAndStepsToNext() {
    when(rocksDBIteratorMock.isValid()).thenReturn(true);
    RDBStoreIterator iter = new RDBStoreIterator(rocksDBIteratorMock);

    InOrder verifier = inOrder(rocksDBIteratorMock);

    iter.next();

    verifier.verify(rocksDBIteratorMock).isValid();
    verifier.verify(rocksDBIteratorMock).key();
    verifier.verify(rocksDBIteratorMock).value();
    verifier.verify(rocksDBIteratorMock).next();
  }

  @Test
  public void testConstructorSeeksToFirstElement() {
    new RDBStoreIterator(rocksDBIteratorMock);

    verify(rocksDBIteratorMock, times(1)).seekToFirst();
  }

  @Test
  public void testSeekToFirstSeeks() {
    RDBStoreIterator iter = new RDBStoreIterator(rocksDBIteratorMock);

    iter.seekToFirst();

    verify(rocksDBIteratorMock, times(2)).seekToFirst();
  }

  @Test
  public void testSeekToLastSeeks() {
    RDBStoreIterator iter = new RDBStoreIterator(rocksDBIteratorMock);

    iter.seekToLast();

    verify(rocksDBIteratorMock, times(1)).seekToLast();
  }

  @Test
  public void testSeekReturnsTheActualKey() {
    when(rocksDBIteratorMock.isValid()).thenReturn(true);
    when(rocksDBIteratorMock.key()).thenReturn(new byte[]{0x00});
    when(rocksDBIteratorMock.value()).thenReturn(new byte[]{0x7f});

    RDBStoreIterator iter = new RDBStoreIterator(rocksDBIteratorMock);
    ByteArrayKeyValue val = iter.seek(new byte[]{0x55});

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
  public void testGettingTheKeyIfIteratorIsValid() {
    when(rocksDBIteratorMock.isValid()).thenReturn(true);
    when(rocksDBIteratorMock.key()).thenReturn(new byte[]{0x00});

    RDBStoreIterator iter = new RDBStoreIterator(rocksDBIteratorMock);
    byte[] key = iter.key();

    InOrder verifier = inOrder(rocksDBIteratorMock);

    verifier.verify(rocksDBIteratorMock, times(1)).isValid();
    verifier.verify(rocksDBIteratorMock, times(1)).key();
    assertArrayEquals(new byte[]{0x00}, key);
  }

  @Test
  public void testGettingTheValueIfIteratorIsValid() {
    when(rocksDBIteratorMock.isValid()).thenReturn(true);
    when(rocksDBIteratorMock.key()).thenReturn(new byte[]{0x00});
    when(rocksDBIteratorMock.value()).thenReturn(new byte[]{0x7f});

    RDBStoreIterator iter = new RDBStoreIterator(rocksDBIteratorMock);
    ByteArrayKeyValue val = iter.value();

    InOrder verifier = inOrder(rocksDBIteratorMock);

    verifier.verify(rocksDBIteratorMock, times(1)).isValid();
    verifier.verify(rocksDBIteratorMock, times(1)).key();
    assertArrayEquals(new byte[]{0x00}, val.getKey());
    assertArrayEquals(new byte[]{0x7f}, val.getValue());
  }

  @Test
  public void testRemovingFromDBActuallyDeletesFromTable() throws Exception {
    byte[] testKey = new byte[]{0x00};
    when(rocksDBIteratorMock.isValid()).thenReturn(true);
    when(rocksDBIteratorMock.key()).thenReturn(testKey);

    RDBStoreIterator iter =
        new RDBStoreIterator(rocksDBIteratorMock, rocksTableMock);
    iter.removeFromDB();

    InOrder verifier = inOrder(rocksDBIteratorMock, rocksTableMock);

    verifier.verify(rocksDBIteratorMock, times(1)).isValid();
    verifier.verify(rocksTableMock, times(1)).delete(testKey);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testRemoveFromDBWithoutDBTableSet() throws Exception {
    RDBStoreIterator iter = new RDBStoreIterator(rocksDBIteratorMock);
    iter.removeFromDB();
  }

  @Test
  public void testCloseCloses() throws Exception {
    RDBStoreIterator iter = new RDBStoreIterator(rocksDBIteratorMock);
    iter.close();

    verify(rocksDBIteratorMock, times(1)).close();
  }
}
