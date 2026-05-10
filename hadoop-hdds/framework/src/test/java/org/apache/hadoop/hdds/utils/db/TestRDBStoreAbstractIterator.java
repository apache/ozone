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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksIterator;

/**
 * Tests prefix bounds used by RocksDB table iterators.
 */
public class TestRDBStoreAbstractIterator {

  @BeforeEach
  public void setup() {
    ManagedRocksObjectUtils.loadRocksDBLibrary();
  }

  @Test
  public void testNextHigherPrefixForNullAndEmptyPrefix() {
    assertNull(RDBStoreAbstractIterator.getNextHigherPrefix(null));
    assertNull(RDBStoreAbstractIterator.getNextHigherPrefix(new byte[0]));
  }

  @Test
  public void testNextHigherPrefixForNormalPrefix() {
    assertArrayEquals(new byte[] {'a', 'b', 'd'},
        RDBStoreAbstractIterator.getNextHigherPrefix(
            new byte[] {'a', 'b', 'c'}));
  }

  @Test
  public void testNextHigherPrefixForTrailingFF() {
    assertArrayEquals(new byte[] {0x02},
        RDBStoreAbstractIterator.getNextHigherPrefix(
            new byte[] {0x01, (byte) 0xFF}));
  }

  @Test
  public void testNextHigherPrefixForAllFF() {
    assertNull(RDBStoreAbstractIterator.getNextHigherPrefix(
        new byte[] {(byte) 0xFF, (byte) 0xFF}));
  }

  @Test
  public void testReadOptionsForNullAndEmptyPrefix() {
    try (ManagedReadOptions nullPrefix =
             RDBStoreAbstractIterator.newReadOptions(null, false);
         ManagedReadOptions emptyPrefix =
             RDBStoreAbstractIterator.newReadOptions(new byte[0], false)) {
      assertNull(nullPrefix.getLowerBound());
      assertNull(nullPrefix.getUpperBound());
      assertNull(emptyPrefix.getLowerBound());
      assertNull(emptyPrefix.getUpperBound());
    }
  }

  @Test
  public void testReadOptionsForNormalPrefix() {
    final byte[] prefix = new byte[] {'a', 'b', 'c'};
    try (ManagedReadOptions readOptions =
             RDBStoreAbstractIterator.newReadOptions(prefix, false)) {
      assertArrayEquals(prefix, readOptions.getLowerBound());
      assertArrayEquals(new byte[] {'a', 'b', 'd'},
          readOptions.getUpperBound());
    }
  }

  @Test
  public void testReadOptionsForAllFFPrefix() {
    final byte[] prefix = new byte[] {(byte) 0xFF, (byte) 0xFF};
    try (ManagedReadOptions readOptions =
             RDBStoreAbstractIterator.newReadOptions(prefix, false)) {
      assertArrayEquals(prefix, readOptions.getLowerBound());
      assertNull(readOptions.getUpperBound());
    }
  }

  @Test
  public void testIteratorClosesOwnedReadOptions() {
    final RocksIterator rocksIterator = mock(RocksIterator.class);
    final CountingReadOptions readOptions = new CountingReadOptions();
    final RDBStoreByteArrayIterator iterator = new RDBStoreByteArrayIterator(
        new ManagedRocksIterator(rocksIterator), null, null, KEY_AND_VALUE,
        readOptions);

    iterator.close();

    verify(rocksIterator, times(1)).close();
    assertEquals(1, readOptions.getCloseCount());

    iterator.close();
    assertEquals(1, readOptions.getCloseCount());
  }

  private static final class CountingReadOptions extends ManagedReadOptions {
    private int closeCount;

    @Override
    public void close() {
      closeCount++;
      super.close();
    }

    int getCloseCount() {
      return closeCount;
    }
  }
}
