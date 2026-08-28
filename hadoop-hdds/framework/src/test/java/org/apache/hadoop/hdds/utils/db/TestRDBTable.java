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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RDBTable}.
 */
public class TestRDBTable {

  @Test
  public void testGetIfExistByteBufferFallbackUsesFreshKeyBuffer()
      throws Exception {
    RocksDatabase db = mock(RocksDatabase.class);
    ColumnFamily columnFamily = mock(ColumnFamily.class);
    RDBMetrics metrics = mock(RDBMetrics.class);
    RDBTable table = new RDBTable(db, columnFamily, metrics);

    byte[] keyBytes = "key-1".getBytes(UTF_8);
    ByteBuffer key = ByteBuffer.wrap(keyBytes);
    ByteBuffer outValue = ByteBuffer.allocate(64);

    // RocksDatabase.keyMayExist duplicates the key internally, so it leaves the
    // caller's key buffer untouched. Return an inconclusive result (value-less
    // "may exist") to force the fallback point-get.
    when(db.keyMayExist(eq(columnFamily), any(ByteBuffer.class),
        any(ByteBuffer.class))).thenReturn((Supplier<Integer>) () -> null);

    // get() advances the key buffer position as native RocksDB does. It must
    // still see the full key, i.e. RDBTable must hand it a fresh duplicate.
    when(db.get(eq(columnFamily), any(ByteBuffer.class), any(ByteBuffer.class)))
        .thenAnswer(invocation -> {
          ByteBuffer keyBuffer = invocation.getArgument(1);
          if (keyBuffer.remaining() != keyBytes.length) {
            return null;
          }
          keyBuffer.position(keyBuffer.limit());
          return 0;
        });

    Integer result = table.getIfExist(key, outValue);
    assertEquals(0, result);
    assertEquals(0, key.position(), "caller key buffer position must be unchanged");
  }

  @Test
  public void testGetIfExistByteBufferFastPathReturnsValue()
      throws Exception {
    RocksDatabase db = mock(RocksDatabase.class);
    ColumnFamily columnFamily = mock(ColumnFamily.class);
    RDBMetrics metrics = mock(RDBMetrics.class);
    RDBTable table = new RDBTable(db, columnFamily, metrics);

    byte[] keyBytes = "key-1".getBytes(UTF_8);
    byte[] valueBytes = "value-1".getBytes(UTF_8);
    ByteBuffer key = ByteBuffer.wrap(keyBytes);
    ByteBuffer outValue = ByteBuffer.allocate(64);

    // Simulate the RocksDB "exists with value" fast path: native code writes
    // the value into the buffer handed to keyMayExist and reports its length.
    // getIfExist passes outValue.duplicate(), so the write must land in the
    // caller's outValue via the shared backing memory.
    when(db.keyMayExist(eq(columnFamily), any(ByteBuffer.class),
        any(ByteBuffer.class))).thenAnswer(invocation -> {
          ByteBuffer valueBuffer = invocation.getArgument(2);
          valueBuffer.put(valueBytes);
          return (Supplier<Integer>) () -> valueBytes.length;
        });

    Integer result = table.getIfExist(key, outValue);
    assertEquals(valueBytes.length, result);
    // The fast path must not fall back to a point-get.
    verify(db, never()).get(eq(columnFamily), any(ByteBuffer.class), any(ByteBuffer.class));
    // Value bytes written through the duplicate are visible in the caller's buffer.
    byte[] readBack = new byte[valueBytes.length];
    outValue.duplicate().get(readBack);
    assertArrayEquals(valueBytes, readBack);
  }
}

