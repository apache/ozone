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

package org.apache.hadoop.hdds.utils.db.managed;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.rocksdb.DirectSlice;

/**
 * Tests for ManagedDirectSlice.
 */
public class TestManagedDirectSlice {

  static {
    ManagedRocksObjectUtils.loadRocksDBLibrary();
  }

  @ParameterizedTest
  @CsvSource({"0, 1024", "1024, 1024", "512, 1024", "0, 100", "10, 512", "0, 0"})
  public void testManagedDirectSliceWithOffsetMovedAheadByteBuffer(int offset, int numberOfBytesWritten)
      throws RocksDatabaseException {
    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024);
    byte[] randomBytes = RandomUtils.secure().nextBytes(numberOfBytesWritten);
    byteBuffer.put(randomBytes);
    byteBuffer.flip();
    try (ManagedDirectSlice directSlice = new ManagedDirectSlice(byteBuffer);
         ManagedSlice slice = new ManagedSlice(Arrays.copyOfRange(randomBytes, offset, numberOfBytesWritten))) {
      byteBuffer.position(offset);
      directSlice.putFromBuffer((ds) -> {
        DirectSlice directSliceFromByteBuffer = directSlice.getDirectSlice();
        assertEquals(numberOfBytesWritten - offset, ds.size());
        assertEquals(0, directSliceFromByteBuffer.compare(slice));
        assertEquals(0, slice.compare(directSliceFromByteBuffer));
      });
      Assertions.assertEquals(numberOfBytesWritten, byteBuffer.position());
    }
  }

  @ParameterizedTest
  @CsvSource({"0, 1024, 512", "1024, 1024, 5", "512, 1024, 600", "0, 100, 80", "10, 512, 80", "0, 0, 10",
      "100, 256, -1"})
  public void testManagedDirectSliceWithOpPutToByteBuffer(int offset, int maxNumberOfBytesWrite,
      int numberOfBytesToWrite) throws RocksDatabaseException {
    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024);
    byte[] randomBytes = RandomUtils.secure().nextBytes(offset);
    byteBuffer.put(randomBytes);
    try (ManagedDirectSlice directSlice = new ManagedDirectSlice(byteBuffer)) {
      byteBuffer.position(offset);
      byteBuffer.limit(Math.min(offset + maxNumberOfBytesWrite, 1024));
      assertEquals(numberOfBytesToWrite, directSlice.getToBuffer((ds) -> {
        assertEquals(byteBuffer.remaining(), ds.size());
        return numberOfBytesToWrite;
      }));
      Assertions.assertEquals(offset, byteBuffer.position());
      if (numberOfBytesToWrite == -1) {
        assertEquals(offset + maxNumberOfBytesWrite, byteBuffer.limit());
      } else {
        Assertions.assertEquals(Math.min(Math.min(offset + numberOfBytesToWrite, 1024), maxNumberOfBytesWrite),
            byteBuffer.limit());
      }

    }
  }
}
