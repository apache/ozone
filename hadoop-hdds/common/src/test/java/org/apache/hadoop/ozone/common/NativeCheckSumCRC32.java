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

package org.apache.hadoop.ozone.common;

import java.nio.ByteBuffer;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.util.NativeCRC32Wrapper;

/**
 * This is a partial implementation to be used only in benchmarks.
 *
 * The Hadoop Native checksum libraries do not allow for updating a checksum
 * as the java.util.zip.Checksum dictates in its update(...) method.
 *
 * This class allows the Native Hadoop CRC32 implementations to be called to
 * generate checksums, provided only a single call is made to the update(...)
 * method.
 *
 */
public class NativeCheckSumCRC32 implements java.util.zip.Checksum {

  // 1 for crc32, 2 for crc32c - see NativeCRC32Wrapper
  private int checksumType;
  private int bytesPerSum;

  private ByteBuffer checksum = ByteBuffer.allocate(4);
  private boolean needsReset = false;

  public NativeCheckSumCRC32(int checksumType, int bytesPerSum) {
    this.checksumType = checksumType;
    this.bytesPerSum = bytesPerSum;
  }

  @Override
  public void update(int b) {
    throw new NotImplementedException("Update method is not implemented");
  }

  /**
   * Calculate the checksum. Note the checksum is not updatable. You should
   * make a single call to this method and then call getValue() to retrive the
   * value.
   * @param b A byte array whose contents will be used to calculate a CRC32(C)
   * @param off The offset in the byte array to start reading.
   * @param len The number of bytes in the byte array to read.
   */
  @Override
  public void update(byte[] b, int off, int len) {
    if (needsReset) {
      throw new IllegalArgumentException(
          "This checksum implementation is not updatable");
    }
    NativeCRC32Wrapper.calculateChunkedSumsByteArray(bytesPerSum, checksumType,
        checksum.array(), 0, b, off, len);
    needsReset = true;
  }

  @Override
  public long getValue() {
    checksum.position(0);
    return checksum.getInt();
  }

  @Override
  public void reset() {
    checksum.clear();
    needsReset = false;
  }
}
