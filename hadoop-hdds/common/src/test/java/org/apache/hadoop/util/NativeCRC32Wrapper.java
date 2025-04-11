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

package org.apache.hadoop.util;

import java.nio.ByteBuffer;
import org.apache.hadoop.fs.ChecksumException;

/**
 * This class wraps the NativeCRC32 class in hadoop-common, because the class
 * is package private there. The intention of making this class available
 * in Ozone is to allow the native libraries to be benchmarked alongside other
 * implementations. At the current time, the hadoop native CRC is not used
 * anywhere in Ozone except for benchmarks.
 */
public final class NativeCRC32Wrapper {

  public static final int CHECKSUM_CRC32 = NativeCrc32.CHECKSUM_CRC32;
  public static final int CHECKSUM_CRC32C = NativeCrc32.CHECKSUM_CRC32C;

  // Private constructor
  private NativeCRC32Wrapper() {
  }

  public static boolean isAvailable() {
    return NativeCrc32.isAvailable();
  }

  public static void verifyChunkedSums(int bytesPerSum, int checksumType,
      ByteBuffer sums, ByteBuffer data, String fileName, long basePos)
      throws ChecksumException {
    NativeCrc32.verifyChunkedSums(bytesPerSum, checksumType, sums, data,
        fileName, basePos);
  }

  @SuppressWarnings("checkstyle:parameternumber")
  public static void verifyChunkedSumsByteArray(int bytesPerSum,
      int checksumType, byte[] sums, int sumsOffset, byte[] data,
      int dataOffset, int dataLength, String fileName, long basePos)
      throws ChecksumException {
    NativeCrc32.verifyChunkedSumsByteArray(bytesPerSum, checksumType, sums,
        sumsOffset, data, dataOffset, dataLength, fileName, basePos);
  }

  public static void calculateChunkedSums(int bytesPerSum, int checksumType,
                                          ByteBuffer sums, ByteBuffer data) {
    NativeCrc32.calculateChunkedSums(bytesPerSum, checksumType, sums, data);
  }

  public static void calculateChunkedSumsByteArray(int bytesPerSum,
      int checksumType, byte[] sums, int sumsOffset, byte[] data,
      int dataOffset, int dataLength) {
    NativeCrc32.calculateChunkedSumsByteArray(bytesPerSum, checksumType, sums,
        sumsOffset, data, dataOffset, dataLength);
  }

}
