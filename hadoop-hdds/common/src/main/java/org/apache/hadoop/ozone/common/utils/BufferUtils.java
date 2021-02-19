/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.common.utils;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

public final class BufferUtils {

  /** Utility classes should not be constructed. **/
  private BufferUtils() {

  }

  /**
   * Assign an array of ByteBuffers.
   * @param totalLen total length of all ByteBuffers
   * @param bufferCapacity max capacity of each ByteBuffer
   */
  public static ByteBuffer[] assignByteBuffers(long totalLen,
      long bufferCapacity) {
    Preconditions.checkArgument(totalLen > 0, "Buffer Length should be a " +
        "positive integer.");
    Preconditions.checkArgument(bufferCapacity > 0, "Buffer Capacity should " +
        "be a positive integer.");

    int numBuffers =
        (int) Math.ceil((double) totalLen / (double) bufferCapacity);

    ByteBuffer[] dataBuffers = new ByteBuffer[numBuffers];
    int buffersAllocated = 0;
    // For each ByteBuffer (except the last) allocate bufferLen of capacity
    for (int i = 0; i < numBuffers - 1; i++) {
      dataBuffers[i] = ByteBuffer.allocate((int) bufferCapacity);
      buffersAllocated += bufferCapacity;
    }
    // For the last ByteBuffer, allocate as much space as is needed to fit
    // remaining bytes
    dataBuffers[numBuffers - 1] = ByteBuffer.allocate(
        (int) (totalLen - buffersAllocated));
    return dataBuffers;
  }

  /**
   * Return a read only ByteBuffer list for the input ByteStrings list.
   */
  public static List<ByteBuffer> getReadOnlyByteBuffers(
      List<ByteString> byteStrings) {
    List<ByteBuffer> buffers = new ArrayList<>();
    for (ByteString byteString : byteStrings) {
      buffers.add(byteString.asReadOnlyByteBuffer());
    }
    return buffers;
  }

  /**
   * Return the summation of the length of all ByteStrings.
   */
  public static long getBuffersLen(List<ByteString> buffers) {
    long length = 0;
    for (ByteString buffer : buffers) {
      length += buffer.size();
    }
    return length;
  }
}
