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

package org.apache.hadoop.ozone.util;

import org.apache.commons.lang3.RandomUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.ratis.util.Preconditions;

/**
 * Utility class for payload operations.
 */
public final class PayloadUtils {
  private static final int MAX_SIZE = 2097151 * 1024;
  private static final byte[] SEED = RandomUtils.secure().randomBytes(1024);

  private PayloadUtils() {
  }

  /** @return a new byte[] containing */
  public static byte[] generatePayload(int payloadSizeBytes) {
    byte[] result = new byte[Math.min(payloadSizeBytes, MAX_SIZE)];

    // duplicate SEED to create the required payload.
    int curIdx = 0;
    while (curIdx < result.length) {
      int remaining = result.length - curIdx;
      int copySize = Math.min(SEED.length, remaining);
      System.arraycopy(SEED, 0, result, curIdx, copySize);
      curIdx += copySize;
    }

    Preconditions.assertTrue(curIdx == result.length);

    return result;
  }

  public static com.google.protobuf.ByteString generatePayloadProto2(int payloadSizeBytes) {
    byte[] payload = generatePayload(payloadSizeBytes);
    return payload.length > 0 ? com.google.protobuf.UnsafeByteOperations.unsafeWrap(payload)
        : com.google.protobuf.ByteString.EMPTY;
  }

  public static ByteString generatePayloadProto3(int payloadSizeBytes) {
    return UnsafeByteOperations.unsafeWrap(generatePayload(payloadSizeBytes));
  }
}
