/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.common;

import org.apache.commons.lang3.RandomUtils;

/**
 * Utility class for payload operations.
 */
public final class PayloadUtils {

  private static final int RPC_PAYLOAD_MULTIPLICATION_FACTOR = 1024;
  private static final int MAX_SIZE_KB = 2097151;

  private PayloadUtils() {
  }

  public static byte[] generatePayloadBytes(int payloadSize) {

    byte[] payloadBytes = new byte[0];
    int payloadRespSize =
        Math.min(payloadSize * RPC_PAYLOAD_MULTIPLICATION_FACTOR, MAX_SIZE_KB);
    if (payloadRespSize > 0) {
      payloadBytes = RandomUtils.nextBytes(payloadRespSize);
    }

    return payloadBytes;
  }
}
