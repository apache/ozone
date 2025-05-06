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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.primitives.Longs;
import org.junit.jupiter.api.Test;

/**
 * Test for class {@link FixedLengthStringCodec}.
 */
public class TestFixedLengthStringCodec {

  @Test
  public void testStringEncodeAndDecode() {
    long[] testContainerIDs = {
        0L, 1L, 2L, 12345L,
        Long.MAX_VALUE / 2, Long.MAX_VALUE - 1, Long.MAX_VALUE
    };

    for (long containerID : testContainerIDs) {
      String containerPrefix = FixedLengthStringCodec.bytes2String(
          Longs.toByteArray(containerID));
      long decodedContainerID = Longs.fromByteArray(
          FixedLengthStringCodec.string2Bytes(containerPrefix));
      assertEquals(containerID, decodedContainerID);
    }
  }
}
