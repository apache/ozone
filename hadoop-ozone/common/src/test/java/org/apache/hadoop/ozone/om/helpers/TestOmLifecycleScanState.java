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

package org.apache.hadoop.ozone.om.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleScanState;
import org.junit.jupiter.api.Test;

/**
 * Tests for OmLifecycleScanState.
 */
public class TestOmLifecycleScanState {

  @Test
  public void testBuilderAndProtobufConversion() {
    OmLifecycleScanState state = new OmLifecycleScanState.Builder()
        .setBucketKey("/vol1/bucket1")
        .setScanStartTime(123456789L)
        .setScanEndTime(123456799L)
        .setLastScannedKey("key1")
        .setLastScannedDir("subDir1")
        .build();

    assertEquals("/vol1/bucket1", state.getBucketKey());
    assertEquals(123456789L, state.getScanStartTime());
    assertEquals(123456799L, state.getScanEndTime());
    assertEquals("key1", state.getLastScannedKey());
    assertEquals("subDir1", state.getLastScannedDir());

    LifecycleScanState proto = state.getProtobuf();
    OmLifecycleScanState decodedState = OmLifecycleScanState.getFromProtobuf(proto);

    assertEquals(state.getBucketKey(), decodedState.getBucketKey());
    assertEquals(state.getScanStartTime(), decodedState.getScanStartTime());
    assertEquals(state.getScanEndTime(), decodedState.getScanEndTime());
    assertEquals(state.getLastScannedKey(), decodedState.getLastScannedKey());
    assertEquals(state.getLastScannedDir(), decodedState.getLastScannedDir());
  }

  @Test
  public void testBuilderAndProtobufConversionWithoutOptionals() {
    OmLifecycleScanState state = new OmLifecycleScanState.Builder()
        .setBucketKey("/vol1/bucket1")
        .setScanStartTime(123456789L)
        .build();

    assertEquals("/vol1/bucket1", state.getBucketKey());
    assertEquals(123456789L, state.getScanStartTime());
    assertNull(state.getScanEndTime());
    assertNull(state.getLastScannedKey());
    assertNull(state.getLastScannedDir());

    LifecycleScanState proto = state.getProtobuf();
    OmLifecycleScanState decodedState = OmLifecycleScanState.getFromProtobuf(proto);

    assertEquals(state.getBucketKey(), decodedState.getBucketKey());
    assertEquals(state.getScanStartTime(), decodedState.getScanStartTime());
    assertNull(decodedState.getScanEndTime());
    assertNull(decodedState.getLastScannedKey());
    assertNull(decodedState.getLastScannedDir());
  }
}
