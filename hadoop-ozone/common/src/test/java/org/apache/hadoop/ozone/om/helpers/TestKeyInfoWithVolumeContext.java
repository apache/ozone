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
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetKeyInfoResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link KeyInfoWithVolumeContext}. */
public class TestKeyInfoWithVolumeContext {

  @Test
  public void fromProtobufReadsStsOriginalAccessKeyId() throws Exception {
    final GetKeyInfoResponse proto = GetKeyInfoResponse.newBuilder()
        .setKeyInfo(minimalKeyInfo())
        .setUserPrincipal("alice")
        .setStsOriginalAccessKeyId("AKIAORIGINAL123")
        .build();

    final KeyInfoWithVolumeContext decoded = KeyInfoWithVolumeContext.fromProtobuf(proto);

    assertEquals("alice", decoded.getUserPrincipal().orElse(null));
    assertEquals("AKIAORIGINAL123", decoded.getStsOriginalAccessKeyId().orElse(null));
    assertEquals("key", decoded.getKeyInfo().getKeyName());
  }

  @Test
  public void omitsStsOriginalAccessKeyIdWhenUnset() throws Exception {
    final GetKeyInfoResponse proto = GetKeyInfoResponse.newBuilder()
        .setKeyInfo(minimalKeyInfo())
        .setUserPrincipal("alice")
        .build();

    final KeyInfoWithVolumeContext decoded = KeyInfoWithVolumeContext.fromProtobuf(proto);

    assertEquals("alice", decoded.getUserPrincipal().orElse(null));
    assertFalse(decoded.getStsOriginalAccessKeyId().isPresent());
  }

  private static KeyInfo minimalKeyInfo() {
    return KeyInfo.newBuilder()
        .setVolumeName("s3v")
        .setBucketName("bucket")
        .setKeyName("key")
        .setDataSize(0L)
        .setCreationTime(0L)
        .setModificationTime(0L)
        .setType(HddsProtos.ReplicationType.STAND_ALONE)
        .build();
  }
}
