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

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Class to test OmMultipartKeyInfo.
 */
public class TestOmMultipartKeyInfo {

  @Test
  public void testCopyObject() {
    for (ReplicationConfig param : replicationConfigs().collect(toList())) {
      testCopyObject(param);
    }
  }

  //@ParameterizedTest
  //@MethodSource("replicationConfigs")
  private void testCopyObject(ReplicationConfig replicationConfig) {
    // GIVEN
    OmMultipartKeyInfo subject = createSubject()
        .setReplicationConfig(replicationConfig)
        .build();

    // WHEN
    OmMultipartKeyInfo copy = subject.copyObject();

    // THEN
    assertNotSame(subject, copy);
    assertEquals(subject, copy);
    assertEquals(replicationConfig, copy.getReplicationConfig());
  }

  @Test
  public void protoConversion() {
    for (ReplicationConfig param : replicationConfigs().collect(toList())) {
      protoConversion(param);
    }
  }

  //@ParameterizedTest
  //@MethodSource("replicationConfigs")
  private void protoConversion(ReplicationConfig replicationConfig) {
    // GIVEN
    OmMultipartKeyInfo subject = createSubject()
        .setReplicationConfig(replicationConfig)
        .build();

    // WHEN
    OzoneManagerProtocolProtos.MultipartKeyInfo proto = subject.getProto();
    OmMultipartKeyInfo fromProto = OmMultipartKeyInfo.getFromProto(proto);

    // THEN
    assertEquals(subject, fromProto);
    assertEquals(replicationConfig, fromProto.getReplicationConfig());
  }

  private static Stream<ReplicationConfig> replicationConfigs() {
    return Stream.of(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE),
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        new ECReplicationConfig(3, 2)
    );
  }

  @Test
  public void distinctListOfParts() {
    // GIVEN
    OmMultipartKeyInfo subject = createSubject().build();
    OmMultipartKeyInfo copy = subject.copyObject();

    // WHEN
    subject.addPartKeyInfo(createPart(createKeyInfo()).build());

    // THEN
    assertEquals(0, copy.getPartKeyInfoMap().size());
    assertEquals(1, subject.getPartKeyInfoMap().size());
  }

  private static OmMultipartKeyInfo.Builder createSubject() {
    return new OmMultipartKeyInfo.Builder()
        .setUploadID(UUID.randomUUID().toString())
        .setCreationTime(Time.now());
  }

  private static PartKeyInfo.Builder createPart(KeyInfo.Builder partKeyInfo) {
    return PartKeyInfo.newBuilder()
        .setPartNumber(1)
        .setPartName("/path")
        .setPartKeyInfo(partKeyInfo);
  }

  private static KeyInfo.Builder createKeyInfo() {
    return KeyInfo.newBuilder()
        .setVolumeName(UUID.randomUUID().toString())
        .setBucketName(UUID.randomUUID().toString())
        .setKeyName(UUID.randomUUID().toString())
        .setDataSize(100L)
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setType(HddsProtos.ReplicationType.STAND_ALONE);
  }
}
