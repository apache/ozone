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

package org.apache.hadoop.hdds.scm.container;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.junit.jupiter.api.Test;

/**
 * Test for the ContainerReplicaInfo class.
 */
public class TestContainerReplicaInfo {

  @Test
  public void testObjectCreatedFromProto() {
    HddsProtos.SCMContainerReplicaProto proto =
        HddsProtos.SCMContainerReplicaProto.newBuilder()
            .setKeyCount(10)
            .setBytesUsed(12345)
            .setContainerID(567)
            .setPlaceOfBirth(UUID.randomUUID().toString())
            .setSequenceID(5)
            .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails()
                .getProtoBufMessage())
            .setState("OPEN")
            .build();

    ContainerReplicaInfo info = ContainerReplicaInfo.fromProto(proto);

    assertEquals(proto.getContainerID(), info.getContainerID());
    assertEquals(proto.getBytesUsed(), info.getBytesUsed());
    assertEquals(proto.getKeyCount(), info.getKeyCount());
    assertEquals(proto.getPlaceOfBirth(),
        info.getPlaceOfBirth().toString());
    assertEquals(DatanodeDetails.getFromProtoBuf(
        proto.getDatanodeDetails()), info.getDatanodeDetails());
    assertEquals(proto.getSequenceID(), info.getSequenceId());
    assertEquals(proto.getState(), info.getState());
    // If replicaIndex is not in the proto, then -1 should be returned
    assertEquals(-1, info.getReplicaIndex());
    // Storage fields are absent in the proto, so they must be null rather than
    // the protobuf defaults, otherwise they would show up in JSON output for
    // datanodes that do not report them.
    assertThat(info.getStorageType()).isNull();
    assertThat(info.getVolumeStorageType()).isNull();
  }

  @Test
  public void testObjectCreatedFromProtoWithStorageFields() {
    HddsProtos.SCMContainerReplicaProto proto =
        HddsProtos.SCMContainerReplicaProto.newBuilder()
            .setKeyCount(10)
            .setBytesUsed(12345)
            .setContainerID(567)
            .setPlaceOfBirth(UUID.randomUUID().toString())
            .setSequenceID(5)
            .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails()
                .getProtoBufMessage())
            .setState("CLOSED")
            .setStorageType(HddsProtos.StorageTypeProto.SSD)
            .setVolumeStorageType(HddsProtos.StorageTypeProto.SSD)
            .build();

    ContainerReplicaInfo info = ContainerReplicaInfo.fromProto(proto);

    // The container was stamped SSD and sits on the SSD volume.
    assertThat(info.getStorageType()).isEqualTo(StorageType.SSD);
    assertThat(info.getVolumeStorageType()).isEqualTo(StorageType.SSD);
  }

  @Test
  public void testObjectCreatedFromProtoWithReplicaIndedx() {
    HddsProtos.SCMContainerReplicaProto proto =
        HddsProtos.SCMContainerReplicaProto.newBuilder()
            .setKeyCount(10)
            .setBytesUsed(12345)
            .setContainerID(567)
            .setPlaceOfBirth(UUID.randomUUID().toString())
            .setSequenceID(5)
            .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails()
                .getProtoBufMessage())
            .setState("OPEN")
            .setReplicaIndex(4)
            .build();

    ContainerReplicaInfo info = ContainerReplicaInfo.fromProto(proto);

    assertEquals(proto.getContainerID(), info.getContainerID());
    assertEquals(proto.getBytesUsed(), info.getBytesUsed());
    assertEquals(proto.getKeyCount(), info.getKeyCount());
    assertEquals(proto.getPlaceOfBirth(),
        info.getPlaceOfBirth().toString());
    assertEquals(DatanodeDetails.getFromProtoBuf(
        proto.getDatanodeDetails()), info.getDatanodeDetails());
    assertEquals(proto.getSequenceID(), info.getSequenceId());
    assertEquals(proto.getState(), info.getState());
    assertEquals(4, info.getReplicaIndex());
  }
}
