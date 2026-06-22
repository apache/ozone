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

package org.apache.hadoop.ozone.protocol.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicateContainerCommandProto;
import org.junit.jupiter.api.Test;

/**
 * Test cases to verify {@link ReplicateContainerCommand} serialization.
 */
public class TestReplicateContainerCommand {

  @Test
  public void testPeerApparentVersionRoundTrip() {
    DatanodeDetails source = MockDatanodeDetails.randomDatanodeDetails();
    List<DatanodeDetails> sources = Collections.singletonList(source);

    ReplicateContainerCommand cmd =
        ReplicateContainerCommand.fromSources(1L, sources);
    cmd.setPeerApparentVersion(
        HDDSVersion.STREAM_BLOCK_SUPPORT.serialize());

    ReplicateContainerCommandProto proto = cmd.getProto();
    ReplicateContainerCommand deserialized =
        ReplicateContainerCommand.getFromProtobuf(proto);

    assertEquals(HDDSVersion.STREAM_BLOCK_SUPPORT.serialize(),
        deserialized.getPeerApparentVersion());
  }

  @Test
  public void testPeerApparentVersionDefaultWhenAbsent() {
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();

    ReplicateContainerCommand cmd =
        ReplicateContainerCommand.toTarget(1L, target);

    ReplicateContainerCommandProto proto =
        ReplicateContainerCommandProto.newBuilder()
            .setContainerID(1L)
            .setCmdId(cmd.getId())
            .setTarget(target.getProtoBufMessage())
            .build();

    ReplicateContainerCommand deserialized =
        ReplicateContainerCommand.getFromProtobuf(proto);

    assertEquals(HDDSVersion.DEFAULT_VERSION.serialize(),
        deserialized.getPeerApparentVersion());
  }

  @Test
  public void testPeerApparentVersionInPushMode() {
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();

    ReplicateContainerCommand cmd =
        ReplicateContainerCommand.toTarget(1L, target);
    cmd.setPeerApparentVersion(HDDSVersion.ZDU.serialize());

    ReplicateContainerCommandProto proto = cmd.getProto();
    ReplicateContainerCommand deserialized =
        ReplicateContainerCommand.getFromProtobuf(proto);

    assertEquals(HDDSVersion.ZDU.serialize(),
        deserialized.getPeerApparentVersion());
    assertEquals(target.getUuid(),
        deserialized.getTargetDatanode().getUuid());
  }

  @Test
  public void testToStringIncludesPeerApparentVersion() {
    ReplicateContainerCommand cmd =
        ReplicateContainerCommand.forTest(1L);
    cmd.setPeerApparentVersion(
        HDDSVersion.STREAM_BLOCK_SUPPORT.serialize());

    String str = cmd.toString();
    assertTrue(str.contains("peerApparentVersion="
        + HDDSVersion.STREAM_BLOCK_SUPPORT.serialize()));
  }
}
