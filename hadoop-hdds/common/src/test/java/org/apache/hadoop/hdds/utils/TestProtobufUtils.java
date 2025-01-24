/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.utils;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.util.ProtobufUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.apache.hadoop.ozone.util.ProtobufUtils.fromProtobuf;
import static org.apache.hadoop.ozone.util.ProtobufUtils.toProtobuf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test-cases for {@link ProtobufUtils}.
 */
public class TestProtobufUtils {
  @Test
  public void testUuidToProtobuf() {
    UUID object = UUID.randomUUID();
    HddsProtos.UUID protobuf = toProtobuf(object);
    assertEquals(object.getLeastSignificantBits(), protobuf.getLeastSigBits());
    assertEquals(object.getMostSignificantBits(), protobuf.getMostSigBits());
  }

  @Test
  public void testUuidConversion() {
    UUID original = UUID.randomUUID();
    HddsProtos.UUID protobuf = toProtobuf(original);
    UUID deserialized = fromProtobuf(protobuf);
    assertEquals(original, deserialized);
  }

  @Test
  public void testContainerCommandRequestProtoConversion() throws InvalidProtocolBufferException {
    long containerID = 1L;
    long localBlockID = 2L;
    long bcsid = 3L;
    String datanodeID = UUID.randomUUID().toString();
    ContainerProtos.DatanodeBlockID.Builder blkIDBuilder =
        ContainerProtos.DatanodeBlockID.newBuilder().setContainerID(containerID)
            .setLocalID(localBlockID)
            .setBlockCommitSequenceId(bcsid);
    ContainerProtos.GetBlockRequestProto.Builder readBlockRequest =
        ContainerProtos.GetBlockRequestProto.newBuilder().setBlockID(blkIDBuilder.build());

    ContainerProtos.ContainerCommandRequestProto.Builder builder =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.GetBlock)
            .setContainerID(containerID)
            .setDatanodeUuid(datanodeID)
            .setGetBlock(readBlockRequest.build());

    ContainerProtos.ContainerCommandRequestProto request = builder.build();
    byte[] requestInBytes = request.toByteArray();

    request = ContainerProtos.ContainerCommandRequestProto.parseFrom(requestInBytes);
    assertTrue(request.hasGetBlock());
    assertEquals(ContainerProtos.Type.GetBlock, request.getCmdType());
    assertEquals(containerID, request.getContainerID());
    assertEquals(datanodeID, request.getDatanodeUuid());
    assertEquals(localBlockID, request.getGetBlock().getBlockID().getLocalID());
    assertEquals(containerID, request.getGetBlock().getBlockID().getContainerID());
    assertEquals(bcsid, request.getGetBlock().getBlockID().getBlockCommitSequenceId());
  }
}
