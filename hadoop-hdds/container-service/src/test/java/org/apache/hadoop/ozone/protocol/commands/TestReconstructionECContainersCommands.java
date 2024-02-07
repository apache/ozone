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
package org.apache.hadoop.ozone.protocol.commands;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test ECReconstructionContainersCommand.
 */
public class TestReconstructionECContainersCommands {

  @Test
  public void testExceptionIfSourceAndMissingNotSameLength() {
    ECReplicationConfig ecReplicationConfig = new ECReplicationConfig(3, 2);
    byte[] missingContainerIndexes = {1, 2};

    List<DatanodeDetails> targetDns = new ArrayList<>();
    targetDns.add(MockDatanodeDetails.randomDatanodeDetails());

    assertThrows(IllegalArgumentException.class,
        () -> new ReconstructECContainersCommand(1L, Collections.emptyList(),
        targetDns, missingContainerIndexes, ecReplicationConfig));
  }

  @Test
  public void protobufConversion() {
    byte[] missingContainerIndexes = {1, 2};
    List<Long> srcNodesIndexes = new ArrayList<>();
    for (int i = 0; i < srcNodesIndexes.size(); i++) {
      srcNodesIndexes.add(i + 1L);
    }
    ECReplicationConfig ecReplicationConfig = new ECReplicationConfig(3, 2);
    final List<DatanodeDetails> dnDetails = getDNDetails(5);

    List<ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex>
        sources = dnDetails.stream().map(
          a -> new ReconstructECContainersCommand
              .DatanodeDetailsAndReplicaIndex(a, dnDetails.indexOf(a)))
        .collect(Collectors.toList());
    List<DatanodeDetails> targets = getDNDetails(2);
    ReconstructECContainersCommand reconstructECContainersCommand =
        new ReconstructECContainersCommand(1L, sources, targets,
            missingContainerIndexes, ecReplicationConfig);
    StorageContainerDatanodeProtocolProtos.ReconstructECContainersCommandProto
        proto = reconstructECContainersCommand.getProto();

    List<ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex>
        srcDnsFromProto = proto.getSourcesList().stream().map(
          a -> ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex
            .fromProto(a)).collect(Collectors.toList());
    List<DatanodeDetails> targetDnsFromProto = proto.getTargetsList().stream()
        .map(a -> DatanodeDetails.getFromProtoBuf(a))
        .collect(Collectors.toList());
    assertEquals(1L, proto.getContainerID());
    assertEquals(sources, srcDnsFromProto);
    assertEquals(targets, targetDnsFromProto);
    assertArrayEquals(missingContainerIndexes,
        proto.getMissingContainerIndexes().toByteArray());
    assertEquals(ecReplicationConfig,
        new ECReplicationConfig(proto.getEcReplicationConfig()));

    ReconstructECContainersCommand fromProtobuf =
        ReconstructECContainersCommand.getFromProtobuf(proto);

    assertEquals(reconstructECContainersCommand.getContainerID(),
        fromProtobuf.getContainerID());
    assertEquals(reconstructECContainersCommand.getSources(),
        fromProtobuf.getSources());
    assertEquals(reconstructECContainersCommand.getTargetDatanodes(),
        fromProtobuf.getTargetDatanodes());
    assertArrayEquals(
        reconstructECContainersCommand.getMissingContainerIndexes(),
        fromProtobuf.getMissingContainerIndexes());
    assertEquals(
        reconstructECContainersCommand.getEcReplicationConfig(),
        fromProtobuf.getEcReplicationConfig());
  }

  private List<DatanodeDetails> getDNDetails(int numDns) {
    List<DatanodeDetails> dns = new ArrayList<>();
    for (int i = 0; i < numDns; i++) {
      dns.add(MockDatanodeDetails.randomDatanodeDetails());
    }
    return dns;
  }

}
