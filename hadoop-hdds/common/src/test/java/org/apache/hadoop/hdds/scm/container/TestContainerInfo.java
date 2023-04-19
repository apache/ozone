/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;

/**
 * Tests for the ContainerInfo class.
 */

public class TestContainerInfo {

  @Test
  public void getProtobufMessageEC() throws IOException {
    ContainerInfo container =
        createContainerInfo(RatisReplicationConfig.getInstance(THREE));
    HddsProtos.ContainerInfoProto proto = container.getProtobuf();

    // No EC Config
    Assertions.assertFalse(proto.hasEcReplicationConfig());
    Assertions.assertEquals(THREE, proto.getReplicationFactor());
    Assertions.assertEquals(RATIS, proto.getReplicationType());

    // Reconstruct object from Proto
    ContainerInfo recovered = ContainerInfo.fromProtobuf(proto);
    Assertions.assertEquals(RATIS, recovered.getReplicationType());
    Assertions.assertTrue(
        recovered.getReplicationConfig() instanceof RatisReplicationConfig);

    // EC Config
    container = createContainerInfo(new ECReplicationConfig(3, 2));
    proto = container.getProtobuf();

    Assertions.assertEquals(3, proto.getEcReplicationConfig().getData());
    Assertions.assertEquals(2, proto.getEcReplicationConfig().getParity());
    Assertions.assertFalse(proto.hasReplicationFactor());
    Assertions.assertEquals(EC, proto.getReplicationType());

    // Reconstruct object from Proto
    recovered = ContainerInfo.fromProtobuf(proto);
    Assertions.assertEquals(EC, recovered.getReplicationType());
    Assertions.assertTrue(
        recovered.getReplicationConfig() instanceof ECReplicationConfig);
    ECReplicationConfig config =
        (ECReplicationConfig)recovered.getReplicationConfig();
    Assertions.assertEquals(3, config.getData());
    Assertions.assertEquals(2, config.getParity());
  }

  private ContainerInfo createContainerInfo(ReplicationConfig repConfig) {
    ContainerInfo.Builder builder = new ContainerInfo.Builder();
    builder.setContainerID(1234)
        .setReplicationConfig(repConfig)
        .setPipelineID(PipelineID.randomId())
        .setState(HddsProtos.LifeCycleState.OPEN)
        .setOwner("scm");
    return builder.build();
  }
}
