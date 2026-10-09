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

package org.apache.hadoop.ozone.admin.reconfig;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_CLIENT_PORT_DEFAULT;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ReconfigureSubCommandUtil}.
 */
public class TestReconfigureSubCommandUtil {

  @Test
  public void testGetAllOperableNodesClientRpcAddress() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.queryNode(IN_SERVICE, null, HddsProtos.QueryScope.CLUSTER, "")).thenReturn(Arrays.asList(
        buildNode("10.140.95.199", HddsProtos.NodeState.HEALTHY),
        buildNode("2001:db8::5", HddsProtos.NodeState.HEALTHY),
        buildNode("2001:db8::6", HddsProtos.NodeState.DEAD)));

    assertThat(ReconfigureSubCommandUtil.getAllOperableNodesClientRpcAddress(scmClient)).containsExactly(
        "10.140.95.199:" + HDDS_DATANODE_CLIENT_PORT_DEFAULT,
        "[2001:db8::5]:" + HDDS_DATANODE_CLIENT_PORT_DEFAULT);
  }

  private static HddsProtos.Node buildNode(String ipAddress, HddsProtos.NodeState state) {
    HddsProtos.DatanodeDetailsProto dnd = HddsProtos.DatanodeDetailsProto.newBuilder()
        .setUuid(UUID.randomUUID().toString())
        .setHostName("nodename")
        .setIpAddress(ipAddress)
        .addPorts(HddsProtos.Port.newBuilder()
            .setName(DatanodeDetails.Port.Name.CLIENT_RPC.name())
            .setValue(HDDS_DATANODE_CLIENT_PORT_DEFAULT)
            .build())
        .build();
    return HddsProtos.Node.newBuilder().setNodeID(dnd).addNodeStates(state).build();
  }
}
