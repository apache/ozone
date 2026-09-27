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

package org.apache.hadoop.hdds.scm.cli.datanode;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_CLIENT_PORT_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DiskBalancerSubCommandUtil}.
 */
public class TestDiskBalancerSubCommandUtil {

  private static final String DN_UUID = "a3b63511-bdf8-4fa1-8ab6-d19c0e806f84";

  @Test
  public void testIsDatanodeUuid() {
    assertTrue(DiskBalancerSubCommandUtil.isDatanodeUuid(DN_UUID));
    assertFalse(DiskBalancerSubCommandUtil.isDatanodeUuid("host-1"));
    assertFalse(DiskBalancerSubCommandUtil.isDatanodeUuid("host-1:19864"));
    assertFalse(DiskBalancerSubCommandUtil.isDatanodeUuid("10.140.95.199"));
    assertFalse(DiskBalancerSubCommandUtil.isDatanodeUuid(
        "a3b63511bdf84fa18ab6d19c0e806f84"));
  }

  @Test
  public void testResolveDatanodeTargetWithHostname() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);

    DiskBalancerSubCommandUtil.DatanodeTarget target =
        DiskBalancerSubCommandUtil.resolveDatanodeTarget(scmClient, "host-1");

    assertEquals("host-1", target.getClientRpcAddress());
    assertEquals("host-1", target.getDisplayName());
  }

  @Test
  public void testResolveDatanodeTargetWithUuid() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    HddsProtos.Node node = buildNode(DN_UUID, "nodename", "10.140.95.199", HDDS_DATANODE_CLIENT_PORT_DEFAULT);
    when(scmClient.queryNode(UUID.fromString(DN_UUID))).thenReturn(node);

    DiskBalancerSubCommandUtil.DatanodeTarget target =
        DiskBalancerSubCommandUtil.resolveDatanodeTarget(scmClient, DN_UUID);

    assertEquals("10.140.95.199:" + HDDS_DATANODE_CLIENT_PORT_DEFAULT,
        target.getClientRpcAddress());
    assertEquals(DN_UUID, target.getDisplayName());
  }

  @Test
  public void testResolveDatanodeTargetWithUnknownUuid() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.queryNode(UUID.fromString(DN_UUID)))
        .thenReturn(HddsProtos.Node.getDefaultInstance());

    IOException ex = assertThrows(IOException.class,
        () -> DiskBalancerSubCommandUtil.resolveDatanodeTarget(scmClient, DN_UUID));
    assertTrue(ex.getMessage().contains("Datanode not found"));
  }

  @Test
  public void testGetClientRpcAddress() throws IOException {
    DatanodeDetails details = DatanodeDetails.getFromProtoBuf(
        buildNode(DN_UUID, "nodename", "10.140.95.199", HDDS_DATANODE_CLIENT_PORT_DEFAULT)
            .getNodeID());

    assertEquals("10.140.95.199:" + HDDS_DATANODE_CLIENT_PORT_DEFAULT,
        DiskBalancerSubCommandUtil.getClientRpcAddress(details));
  }

  @Test
  public void testGetDatanodeHostAndIp() {
    HddsProtos.DatanodeDetailsProto nodeProto = buildNode(
        "6d8157c2-280d-4eb2-a264-d388b05a0a87",
        "ozone-datanode-2.ozone_default",
        "172.18.0.6",
        HDDS_DATANODE_CLIENT_PORT_DEFAULT).getNodeID();

    assertEquals(
        "ozone-datanode-2.ozone_default (172.18.0.6:" + HDDS_DATANODE_CLIENT_PORT_DEFAULT + ")",
        DiskBalancerSubCommandUtil.getDatanodeHostAndIp(nodeProto));
  }

  @Test
  public void testNormalizeNodeIds() {
    List<String> normalized = DiskBalancerSubCommandUtil.normalizeNodeIds(
        Arrays.asList("uuid1,", " uuid2"));
    assertEquals(2, normalized.size());
    assertEquals("uuid1", normalized.get(0));
    assertEquals("uuid2", normalized.get(1));
  }

  private static HddsProtos.Node buildNode(
      String uuid, String hostname, String ipAddress, int clientRpcPort) {
    HddsProtos.DatanodeDetailsProto dnd = HddsProtos.DatanodeDetailsProto.newBuilder()
        .setUuid(uuid)
        .setHostName(hostname)
        .setIpAddress(ipAddress)
        .addPorts(HddsProtos.Port.newBuilder()
            .setName(DatanodeDetails.Port.Name.CLIENT_RPC.name())
            .setValue(clientRpcPort)
            .build())
        .build();
    return HddsProtos.Node.newBuilder().setNodeID(dnd).build();
  }
}
