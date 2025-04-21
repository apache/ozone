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

package org.apache.hadoop.hdds.scm.container.placement.algorithms;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.ozone.container.upgrade.UpgradeUtils;
import org.junit.jupiter.api.Test;

/**
 * Test for the random container placement.
 */
public class TestSCMContainerPlacementRandom {

  @Test
  public void chooseDatanodes() throws SCMException {
    //given
    OzoneConfiguration conf = new OzoneConfiguration();
    // We are using small units here
    conf.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        1, StorageUnit.BYTES);

    List<DatanodeInfo> datanodes = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      DatanodeInfo datanodeInfo = new DatanodeInfo(
          MockDatanodeDetails.randomDatanodeDetails(),
          NodeStatus.inServiceHealthy(),
          UpgradeUtils.defaultLayoutVersionProto());

      StorageReportProto storage1 = HddsTestUtils.createStorageReport(
          datanodeInfo.getID(), "/data1-" + datanodeInfo.getID(),
          100L, 0, 100L, null);
      MetadataStorageReportProto metaStorage1 =
          HddsTestUtils.createMetadataStorageReport(
              "/metadata1-" + datanodeInfo.getID(),
              100L, 0, 100L, null);
      datanodeInfo.updateStorageReports(
          new ArrayList<>(Arrays.asList(storage1)));
      datanodeInfo.updateMetaDataStorageReports(
          new ArrayList<>(Arrays.asList(metaStorage1)));

      datanodes.add(datanodeInfo);
    }

    StorageReportProto storage2 = HddsTestUtils.createStorageReport(
        datanodes.get(2).getID(),
        "/data1-" + datanodes.get(2).getID(),
        100L, 90L, 10L, null);
    datanodes.get(2).updateStorageReports(
        new ArrayList<>(Arrays.asList(storage2)));

    NodeManager mockNodeManager = mock(NodeManager.class);
    when(mockNodeManager.getNodes(NodeStatus.inServiceHealthy()))
        .thenReturn(new ArrayList<>(datanodes));

    SCMContainerPlacementRandom scmContainerPlacementRandom =
        new SCMContainerPlacementRandom(mockNodeManager, conf, null, true,
            mock(SCMContainerPlacementMetrics.class));

    List<DatanodeDetails> existingNodes = new ArrayList<>();
    existingNodes.add(datanodes.get(0));
    existingNodes.add(datanodes.get(1));

    for (int i = 0; i < 100; i++) {
      //when
      List<DatanodeDetails> datanodeDetails = scmContainerPlacementRandom
          .chooseDatanodes(existingNodes, null, 1, 15, 15);

      //then
      assertEquals(1, datanodeDetails.size());
      DatanodeDetails datanode0Details = datanodeDetails.get(0);

      assertNotEquals(datanodes.get(0), datanode0Details,
          "Datanode 0 should not been selected: excluded by parameter");
      assertNotEquals(datanodes.get(1), datanode0Details,
          "Datanode 1 should not been selected: excluded by parameter");
      assertNotEquals(datanodes.get(2), datanode0Details,
          "Datanode 2 should not been selected: not enough space there");

    }
  }

  @Test
  public void testPlacementPolicySatisified() {
    //given
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        10, StorageUnit.MB);

    List<DatanodeDetails> datanodes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      datanodes.add(MockDatanodeDetails.randomDatanodeDetails());
    }

    NodeManager mockNodeManager = mock(NodeManager.class);
    SCMContainerPlacementRandom scmContainerPlacementRandom =
        new SCMContainerPlacementRandom(mockNodeManager, conf, null, true,
            mock(SCMContainerPlacementMetrics.class));
    ContainerPlacementStatus status =
        scmContainerPlacementRandom.validateContainerPlacement(datanodes, 3);
    assertTrue(status.isPolicySatisfied());
    assertEquals(0, status.misReplicationCount());

    status = scmContainerPlacementRandom.validateContainerPlacement(
        new ArrayList<DatanodeDetails>(), 3);
    assertFalse(status.isPolicySatisfied());

    // Only expect 1 more replica to give us one rack on this policy.
    assertEquals(1, status.misReplicationCount(), 3);

    datanodes = new ArrayList<DatanodeDetails>();
    datanodes.add(MockDatanodeDetails.randomDatanodeDetails());
    status = scmContainerPlacementRandom.validateContainerPlacement(
        datanodes, 3);
    assertTrue(status.isPolicySatisfied());

    // Only expect 1 more replica to give us one rack on this policy.
    assertEquals(0, status.misReplicationCount(), 3);
  }

  @Test
  public void testIsValidNode() throws SCMException {
    //given
    OzoneConfiguration conf = new OzoneConfiguration();
    // We are using small units here
    conf.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        1, StorageUnit.BYTES);

    List<DatanodeInfo> datanodes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      DatanodeInfo datanodeInfo = new DatanodeInfo(
          MockDatanodeDetails.randomDatanodeDetails(),
          NodeStatus.inServiceHealthy(),
          UpgradeUtils.defaultLayoutVersionProto());

      StorageReportProto storage1 = HddsTestUtils.createStorageReport(
          datanodeInfo.getID(), "/data1-" + datanodeInfo.getID(),
          100L, 0, 100L, null);
      MetadataStorageReportProto metaStorage1 =
          HddsTestUtils.createMetadataStorageReport(
              "/metadata1-" + datanodeInfo.getID(),
              100L, 0, 100L, null);
      datanodeInfo.updateStorageReports(
          new ArrayList<>(Arrays.asList(storage1)));
      datanodeInfo.updateMetaDataStorageReports(
          new ArrayList<>(Arrays.asList(metaStorage1)));

      datanodes.add(datanodeInfo);
    }

    StorageReportProto storage1 = HddsTestUtils.createStorageReport(
        datanodes.get(1).getID(),
        "/data1-" + datanodes.get(1).getID(),
        100L, 90L, 10L, null);
    datanodes.get(1).updateStorageReports(
        new ArrayList<>(Arrays.asList(storage1)));

    MetadataStorageReportProto metaStorage2 =
        HddsTestUtils.createMetadataStorageReport(
            "/metadata1-" + datanodes.get(2).getID(),
            100L, 90, 10L, null);
    datanodes.get(2).updateMetaDataStorageReports(
        new ArrayList<>(Arrays.asList(metaStorage2)));

    NodeManager mockNodeManager = mock(NodeManager.class);
    when(mockNodeManager.getNodes(NodeStatus.inServiceHealthy()))
        .thenReturn(new ArrayList<>(datanodes));
    when(mockNodeManager.getNode(datanodes.get(0).getID()))
        .thenReturn(datanodes.get(0));
    when(mockNodeManager.getNode(datanodes.get(1).getID()))
        .thenReturn(datanodes.get(1));
    when(mockNodeManager.getNode(datanodes.get(2).getID()))
        .thenReturn(datanodes.get(2));

    SCMContainerPlacementRandom scmContainerPlacementRandom =
        new SCMContainerPlacementRandom(mockNodeManager, conf, null, true,
            mock(SCMContainerPlacementMetrics.class));

    assertTrue(
        scmContainerPlacementRandom.isValidNode(datanodes.get(0), 15L, 15L));
    assertFalse(
        scmContainerPlacementRandom.isValidNode(datanodes.get(1), 15L, 15L));
    assertFalse(
        scmContainerPlacementRandom.isValidNode(datanodes.get(2), 15L, 15L));

  }

}
