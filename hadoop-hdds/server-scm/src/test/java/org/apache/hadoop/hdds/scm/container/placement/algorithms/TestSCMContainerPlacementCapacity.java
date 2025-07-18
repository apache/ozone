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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.ozone.container.upgrade.UpgradeUtils;
import org.junit.jupiter.api.Test;

/**
 * Test for the scm container placement.
 */
public class TestSCMContainerPlacementCapacity {

  @Test
  public void chooseDatanodes() throws SCMException {
    //given
    OzoneConfiguration conf = new OzoneConfiguration();
    // We are using small units here
    conf.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        1, StorageUnit.BYTES);

    List<DatanodeInfo> datanodes = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
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
    StorageReportProto storage3 = HddsTestUtils.createStorageReport(
        datanodes.get(3).getID(),
        "/data1-" + datanodes.get(3).getID(),
        100L, 80L, 20L, null);
    datanodes.get(3).updateStorageReports(
        new ArrayList<>(Arrays.asList(storage3)));
    StorageReportProto storage4 = HddsTestUtils.createStorageReport(
        datanodes.get(4).getID(),
        "/data1-" + datanodes.get(4).getID(),
        100L, 70L, 30L, null);
    datanodes.get(4).updateStorageReports(
        new ArrayList<>(Arrays.asList(storage4)));

    NodeManager mockNodeManager = mock(NodeManager.class);
    when(mockNodeManager.getNodes(NodeStatus.inServiceHealthy()))
        .thenReturn(new ArrayList<>(datanodes));

    when(mockNodeManager.getNodeStat(any()))
        .thenReturn(new SCMNodeMetric(100L, 0L, 100L, 0, 90, 0));
    when(mockNodeManager.getNodeStat(datanodes.get(2)))
        .thenReturn(new SCMNodeMetric(100L, 90L, 10L, 0, 9, 0));
    when(mockNodeManager.getNodeStat(datanodes.get(3)))
        .thenReturn(new SCMNodeMetric(100L, 80L, 20L, 0, 19, 0));
    when(mockNodeManager.getNodeStat(datanodes.get(4)))
        .thenReturn(new SCMNodeMetric(100L, 70L, 30L, 0, 20, 0));
    when(mockNodeManager.getNode(any(DatanodeID.class))).thenAnswer(
            invocation -> datanodes.stream()
                .filter(dn -> dn.getID().equals(invocation.getArgument(0)))
                .findFirst()
                .orElse(null));

    SCMContainerPlacementCapacity scmContainerPlacementRandom =
        new SCMContainerPlacementCapacity(mockNodeManager, conf, null, true,
            mock(SCMContainerPlacementMetrics.class));

    List<DatanodeDetails> existingNodes = new ArrayList<>();
    existingNodes.add(datanodes.get(0));
    existingNodes.add(datanodes.get(1));

    Map<DatanodeDetails, Integer> selectedCount = new HashMap<>();
    for (DatanodeDetails datanode : datanodes) {
      selectedCount.put(datanode, 0);
    }

    for (int i = 0; i < 1000; i++) {

      //when
      List<DatanodeDetails> datanodeDetails = scmContainerPlacementRandom
          .chooseDatanodes(existingNodes, null, 1, 15, 15);

      //then
      assertEquals(1, datanodeDetails.size());
      DatanodeDetails datanode0Details = datanodeDetails.get(0);

      assertNotEquals(
          datanodes.get(0), datanode0Details,
          "Datanode 0 should not been selected: excluded by parameter");
      assertNotEquals(
          datanodes.get(1), datanode0Details,
          "Datanode 1 should not been selected: excluded by parameter");
      assertNotEquals(
          datanodes.get(2), datanode0Details,
          "Datanode 2 should not been selected: not enough space there");

      selectedCount
          .put(datanode0Details, selectedCount.get(datanode0Details) + 1);

    }

    //datanode 6 has more space than datanode 3 and datanode 4.
    assertThat(selectedCount.get(datanodes.get(3)))
        .isLessThan(selectedCount.get(datanodes.get(6)));
    assertThat(selectedCount.get(datanodes.get(4)))
        .isLessThan(selectedCount.get(datanodes.get(6)));
  }
}
