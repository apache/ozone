/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container.placement.algorithms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.anyObject;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;

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
          datanodeInfo.getUuid(), "/data1-" + datanodeInfo.getUuidString(),
          100L, 0, 100L, null);
      MetadataStorageReportProto metaStorage1 =
          HddsTestUtils.createMetadataStorageReport(
              "/metadata1-" + datanodeInfo.getUuidString(),
          100L, 0, 100L, null);
      datanodeInfo.updateStorageReports(
          new ArrayList<>(Arrays.asList(storage1)));
      datanodeInfo.updateMetaDataStorageReports(
          new ArrayList<>(Arrays.asList(metaStorage1)));

      datanodes.add(datanodeInfo);
    }

    StorageReportProto storage2 = HddsTestUtils.createStorageReport(
        datanodes.get(2).getUuid(),
        "/data1-" + datanodes.get(2).getUuidString(),
        100L, 90L, 10L, null);
    datanodes.get(2).updateStorageReports(
        new ArrayList<>(Arrays.asList(storage2)));
    StorageReportProto storage3 = HddsTestUtils.createStorageReport(
        datanodes.get(3).getUuid(),
        "/data1-" + datanodes.get(3).getUuidString(),
        100L, 80L, 20L, null);
    datanodes.get(3).updateStorageReports(
        new ArrayList<>(Arrays.asList(storage3)));
    StorageReportProto storage4 = HddsTestUtils.createStorageReport(
        datanodes.get(4).getUuid(),
        "/data1-" + datanodes.get(4).getUuidString(),
        100L, 70L, 30L, null);
    datanodes.get(4).updateStorageReports(
        new ArrayList<>(Arrays.asList(storage4)));

    NodeManager mockNodeManager = Mockito.mock(NodeManager.class);
    when(mockNodeManager.getNodes(NodeStatus.inServiceHealthy()))
        .thenReturn(new ArrayList<>(datanodes));

    when(mockNodeManager.getNodeStat(anyObject()))
        .thenReturn(new SCMNodeMetric(100L, 0L, 100L));
    when(mockNodeManager.getNodeStat(datanodes.get(2)))
        .thenReturn(new SCMNodeMetric(100L, 90L, 10L));
    when(mockNodeManager.getNodeStat(datanodes.get(3)))
        .thenReturn(new SCMNodeMetric(100L, 80L, 20L));
    when(mockNodeManager.getNodeStat(datanodes.get(4)))
        .thenReturn(new SCMNodeMetric(100L, 70L, 30L));
    when(mockNodeManager.getNodeByUuid(anyString())).thenAnswer(
            invocation -> {
              String uuid = invocation.getArgument(0);
              return datanodes.stream().filter(
                              datanode ->
                                      datanode.getUuid().toString()
                                              .equals(uuid)).findFirst()
                      .orElse(null);
            });

    SCMContainerPlacementCapacity scmContainerPlacementRandom =
        new SCMContainerPlacementCapacity(mockNodeManager, conf, null, true,
            null);

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
      Assertions.assertEquals(1, datanodeDetails.size());
      DatanodeDetails datanode0Details = datanodeDetails.get(0);

      Assertions.assertNotEquals(
          datanodes.get(0), datanode0Details,
          "Datanode 0 should not been selected: excluded by parameter");
      Assertions.assertNotEquals(
          datanodes.get(1), datanode0Details,
          "Datanode 1 should not been selected: excluded by parameter");
      Assertions.assertNotEquals(
          datanodes.get(2), datanode0Details,
          "Datanode 2 should not been selected: not enough space there");

      selectedCount
          .put(datanode0Details, selectedCount.get(datanode0Details) + 1);

    }

    //datanode 6 has more space than datanode 3 and datanode 4.
    Assertions.assertTrue(selectedCount.get(datanodes.get(3)) < selectedCount
        .get(datanodes.get(6)));
    Assertions.assertTrue(selectedCount.get(datanodes.get(4)) < selectedCount
        .get(datanodes.get(6)));
  }
}