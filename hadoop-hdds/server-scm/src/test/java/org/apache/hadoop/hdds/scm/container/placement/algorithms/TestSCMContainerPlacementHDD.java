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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container.placement.algorithms;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageTypeProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.ozone.container.upgrade.UpgradeUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.mockito.Mockito.when;

/**
 * Test for the DISK-only volume container placement.
 */
public class TestSCMContainerPlacementHDD {

  @Test
  public void chooseDatanodes() throws SCMException {
    //given
    OzoneConfiguration conf = new OzoneConfiguration();
    // We are using small units here
    conf.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        1, StorageUnit.BYTES);

    List<DatanodeInfo> datanodes = new ArrayList<>();

    DatanodeInfo datanodeInfo0 = new DatanodeInfo(
        MockDatanodeDetails.randomDatanodeDetails(),
        NodeStatus.inServiceHealthy(),
        UpgradeUtils.defaultLayoutVersionProto());
    StorageReportProto storage01 = HddsTestUtils.createStorageReport(
        datanodeInfo0.getUuid(), "/data1-" + datanodeInfo0.getUuidString(),
        100L, 0, 100L, null);
    StorageReportProto storage02 = HddsTestUtils.createStorageReport(
        datanodeInfo0.getUuid(), "/data2-" + datanodeInfo0.getUuidString(),
        300L, 0, 300L, StorageTypeProto.SSD);
    MetadataStorageReportProto metaStorage01 =
        HddsTestUtils.createMetadataStorageReport(
            "/metadata1-" + datanodeInfo0.getUuidString(),
            100L, 0, 100L, null);
    datanodeInfo0.updateStorageReports(
        new ArrayList<>(Arrays.asList(storage01, storage02)));
    datanodeInfo0.updateMetaDataStorageReports(
        new ArrayList<>(Arrays.asList(metaStorage01)));
    datanodes.add(datanodeInfo0);

    DatanodeInfo datanodeInfo1 = new DatanodeInfo(
        MockDatanodeDetails.randomDatanodeDetails(),
        NodeStatus.inServiceHealthy(),
        UpgradeUtils.defaultLayoutVersionProto());
    StorageReportProto storage11 = HddsTestUtils.createStorageReport(
        datanodeInfo1.getUuid(), "/data1-" + datanodeInfo1.getUuidString(),
        100L, 0, 100L, StorageTypeProto.SSD);
    StorageReportProto storage12 = HddsTestUtils.createStorageReport(
        datanodeInfo1.getUuid(), "/data2-" + datanodeInfo1.getUuidString(),
        300L, 0, 300L, null);
    MetadataStorageReportProto metaStorage11 =
        HddsTestUtils.createMetadataStorageReport(
            "/metadata1-" + datanodeInfo1.getUuidString(),
            100L, 0, 100L, null);
    datanodeInfo1.updateStorageReports(
        new ArrayList<>(Arrays.asList(storage11, storage12)));
    datanodeInfo1.updateMetaDataStorageReports(
        new ArrayList<>(Arrays.asList(metaStorage11)));
    datanodes.add(datanodeInfo1);

    DatanodeInfo datanodeInfo2 = new DatanodeInfo(
        MockDatanodeDetails.randomDatanodeDetails(),
        NodeStatus.inServiceHealthy(),
        UpgradeUtils.defaultLayoutVersionProto());
    StorageReportProto storage21 = HddsTestUtils.createStorageReport(
        datanodeInfo2.getUuid(), "/data1-" + datanodeInfo2.getUuidString(),
        100L, 0, 100L, StorageTypeProto.SSD);
    StorageReportProto storage22 = HddsTestUtils.createStorageReport(
        datanodeInfo2.getUuid(), "/data2-" + datanodeInfo2.getUuidString(),
        300L, 0, 300L, StorageTypeProto.SSD);
    MetadataStorageReportProto metaStorage21 =
        HddsTestUtils.createMetadataStorageReport(
            "/metadata1-" + datanodeInfo2.getUuidString(),
            100L, 0, 100L, null);
    datanodeInfo2.updateStorageReports(
        new ArrayList<>(Arrays.asList(storage21, storage22)));
    datanodeInfo2.updateMetaDataStorageReports(
        new ArrayList<>(Arrays.asList(metaStorage21)));
    datanodes.add(datanodeInfo2);

    DatanodeInfo datanodeInfo3 = new DatanodeInfo(
        MockDatanodeDetails.randomDatanodeDetails(),
        NodeStatus.inServiceHealthy(),
        UpgradeUtils.defaultLayoutVersionProto());
    StorageReportProto storage31 = HddsTestUtils.createStorageReport(
        datanodeInfo3.getUuid(), "/data1-" + datanodeInfo3.getUuidString(),
        100L, 0, 100L, null);
    StorageReportProto storage32 = HddsTestUtils.createStorageReport(
        datanodeInfo3.getUuid(), "/data2-" + datanodeInfo3.getUuidString(),
        300L, 0, 300L, null);
    MetadataStorageReportProto metaStorage31 =
        HddsTestUtils.createMetadataStorageReport(
            "/metadata1-" + datanodeInfo3.getUuidString(),
            100L, 0, 100L, StorageTypeProto.SSD);
    datanodeInfo3.updateStorageReports(
        new ArrayList<>(Arrays.asList(storage31, storage32)));
    datanodeInfo3.updateMetaDataStorageReports(
        new ArrayList<>(Arrays.asList(metaStorage31)));
    datanodes.add(datanodeInfo3);

    NodeManager mockNodeManager = Mockito.mock(NodeManager.class);
    when(mockNodeManager.getNodes(NodeStatus.inServiceHealthy()))
        .thenReturn(new ArrayList<>(datanodes));

    SCMContainerPlacementHDD scmContainerPlacement =
        new SCMContainerPlacementHDD(mockNodeManager, conf, null, true,
            null);

    for (int i = 0; i < 100; i++) {
      //when
      List<DatanodeDetails> datanodeDetails = scmContainerPlacement
          .chooseDatanodes(null, null, 1, 15, 120);
      //then
      Assertions.assertEquals(1, datanodeDetails.size());
      //this is either node1 or node3. The type of metadata storage
      //is not taken into account while filtering volumes
      DatanodeDetails datanode0Details = datanodeDetails.get(0);

      Assertions.assertNotEquals(
          datanodes.get(0), datanode0Details,
          "Datanode 0 should not been selected: not available space on DISK");
      Assertions.assertNotEquals(
          datanodes.get(2), datanode0Details,
          "Datanode 2 should not been selected: no DISK volumes");
    }
  }

}
