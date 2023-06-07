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

import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageTypeProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.ozone.container.upgrade.UpgradeUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for StorageUtils.
 */
public class TestStorageUtils {

  @Test
  public void testNoFilters() {
    DatanodeInfo datanodeInfo = new DatanodeInfo(
        MockDatanodeDetails.randomDatanodeDetails(),
        NodeStatus.inServiceHealthy(),
        UpgradeUtils.defaultLayoutVersionProto());
    StorageReportProto data0 =
        HddsTestUtils.createStorageReport(UUID.randomUUID(),
            "/" + UUID.randomUUID(), 2_000_000);
    StorageReportProto data1 =
        HddsTestUtils.createStorageReport(UUID.randomUUID(),
            "/" + UUID.randomUUID(), 6_000_000, 0, 6_000_000,
            StorageTypeProto.SSD);
    datanodeInfo.
        updateStorageReports(new ArrayList<>(asList(data0, data1)));

    MetadataStorageReportProto meta0 =
        HddsTestUtils.createMetadataStorageReport("/meta-" + UUID.randomUUID(),
            2_000_000, 0, 2_000_000, StorageTypeProto.SSD);
    datanodeInfo.
        updateMetaDataStorageReports(
            new ArrayList<>(Arrays.asList(meta0)));

    long dataRequired = 5_000_000;
    long metaRequired = 1_000_000;

    boolean hasSpace =
        StorageUtils.hasEnoughSpace(datanodeInfo, metaRequired, dataRequired);
    //no filters, able to pick suitable volumes
    assertTrue(hasSpace);
  }

  @Test
  public void testFilterNoDataSpace() {
    DatanodeInfo datanodeInfo = new DatanodeInfo(
        MockDatanodeDetails.randomDatanodeDetails(),
        NodeStatus.inServiceHealthy(),
        UpgradeUtils.defaultLayoutVersionProto());

    StorageReportProto data0 =
        HddsTestUtils.createStorageReport(UUID.randomUUID(),
            "/" + UUID.randomUUID(), 2_000_000);
    StorageReportProto data1 =
        HddsTestUtils.createStorageReport(UUID.randomUUID(),
            "/" + UUID.randomUUID(), 6_000_000, 0, 6_000_000,
            StorageTypeProto.SSD);
    datanodeInfo.
        updateStorageReports(new ArrayList<>(asList(data0, data1)));

    MetadataStorageReportProto meta0 =
        HddsTestUtils.createMetadataStorageReport("/meta-" + UUID.randomUUID(),
            2_000_000, 0, 2_000_000, StorageTypeProto.SSD);
    datanodeInfo.
        updateMetaDataStorageReports(
            new ArrayList<>(Arrays.asList(meta0)));

    long dataRequired = 5_000_000;
    long metaRequired = 1_000_000;

    boolean hasSpace =
        StorageUtils.hasEnoughSpace(datanodeInfo, data ->
                data.getStorageType() == StorageTypeProto.DISK,
            null,
            metaRequired, dataRequired);
    //no free DISK storage
    assertFalse(hasSpace);
  }

  @Test
  public void testHasSuitableDiskVolume() {
    DatanodeInfo datanodeInfo = new DatanodeInfo(
        MockDatanodeDetails.randomDatanodeDetails(),
        NodeStatus.inServiceHealthy(),
        UpgradeUtils.defaultLayoutVersionProto());

    StorageReportProto data0 =
        HddsTestUtils.createStorageReport(UUID.randomUUID(),
            "/" + UUID.randomUUID(), 2_000_000);
    StorageReportProto data1 =
        HddsTestUtils.createStorageReport(UUID.randomUUID(),
            "/" + UUID.randomUUID(), 6_000_000, 0, 6_000_000,
            StorageTypeProto.SSD);
    StorageReportProto data2 =
        HddsTestUtils.createStorageReport(UUID.randomUUID(),
            "/" + UUID.randomUUID(), 7_000_000);
    datanodeInfo.
        updateStorageReports(new ArrayList<>(asList(data0, data1, data2)));

    MetadataStorageReportProto meta0 =
        HddsTestUtils.createMetadataStorageReport("/meta-" + UUID.randomUUID(),
            2_000_000, 0, 2_000_000, StorageTypeProto.SSD);
    datanodeInfo.
        updateMetaDataStorageReports(
            new ArrayList<>(Arrays.asList(meta0)));

    long dataRequired = 5_000_000;
    long metaRequired = 1_000_000;

    boolean hasSpace =
        StorageUtils.hasEnoughSpace(datanodeInfo, data ->
                data.getStorageType() == StorageTypeProto.DISK,
            null,
            metaRequired, dataRequired);
    //found suitable volume after filtering
    assertTrue(hasSpace);
  }

  @Test
  public void testFilterNoMetaSpace() {
    DatanodeInfo datanodeInfo = new DatanodeInfo(
        MockDatanodeDetails.randomDatanodeDetails(),
        NodeStatus.inServiceHealthy(),
        UpgradeUtils.defaultLayoutVersionProto());

    StorageReportProto data0 =
        HddsTestUtils.createStorageReport(UUID.randomUUID(),
            "/" + UUID.randomUUID(), 2_000_000);
    StorageReportProto data1 =
        HddsTestUtils.createStorageReport(UUID.randomUUID(),
            "/" + UUID.randomUUID(), 6_000_000, 0, 6_000_000,
            StorageTypeProto.SSD);
    datanodeInfo.
        updateStorageReports(new ArrayList<>(asList(data0, data1)));

    MetadataStorageReportProto meta0 =
        HddsTestUtils.createMetadataStorageReport("/meta-" + UUID.randomUUID(),
            2_000_000, 0, 2_000_000, StorageTypeProto.SSD);
    datanodeInfo.
        updateMetaDataStorageReports(
            new ArrayList<>(Arrays.asList(meta0)));

    long dataRequired = 5_000_000;
    long metaRequired = 1_000_000;

    boolean hasSpace =
        StorageUtils.hasEnoughSpace(datanodeInfo, null,
            meta ->
                meta.getStorageType() == StorageTypeProto.DISK,
            metaRequired, dataRequired);
    //no free metadata DISK storage
    assertFalse(hasSpace);
  }

  @Test
  public void testFilterHasMetaSpace() {
    DatanodeInfo datanodeInfo = new DatanodeInfo(
        MockDatanodeDetails.randomDatanodeDetails(),
        NodeStatus.inServiceHealthy(),
        UpgradeUtils.defaultLayoutVersionProto());

    StorageReportProto data0 =
        HddsTestUtils.createStorageReport(UUID.randomUUID(),
            "/" + UUID.randomUUID(), 2_000_000);
    StorageReportProto data1 =
        HddsTestUtils.createStorageReport(UUID.randomUUID(),
            "/" + UUID.randomUUID(), 6_000_000, 0, 6_000_000,
            StorageTypeProto.SSD);
    datanodeInfo.
        updateStorageReports(new ArrayList<>(asList(data0, data1)));

    MetadataStorageReportProto meta0 =
        HddsTestUtils.createMetadataStorageReport("/meta-" + UUID.randomUUID(),
            2_000_000, 0, 2_000_000, StorageTypeProto.SSD);
    MetadataStorageReportProto meta1 =
        HddsTestUtils.createMetadataStorageReport("/meta-" + UUID.randomUUID(),
            2_000_000, 0, 2_000_000, StorageTypeProto.DISK);
    datanodeInfo.
        updateMetaDataStorageReports(
            new ArrayList<>(Arrays.asList(meta0, meta1)));

    long dataRequired = 5_000_000;
    long metaRequired = 1_000_000;

    boolean hasSpace =
        StorageUtils.hasEnoughSpace(datanodeInfo, null,
            meta ->
                meta.getStorageType() == StorageTypeProto.DISK,
            metaRequired, dataRequired);
    //found suitable metadata DISK space
    assertTrue(hasSpace);
  }

  @Test
  public void testHasSuitableDiskAndMetaVolume() {
    DatanodeInfo datanodeInfo = new DatanodeInfo(
        MockDatanodeDetails.randomDatanodeDetails(),
        NodeStatus.inServiceHealthy(),
        UpgradeUtils.defaultLayoutVersionProto());

    StorageReportProto data0 =
        HddsTestUtils.createStorageReport(UUID.randomUUID(),
            "/" + UUID.randomUUID(), 2_000_000);
    StorageReportProto data1 =
        HddsTestUtils.createStorageReport(UUID.randomUUID(),
            "/" + UUID.randomUUID(), 6_000_000, 0, 6_000_000,
            StorageTypeProto.SSD);
    StorageReportProto data2 =
        HddsTestUtils.createStorageReport(UUID.randomUUID(),
            "/" + UUID.randomUUID(), 7_000_000);
    datanodeInfo.
        updateStorageReports(new ArrayList<>(asList(data0, data1, data2)));

    MetadataStorageReportProto meta0 =
        HddsTestUtils.createMetadataStorageReport("/meta-" + UUID.randomUUID(),
            2_000_000, 0, 2_000_000, StorageTypeProto.SSD);
    MetadataStorageReportProto meta1 =
        HddsTestUtils.createMetadataStorageReport("/meta-" + UUID.randomUUID(),
            2_000_000, 0, 2_000_000, StorageTypeProto.DISK);
    datanodeInfo.
        updateMetaDataStorageReports(
            new ArrayList<>(Arrays.asList(meta0, meta1)));

    long dataRequired = 5_000_000;
    long metaRequired = 1_000_000;

    boolean hasSpace =
        StorageUtils.hasEnoughSpace(datanodeInfo, data ->
                data.getStorageType() == StorageTypeProto.DISK,
            meta ->
                meta.getStorageType() == StorageTypeProto.DISK,
            metaRequired, dataRequired);
    //both filters applied, found space
    assertTrue(hasSpace);
  }
}
