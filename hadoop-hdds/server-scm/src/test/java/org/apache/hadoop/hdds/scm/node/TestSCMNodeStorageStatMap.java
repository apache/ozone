/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashSet;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test Node Storage Map.
 */
public class TestSCMNodeStorageStatMap {
  private static final int DATANODE_COUNT = 100;
  private final long capacity = 10L * OzoneConsts.GB;
  private final long used = 2L * OzoneConsts.GB;
  private final long remaining = capacity - used;
  private static OzoneConfiguration conf = new OzoneConfiguration();
  private final Map<UUID, Set<StorageLocationReport>> testData =
      new ConcurrentHashMap<>();

  private void generateData() {
    for (int dnIndex = 1; dnIndex <= DATANODE_COUNT; dnIndex++) {
      UUID dnId = UUID.randomUUID();
      Set<StorageLocationReport> reportSet = new HashSet<>();
      String path = GenericTestUtils.getTempPath(
          TestSCMNodeStorageStatMap.class.getSimpleName() + "-" + dnIndex);
      StorageLocationReport.Builder builder =
          StorageLocationReport.newBuilder();
      builder.setStorageType(StorageType.DISK).setId(dnId.toString())
          .setStorageLocation(path).setScmUsed(used).setRemaining(remaining)
          .setCapacity(capacity).setFailed(false);
      reportSet.add(builder.build());
      testData.put(UUID.randomUUID(), reportSet);
    }
  }

  private UUID getFirstKey() {
    return testData.keySet().iterator().next();
  }

  @BeforeEach
  public void setUp() throws Exception {
    generateData();
  }

  @Test
  public void testIsKnownDatanode() throws SCMException {
    SCMNodeStorageStatMap map = new SCMNodeStorageStatMap(conf);
    UUID knownNode = getFirstKey();
    UUID unknownNode = UUID.randomUUID();
    Set<StorageLocationReport> report = testData.get(knownNode);
    map.insertNewDatanode(knownNode, report);
    Assertions.assertTrue(map.isKnownDatanode(knownNode),
        "Not able to detect a known node");
    Assertions.assertFalse(map.isKnownDatanode(unknownNode),
        "Unknown node detected");
  }

  @Test
  public void testInsertNewDatanode() throws SCMException {
    SCMNodeStorageStatMap map = new SCMNodeStorageStatMap(conf);
    UUID knownNode = getFirstKey();
    Set<StorageLocationReport> report = testData.get(knownNode);
    map.insertNewDatanode(knownNode, report);
    Assertions.assertEquals(map.getStorageVolumes(knownNode),
        testData.get(knownNode));
    Throwable t = Assertions.assertThrows(SCMException.class,
        () -> map.insertNewDatanode(knownNode, report));
    Assertions.assertEquals("Node already exists in the map", t.getMessage());
  }

  @Test
  public void testUpdateUnknownDatanode() {
    SCMNodeStorageStatMap map = new SCMNodeStorageStatMap(conf);
    UUID unknownNode = UUID.randomUUID();
    String path = GenericTestUtils.getTempPath(
        TestSCMNodeStorageStatMap.class.getSimpleName() + "-" + unknownNode);
    Set<StorageLocationReport> reportSet = new HashSet<>();
    StorageLocationReport.Builder builder = StorageLocationReport.newBuilder();
    builder.setStorageType(StorageType.DISK).setId(unknownNode.toString())
        .setStorageLocation(path).setScmUsed(used).setRemaining(remaining)
        .setCapacity(capacity).setFailed(false);
    reportSet.add(builder.build());
    Throwable t = Assertions.assertThrows(SCMException.class,
        () -> map.updateDatanodeMap(unknownNode, reportSet));
    Assertions.assertEquals("No such datanode", t.getMessage());
  }

  @Test
  public void testProcessNodeReportCheckOneNode() throws IOException {
    UUID key = getFirstKey();
    List<StorageReportProto> reportList = new ArrayList<>();
    Set<StorageLocationReport> reportSet = testData.get(key);
    SCMNodeStorageStatMap map = new SCMNodeStorageStatMap(conf);
    map.insertNewDatanode(key, reportSet);
    Assertions.assertTrue(map.isKnownDatanode(key));
    UUID storageId = UUID.randomUUID();
    String path =
        GenericTestUtils.getRandomizedTempPath().concat("/" + storageId);
    StorageLocationReport report = reportSet.iterator().next();
    long reportCapacity = report.getCapacity();
    long reportScmUsed = report.getScmUsed();
    long reportRemaining = report.getRemaining();
    StorageReportProto storageReport = HddsTestUtils.createStorageReport(
        storageId, path, reportCapacity, reportScmUsed, reportRemaining,
        null);
    StorageReportResult result =
        map.processNodeReport(key, HddsTestUtils.createNodeReport(
            Arrays.asList(storageReport), Collections.emptyList()));
    Assertions.assertEquals(SCMNodeStorageStatMap.ReportStatus.ALL_IS_WELL,
        result.getStatus());
    StorageContainerDatanodeProtocolProtos.NodeReportProto.Builder nrb =
        NodeReportProto.newBuilder();
    StorageReportProto srb = reportSet.iterator().next().getProtoBufMessage();
    reportList.add(srb);
    result = map.processNodeReport(key, HddsTestUtils.createNodeReport(
        reportList, Collections.emptyList()));
    Assertions.assertEquals(SCMNodeStorageStatMap.ReportStatus.ALL_IS_WELL,
        result.getStatus());

    reportList.add(HddsTestUtils
        .createStorageReport(UUID.randomUUID(), path, reportCapacity,
            reportCapacity, 0, null));
    result = map.processNodeReport(key, HddsTestUtils.createNodeReport(
        reportList, Collections.emptyList()));
    Assertions.assertEquals(
        SCMNodeStorageStatMap.ReportStatus.STORAGE_OUT_OF_SPACE,
        result.getStatus());
    // Mark a disk failed 
    StorageReportProto srb2 = StorageReportProto.newBuilder()
        .setStorageUuid(UUID.randomUUID().toString())
        .setStorageLocation(srb.getStorageLocation()).setScmUsed(reportCapacity)
        .setCapacity(reportCapacity).setRemaining(0).setFailed(true).build();
    reportList.add(srb2);
    nrb.addAllStorageReport(reportList);
    result = map.processNodeReport(key, nrb.addStorageReport(srb).build());
    Assertions.assertEquals(SCMNodeStorageStatMap.ReportStatus
        .FAILED_AND_OUT_OF_SPACE_STORAGE, result.getStatus());

  }

  @Test
  public void testProcessMultipleNodeReports() throws SCMException {
    SCMNodeStorageStatMap map = new SCMNodeStorageStatMap(conf);
    int counter = 1;
    // Insert all testData into the SCMNodeStorageStatMap Map.
    for (Map.Entry<UUID, Set<StorageLocationReport>> keyEntry : testData
        .entrySet()) {
      map.insertNewDatanode(keyEntry.getKey(), keyEntry.getValue());
    }
    Assertions.assertEquals(DATANODE_COUNT * capacity, map.getTotalCapacity());
    Assertions.assertEquals(DATANODE_COUNT * remaining,
        map.getTotalFreeSpace());
    Assertions.assertEquals(DATANODE_COUNT * used, map.getTotalSpaceUsed());

    // update 1/4th of the datanode to be full
    for (Map.Entry<UUID, Set<StorageLocationReport>> keyEntry : testData
        .entrySet()) {
      Set<StorageLocationReport> reportSet = new HashSet<>();
      String path = GenericTestUtils.getTempPath(
          TestSCMNodeStorageStatMap.class.getSimpleName() + "-" + keyEntry
              .getKey().toString());
      StorageLocationReport.Builder builder =
          StorageLocationReport.newBuilder();
      builder.setStorageType(StorageType.DISK)
          .setId(keyEntry.getKey().toString()).setStorageLocation(path)
          .setScmUsed(capacity).setRemaining(0).setCapacity(capacity)
          .setFailed(false);
      reportSet.add(builder.build());

      map.updateDatanodeMap(keyEntry.getKey(), reportSet);
      counter++;
      if (counter > DATANODE_COUNT / 4) {
        break;
      }
    }
    Assertions.assertEquals(DATANODE_COUNT / 4,
        map.getDatanodeList(SCMNodeStorageStatMap.UtilizationThreshold.CRITICAL)
            .size());
    Assertions.assertEquals(0,
        map.getDatanodeList(SCMNodeStorageStatMap.UtilizationThreshold.WARN)
            .size());
    Assertions.assertEquals(0.75 * DATANODE_COUNT,
        map.getDatanodeList(SCMNodeStorageStatMap.UtilizationThreshold.NORMAL)
            .size(), 0);

    Assertions.assertEquals(DATANODE_COUNT * capacity,
        map.getTotalCapacity(), 0);
    Assertions.assertEquals(0.75 * DATANODE_COUNT * remaining,
        map.getTotalFreeSpace(), 0);
    Assertions.assertEquals(
        0.75 * DATANODE_COUNT * used + (0.25 * DATANODE_COUNT * capacity),
        map.getTotalSpaceUsed(), 0);
    counter = 1;
    // Remove 1/4 of the DataNodes from the Map
    for (Map.Entry<UUID, Set<StorageLocationReport>> keyEntry : testData
        .entrySet()) {
      map.removeDatanode(keyEntry.getKey());
      counter++;
      if (counter > DATANODE_COUNT / 4) {
        break;
      }
    }

    Assertions.assertEquals(0,
        map.getDatanodeList(SCMNodeStorageStatMap.UtilizationThreshold.CRITICAL)
            .size());
    Assertions.assertEquals(0,
        map.getDatanodeList(SCMNodeStorageStatMap.UtilizationThreshold.WARN)
            .size());
    Assertions.assertEquals(0.75 * DATANODE_COUNT,
        map.getDatanodeList(SCMNodeStorageStatMap.UtilizationThreshold.NORMAL)
            .size(), 0);

    Assertions.assertEquals(0.75 * DATANODE_COUNT * capacity,
        map.getTotalCapacity(), 0);
    Assertions.assertEquals(0.75 * DATANODE_COUNT * remaining,
        map.getTotalFreeSpace(), 0);
    Assertions.assertEquals(0.75 * DATANODE_COUNT * used,
        map.getTotalSpaceUsed(), 0);

  }
}
