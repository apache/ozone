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

package org.apache.hadoop.ozone.scm.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.DiskBalancerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * This class tests disk balancer operations.
 */
@Timeout(300)
public class TestDiskBalancer {

  private static ScmClient storageClient;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration ozoneConf;
  private static DiskBalancerManager diskBalancerManager;

  @BeforeAll
  public static void setup() throws Exception {
    ozoneConf = new OzoneConfiguration();
    ozoneConf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, PlacementPolicy.class);
    cluster = MiniOzoneCluster.newBuilder(ozoneConf).setNumDatanodes(3).build();
    storageClient = new ContainerOperationClient(ozoneConf);
    cluster.waitForClusterToBeReady();
    diskBalancerManager = cluster.getStorageContainerManager()
        .getDiskBalancerManager();

    for (DatanodeDetails dn: cluster.getStorageContainerManager()
        .getScmNodeManager().getAllNodes()) {
      ((DatanodeInfo) dn).updateStorageReports(
          HddsTestUtils.getRandomNodeReport(3, 1).getStorageReportList());
    }
  }

  @AfterAll
  public static void cleanup() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDatanodeDiskBalancerReport() throws IOException {
    List<HddsProtos.DatanodeDiskBalancerInfoProto> reportProtoList =
        storageClient.getDiskBalancerReport(2);

    assertEquals(2, reportProtoList.size());
    assertTrue(
        reportProtoList.get(0).getCurrentVolumeDensitySum()
        >= reportProtoList.get(1).getCurrentVolumeDensitySum());
  }

  @Test
  public void testDiskBalancerStopAfterEven() throws IOException,
      InterruptedException, TimeoutException {
    String datanodeHostName1 = cluster.getStorageContainerManager()
        .getScmNodeManager()
        .getAllNodes()
        .get(0)
        .getHostName();

    // Start the DiskBalancer with specific parameters
    diskBalancerManager.startDiskBalancer(
        Optional.of(10.0), // threshold
        Optional.of(10L),  // bandwidth in MB
        Optional.of(5),    // parallel threads
        Optional.of(true), // stopAfterDiskEven
        Optional.of(Collections.singletonList(datanodeHostName1))//apply to one datanode
    );

    // Wait until the DiskBalancer status becomes RUNNING for that datanode
    GenericTestUtils.waitFor(() -> {
      try {
        List<HddsProtos.DatanodeDiskBalancerInfoProto> statusList =
            storageClient.getDiskBalancerStatus(Optional.of(Collections.singletonList(datanodeHostName1)),
                Optional.empty());

        return statusList.size() == 1 && statusList.get(0).getNode().getHostName().equals(datanodeHostName1) &&
            statusList.get(0).getRunningStatus() == HddsProtos.DiskBalancerRunningStatus.RUNNING;
      } catch (IOException e) {
        return false;
      }
    }, 5000, 60000); // poll every 5s, timeout after 60s

    // Wait until the DiskBalancer status becomes STOPPED automatically (after even)
    GenericTestUtils.waitFor(() -> {
      try {
        List<HddsProtos.DatanodeDiskBalancerInfoProto> statusList =
            storageClient.getDiskBalancerStatus(Optional.of(Collections.singletonList(datanodeHostName1)),
                Optional.empty());

        return statusList.size() == 1 && statusList.get(0).getNode().getHostName().equals(datanodeHostName1) &&
            statusList.get(0).getRunningStatus() == HddsProtos.DiskBalancerRunningStatus.STOPPED;
      } catch (IOException e) {
        return false;
      }
    }, 1000, 30000); // poll every 1s, timeout after 30s
  }

@Test
  public void testDatanodeDiskBalancerStatus() throws IOException {
    // TODO: Test status command with datanodes in balancing
  }
}
