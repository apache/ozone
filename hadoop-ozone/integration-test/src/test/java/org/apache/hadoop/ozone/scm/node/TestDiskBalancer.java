/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.scm.node;

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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * This class tests disk balancer operations.
 */
public class TestDiskBalancer {

  /**
    * Set a timeout for each test.
    */
  @Rule
  public Timeout timeout = Timeout.seconds(300);

  private static ScmClient storageClient;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration ozoneConf;
  private static DiskBalancerManager diskBalancerManager;

  @BeforeClass
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

  @AfterClass
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
    Assert.assertTrue(
        reportProtoList.get(0).getCurrentVolumeDensitySum()
        >= reportProtoList.get(1).getCurrentVolumeDensitySum());
  }

  @Test
  public void testDatanodeDiskBalancerStatus() throws IOException {
    // TODO: Test status command with datanodes in balancing
  }
}
