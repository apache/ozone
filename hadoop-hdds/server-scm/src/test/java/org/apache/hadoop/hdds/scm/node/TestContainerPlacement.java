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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.container.ContainerMapping;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .ContainerPlacementPolicy;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMStorageReport;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_DB_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_DB_CACHE_SIZE_MB;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState
    .HEALTHY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for different container placement policy.
 */
public class TestContainerPlacement {
  @Rule
  public ExpectedException thrown = ExpectedException.none();
  private static XceiverClientManager xceiverClientManager =
      new XceiverClientManager(new OzoneConfiguration());

  /**
   * Returns a new copy of Configuration.
   *
   * @return Config
   */
  OzoneConfiguration getConf() {
    return new OzoneConfiguration();
  }

  /**
   * Creates a NodeManager.
   *
   * @param config - Config for the node manager.
   * @return SCNNodeManager
   * @throws IOException
   */

  SCMNodeManager createNodeManager(OzoneConfiguration config)
      throws IOException {
    SCMNodeManager nodeManager = new SCMNodeManager(config,
        UUID.randomUUID().toString(), null);
    assertFalse("Node manager should be in chill mode",
        nodeManager.isOutOfChillMode());
    return nodeManager;
  }

  ContainerMapping createContainerManager(Configuration config,
      NodeManager scmNodeManager) throws IOException {
    final int cacheSize = config.getInt(OZONE_SCM_DB_CACHE_SIZE_MB,
        OZONE_SCM_DB_CACHE_SIZE_DEFAULT);
    return new ContainerMapping(config, scmNodeManager, cacheSize);

  }

  /**
   * Test capacity based container placement policy with node reports.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testContainerPlacementCapacity() throws IOException,
      InterruptedException, TimeoutException {
    OzoneConfiguration conf = getConf();
    final int nodeCount = 4;
    final long capacity = 10L * OzoneConsts.GB;
    final long used = 2L * OzoneConsts.GB;
    final long remaining = capacity - used;

    final File testDir = PathUtils.getTestDir(
        TestContainerPlacement.class);
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        testDir.getAbsolutePath());
    conf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, ContainerPlacementPolicy.class);

    SCMNodeManager nodeManager = createNodeManager(conf);
    ContainerMapping containerManager =
        createContainerManager(conf, nodeManager);
    List<DatanodeDetails> datanodes =
        TestUtils.getListOfRegisteredDatanodeDetails(nodeManager, nodeCount);
    try {
      for (DatanodeDetails datanodeDetails : datanodes) {
        String id = UUID.randomUUID().toString();
        String path = testDir.getAbsolutePath() + "/" + id;
        List<SCMStorageReport> reports = TestUtils
            .createStorageReport(capacity, used, remaining, path, null, id, 1);
        nodeManager.sendHeartbeat(datanodeDetails.getProtoBufMessage(),
            TestUtils.createNodeReport(reports));
      }

      GenericTestUtils.waitFor(() -> nodeManager.waitForHeartbeatProcessed(),
          100, 4 * 1000);
      assertEquals(nodeCount, nodeManager.getNodeCount(HEALTHY));
      assertEquals(capacity * nodeCount,
          (long) nodeManager.getStats().getCapacity().get());
      assertEquals(used * nodeCount,
          (long) nodeManager.getStats().getScmUsed().get());
      assertEquals(remaining * nodeCount,
          (long) nodeManager.getStats().getRemaining().get());

      assertTrue(nodeManager.isOutOfChillMode());

      ContainerInfo containerInfo = containerManager.allocateContainer(
          xceiverClientManager.getType(),
          xceiverClientManager.getFactor(), "OZONE");
      assertEquals(xceiverClientManager.getFactor().getNumber(),
          containerInfo.getPipeline().getMachines().size());
    } finally {
      IOUtils.closeQuietly(containerManager);
      IOUtils.closeQuietly(nodeManager);
      FileUtil.fullyDelete(testDir);
    }
  }
}
