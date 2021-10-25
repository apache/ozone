/*
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

package org.apache.hadoop.ozone;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.fs.DUFactory;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancer;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerConfiguration;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerMetrics;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerSelectionCriteria;
import org.apache.hadoop.hdds.scm.container.balancer.FindTargetGreedy;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

/**
 * An integration test for {@link ContainerBalancer}. Simulates an unbalanced
 * cluster by creating a constant number of containers in a certain number of
 * datanodes, and then adding more datanodes.
 */
public class TestContainerBalancer {

  // timeout for each test
  @Rule
  public Timeout timeout = Timeout.seconds(300);

  private MiniOzoneCluster cluster;
  private OzoneConfiguration ozoneConf;
  private StorageContainerManager scm;
  private ContainerBalancerConfiguration balancerConf;
  private ContainerBalancer balancer;
  private File testDir;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestContainerBalancer.class);
  private static final String BUCKET = "bucket1";
  private static final String VOLUME = "vol1";
  private static final int NUM_DATANODES = 6;

  @Before
  public void setup() throws Exception {
    //TODO reset log levels
    GenericTestUtils.setLogLevel(TestContainerBalancer.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(ContainerBalancer.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(FindTargetGreedy.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(ContainerBalancerSelectionCriteria.LOG,
        Level.DEBUG);
    GenericTestUtils.setLogLevel(ReplicationManager.LOG, Level.DEBUG);

    // test directory is used to calculate space of this partition
    testDir = GenericTestUtils.getTestDir(
        TestContainerBalancer.class.getSimpleName());
    if (!testDir.mkdirs()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to create test directory. Using default test " +
            "directory instead.");
      }
      testDir = GenericTestUtils.getTestDir();
    }
    long usedSpace = testDir.getTotalSpace() - testDir.getUsableSpace();
    String reservedSpace = usedSpace + OzoneConsts.GB + "B";

    ozoneConf = createConfiguration();
    cluster = MiniOzoneCluster.newBuilder(ozoneConf)
        .setNumDatanodes(NUM_DATANODES)
        .setDatanodeReservedSpace(reservedSpace)
        .build();
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
    ozoneConf = cluster.getConf();
  }

  @After
  public void cleanup() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
    FileUtils.deleteQuietly(testDir);
  }

  /**
   * Sets up a 6 node cluster such that there are 3 over and 3 under utilized
   * nodes. The over utilized nodes have 4 container replicas each, for a
   * total of 12 replicas. 3 replicas should move in each of the two
   * iterations of balancing, for a total of 6 replicas moved after two
   * iterations. Finally, each of the 6 datanodes should have 2 container
   * replicas(12 replicas / 6 datanodes).
   *
   * @throws Exception
   */
  @Test
  public void shouldBalanceSixNodeClusterWithThreeEmptyNodes()
      throws Exception {
    long containerSize = 640 * OzoneConsts.KB;
    ozoneConf.setStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        containerSize, StorageUnit.BYTES);

    balancerConf = new ContainerBalancerConfiguration(ozoneConf);
    // the threshold should be low enough for the cluster to be unbalanced
    balancerConf.setThreshold(0.0000001);
    balancerConf.setIdleIteration(2);
    // all healthy, in-service datanodes should be involved in balancing
    balancerConf.setMaxDatanodesRatioToInvolvePerIteration(1d);
    balancerConf.setMaxSizeToMovePerIteration(OzoneConsts.GB);
    // balancing interval should be greater than DU refresh period (check the
    // createConfiguration method)
    balancerConf.setBalancingInterval(Duration.ofSeconds(8));
    // so that max two containers can enter an under utilized node
    balancerConf.setMaxSizeEnteringTarget(2 * containerSize);
    ozoneConf.setFromObject(balancerConf);
    cluster.waitForClusterToBeReady();

    // shut down three nodes so that data is written to only the remaining three
    // restart these nodes later to simulate new, empty nodes
    cluster.shutdownHddsDatanode(3);
    cluster.shutdownHddsDatanode(4);
    cluster.shutdownHddsDatanode(5);
    Thread.sleep(1000);

    OzoneBucket bucket =
        TestDataUtil.createVolumeAndBucket(cluster, VOLUME, BUCKET);

    /*
    Write data such that it can be evenly redistributed among the new nodes.
    For example, if 4 container replicas are present in each of the three
    nodes, when three more nodes are introduced, each node should contain 2
    replicas (after balancing).
    */

    // one key should be one container
    int numberOfKeys = 4, keySize = (int) containerSize;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating keys for testing Container Balancer");
    }
    createKeysAndCloseContainers(numberOfKeys, keySize, bucket);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Done creating keys...");
    }

    // restart the rest of the datanodes and start balancer
    cluster.restartHddsDatanode(3, false);
    cluster.restartHddsDatanode(4, false);
    cluster.restartHddsDatanode(5, false);
    cluster.waitForClusterToBeReady();

    balancer = scm.getContainerBalancer();
    balancer.start(balancerConf);
    Thread.sleep(500);
    ContainerBalancerMetrics metrics = balancer.getMetrics();

    // there should be six unbalanced datanodes; 3 over and 3 under utilized
    Assert.assertEquals(metrics.getDatanodesNumToBalance(), NUM_DATANODES);
    try {
      GenericTestUtils.waitFor(() -> !balancer.isBalancerRunning(),
          (int) SECONDS.toMillis(5), (int) SECONDS.toMillis(40));
    } catch (TimeoutException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Timed out while waiting for container balancer to stop");
      }
    } finally {
      balancer.stop();
    }

    Thread.sleep(100);
    List<ContainerInfo> containerInfos =
        scm.getContainerManager().getContainers();

    // each container should have 3 replicas
    containerInfos.forEach(
        container -> {
          try {
            TestHelper.waitForReplicaCount(
                container.containerID().getProtobuf().getId(), 3, cluster);
          } catch (TimeoutException | InterruptedException e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Failed to verify 3 replicas exist for container {} " +
                  "after balancing", container.containerID());
            }
            Assert.fail("Failed to verify 3 container replicas exist after " +
                "balancing");
          }
        });

    NodeManager nodeManager = scm.getScmNodeManager();
    Thread.sleep(1000); // sleeping for longer to debug CI
    // each datanode should have numberOfKeys * 3 / NUM_DATANODES containers now
    nodeManager.getAllNodes().forEach(datanodeDetails -> {
      try {
        Assert.assertEquals(numberOfKeys * 3 / NUM_DATANODES,
            nodeManager.getContainers(datanodeDetails).size());
      } catch (NodeNotFoundException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("NodeManager could not find datanode {} while testing " +
                  "ContainerBalancer, though it should have.",
              datanodeDetails.getUuidString());
        }
      }
    });

    // half of all containers should have moved to the new datanodes (for
    // balance)
    Assert.assertEquals(numberOfKeys * 3 / 2,
        metrics.getMovedContainersNum());
    Assert.assertEquals(containerSize * numberOfKeys * 3 / 2,
        balancer.getSizeMovedPerIteration() * 2);
  }

  /**
   * Creates keys and waits for containers to close.
   * @param numberOfKeys number of keys to write
   * @param keySize size of each key
   * @param bucket bucket to write keys into
   * @throws IOException thrown if key creation fails
   * @throws InterruptedException thrown while waiting for containers to have
   * 3 replicas and close
   */
  private void createKeysAndCloseContainers(int numberOfKeys,
                                            int keySize, OzoneBucket bucket)
      throws IOException, InterruptedException {
    char[] someData = new char[keySize];
    Arrays.fill(someData, 'a');
    String keyValue = new String(someData);

    for (int i = 1; i <= numberOfKeys; i++) {
      String keyName = "key" + i;
      createKey(bucket, keyName, keyValue, keySize,
          HddsProtos.ReplicationFactor.THREE);

      OmKeyLocationInfo omKeyLocationInfo = lookupKey(keyName, keySize);
      // wait for key to be written and container to close
      try {
        long container = omKeyLocationInfo.getContainerID();
        TestHelper.waitForReplicaCount(container, 3, cluster);
        TestHelper.waitForContainerClose(cluster, container);
      } catch (TimeoutException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failed to verify container {} closed for key {}",
              omKeyLocationInfo.getContainerID(), keyName);
        }
      }
    }
  }

  private void createKey(OzoneBucket bucket, String keyName,
                         String keyValue, int keySize,
                         HddsProtos.ReplicationFactor replicationFactor)
      throws IOException {
    ReplicationConfig replicationConfig =
        new RatisReplicationConfig(replicationFactor);
    try (OzoneOutputStream out =
             bucket.createKey(keyName, keySize,
                 replicationConfig, new HashMap<>())) {
      out.write(keyValue.getBytes(UTF_8), 0, keySize);
    }
  }

  private OmKeyLocationInfo lookupKey(
      String keyName, int keySize) throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(keyName)
        .setDataSize(keySize)
        .setReplicationConfig(new RatisReplicationConfig(THREE))
        .build();

    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
    OmKeyLocationInfoGroup locations = keyInfo.getLatestVersionLocations();
    Assert.assertNotNull(locations);
    return locations.getBlocksLatestVersionOnly().get(0);
  }

  private OzoneConfiguration createConfiguration() {
    ozoneConf = new OzoneConfiguration();

    SpaceUsageCheckFactory.Conf spaceUsageConf =
        ozoneConf.getObject(SpaceUsageCheckFactory.Conf.class);
    spaceUsageConf.setClassName(
        "org.apache.hadoop.hdds.fs.DUFactory");
    ozoneConf.setFromObject(spaceUsageConf);
    DUFactory.Conf duConf = ozoneConf.getObject(DUFactory.Conf.class);
    duConf.setRefreshPeriod(Duration.ofSeconds(6));
    ozoneConf.setFromObject(duConf);

    ozoneConf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3,
        TimeUnit.SECONDS);
    ozoneConf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, TimeUnit.SECONDS);
    ozoneConf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, PlacementPolicy.class);
    ozoneConf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        1, SECONDS);
    ozoneConf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 2, SECONDS);
    ozoneConf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 2, SECONDS);
    ozoneConf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 2, SECONDS);
    ozoneConf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 2, SECONDS);
    ozoneConf.setTimeDuration(HDDS_NODE_REPORT_INTERVAL, 2, SECONDS);

    ReplicationManager.ReplicationManagerConfiguration repConf =
        ozoneConf.getObject(
            ReplicationManager.ReplicationManagerConfiguration.class);
    repConf.setInterval(Duration.ofSeconds(6));

    ozoneConf.setFromObject(repConf);
    return ozoneConf;
  }

}