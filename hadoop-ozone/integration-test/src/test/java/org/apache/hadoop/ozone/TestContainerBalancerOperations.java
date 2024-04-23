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
package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerConfiguration;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;

import org.apache.hadoop.hdds.scm.node.NodeManager;
import static org.apache.hadoop.hdds.scm.node.TestNodeUtil.getDNHostAndPort;
import static org.apache.hadoop.hdds.scm.node.TestNodeUtil.waitForDnToReachPersistedOpState;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

/**
 * This class tests container balancer operations
 * from cblock clients.
 */
@Timeout(value = 300)
public class TestContainerBalancerOperations {
  private static final Logger LOG =
          LoggerFactory.getLogger(TestContainerBalancerOperations.class);

  private static ScmClient containerBalancerClient;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration ozoneConf;
  private static final int DATANODE_COUNT = 4;
  private static String bucketName = "bucket1";
  private static String volName = "vol1";
  private static RatisReplicationConfig ratisRepConfig =
          RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);
  private OzoneBucket bucket;
  private NodeManager nm;
  private ContainerManager cm;
  private PipelineManager pm;
  private StorageContainerManager scm;

  private OzoneClient client;
  private ContainerOperationClient scmClient;

  private static MiniOzoneClusterProvider clusterProvider;

  @BeforeAll
  public static void init() throws Exception {
    ozoneConf = new OzoneConfiguration();
    ozoneConf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, PlacementPolicy.class);
    MiniOzoneCluster.Builder builder = MiniOzoneCluster.newBuilder(ozoneConf)
            .setNumDatanodes(DATANODE_COUNT);
    clusterProvider = new MiniOzoneClusterProvider(builder, 7);
    containerBalancerClient = new ContainerOperationClient(ozoneConf);
    //cluster.waitForClusterToBeReady();
  }

  @AfterAll
  public static void shutdown() throws InterruptedException {
    if (clusterProvider != null) {
      clusterProvider.shutdown();
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    cluster = clusterProvider.provide();
    setManagers();
    client = cluster.newClient();
    bucket = TestDataUtil.createVolumeAndBucket(client, volName, bucketName);
    scmClient = new ContainerOperationClient(cluster.getConf());
  }

  @AfterEach
  public void tearDown() throws InterruptedException, IOException {
    IOUtils.close(LOG, client);
    IOUtils.close(LOG, scmClient);
    if (cluster != null) {
      clusterProvider.destroy(cluster);
    }
  }

  @AfterAll
  public static void cleanup() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * test container balancer operation with {@link ContainerOperationClient}.
   * @throws Exception
   */
  @Test
  @Unhealthy("Since the cluster doesn't have " +
      "unbalanced nodes, ContainerBalancer stops before the assertion checks " +
      "whether balancer is running.")
  public void testContainerBalancerCLIOperations() throws Exception {
    generateData(50, "key", ratisRepConfig);
    ContainerInfo container = waitForAndReturnContainer(ratisRepConfig, 3);
    DatanodeDetails dn
            = getOneDNHostingReplica(getContainerReplicas(container));
    scmClient.decommissionNodes(Arrays.asList(getDNHostAndPort(dn)), false);
    waitForDnToReachPersistedOpState(dn, DECOMMISSIONING);

    // Generate some data on the empty cluster to create some containers
    generateData(50, "key", ratisRepConfig);

    scmClient.recommissionNodes(Arrays.asList(getDNHostAndPort(dn)));
    waitForDnToReachPersistedOpState(dn, IN_SERVICE);

    // test normally start and stop
    boolean running = containerBalancerClient.getContainerBalancerStatus();
    assertFalse(running);
    Optional<Double> threshold = Optional.of(0.1);
    Optional<Integer> iterations = Optional.of(10000);
    Optional<Integer> maxDatanodesPercentageToInvolvePerIteration =
        Optional.of(100);
    Optional<Long> maxSizeToMovePerIterationInGB = Optional.of(1L);
    Optional<Long> maxSizeEnteringTargetInGB = Optional.of(6L);
    Optional<Long> maxSizeLeavingSourceInGB = Optional.of(6L);
    Optional<Integer> balancingInterval = Optional.of(70);
    Optional<Integer> moveTimeout = Optional.of(65);
    Optional<Integer> moveReplicationTimeout = Optional.of(55);
    Optional<Boolean> networkTopologyEnable = Optional.of(false);
    Optional<String> includeNodes = Optional.of("");
    Optional<String> excludeNodes = Optional.of("");
    containerBalancerClient.startContainerBalancer(threshold, iterations,
        maxDatanodesPercentageToInvolvePerIteration,
        maxSizeToMovePerIterationInGB, maxSizeEnteringTargetInGB,
        maxSizeLeavingSourceInGB, balancingInterval, moveTimeout,
        moveReplicationTimeout, networkTopologyEnable, includeNodes,
        excludeNodes);
    running = containerBalancerClient.getContainerBalancerStatus();
    assertTrue(running);

    // waiting for balance completed.
    // TODO: this is a temporary implementation for now
    // modify this after balancer is fully completed
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) { }

    running = containerBalancerClient.getContainerBalancerStatus();
    assertFalse(running);

    // test normally start , and stop it before balance is completed
    containerBalancerClient.startContainerBalancer(threshold, iterations,
        maxDatanodesPercentageToInvolvePerIteration,
        maxSizeToMovePerIterationInGB, maxSizeEnteringTargetInGB,
        maxSizeLeavingSourceInGB, balancingInterval, moveTimeout,
        moveReplicationTimeout, networkTopologyEnable, includeNodes,
        excludeNodes);
    running = containerBalancerClient.getContainerBalancerStatus();
    assertTrue(running);

    containerBalancerClient.stopContainerBalancer();
    running = containerBalancerClient.getContainerBalancerStatus();
    assertFalse(running);
  }

  //TODO: add more acceptance after container balancer is fully completed

  /**
   * Test if Container Balancer CLI overrides default configs and
   * options specified in the configs.
   */
  @Test
  public void testIfCBCLIOverridesConfigs() throws Exception {
    ContainerInfo container = waitForAndReturnContainer(ratisRepConfig, 3);
    DatanodeDetails dn
            = getOneDNHostingReplica(getContainerReplicas(container));
    scmClient.decommissionNodes(Arrays.asList(getDNHostAndPort(dn)), false);
    waitForDnToReachPersistedOpState(dn, DECOMMISSIONING);

    // Generate some data on the empty cluster to create some containers
    generateData(50, "key", ratisRepConfig);

    scmClient.recommissionNodes(Arrays.asList(getDNHostAndPort(dn)));
    waitForDnToReachPersistedOpState(dn, IN_SERVICE);


    //Configurations added in ozone-site.xml
    ozoneConf.setInt("hdds.container.balancer.iterations", 40);
    ozoneConf.setInt("hdds.container.balancer.datanodes.involved.max.percentage.per.iteration", 30);

    boolean running = containerBalancerClient.getContainerBalancerStatus();
    assertFalse(running);

    //CLI option for iterations and balancing interval is not passed
    Optional<Integer> iterations = Optional.empty();
    Optional<Integer> balancingInterval = Optional.empty();

    //CLI options are passed
    Optional<Double> threshold = Optional.of(0.1);
    Optional<Integer> maxDatanodesPercentageToInvolvePerIteration =
            Optional.of(100);
    Optional<Long> maxSizeToMovePerIterationInGB = Optional.of(1L);
    Optional<Long> maxSizeEnteringTargetInGB = Optional.of(6L);
    Optional<Long> maxSizeLeavingSourceInGB = Optional.of(6L);
    Optional<Integer> moveTimeout = Optional.of(65);
    Optional<Integer> moveReplicationTimeout = Optional.of(55);
    Optional<Boolean> networkTopologyEnable = Optional.of(true);
    Optional<String> includeNodes = Optional.of("");
    Optional<String> excludeNodes = Optional.of("");
    containerBalancerClient.startContainerBalancer(threshold, iterations,
            maxDatanodesPercentageToInvolvePerIteration,
            maxSizeToMovePerIterationInGB, maxSizeEnteringTargetInGB,
            maxSizeLeavingSourceInGB, balancingInterval, moveTimeout,
            moveReplicationTimeout, networkTopologyEnable, includeNodes,
            excludeNodes);
    running = containerBalancerClient.getContainerBalancerStatus();
    assertTrue(running);

    ContainerBalancerConfiguration config = cluster.getStorageContainerManager().getContainerBalancer().getConfig();

    //If config value is not added in ozone-site.xml and CLI option is not passed
    //then it takes the default configuration
    assertEquals(70, config.getBalancingInterval().toMinutes());

    //If config value is added in ozone-site.xml and CLI option is not passed
    //then it takes the value from ozone-site.xml
    assertEquals(40, config.getIterations());

    //If config value is added in ozone-site.xml and CLI option is passed
    //then it takes the CLI option.
    assertEquals(100, config.getMaxDatanodesPercentageToInvolvePerIteration());

    containerBalancerClient.stopContainerBalancer();
    running = containerBalancerClient.getContainerBalancerStatus();
    assertFalse(running);
  }

  /**
   * Sets the instance variables to the values for the current MiniCluster.
   */
  private void  setManagers() {
    scm = cluster.getStorageContainerManager();
    nm = scm.getScmNodeManager();
    cm = scm.getContainerManager();
    pm = scm.getPipelineManager();
  }

  /**
   * Generates some data on the cluster so the cluster has some containers.
   * @param keyCount The number of keys to create
   * @param keyPrefix The prefix to use for the key name.
   * @param replicationConfig The replication config for the keys
   * @throws IOException
   */
  private void generateData(int keyCount, String keyPrefix,
                            ReplicationConfig replicationConfig) throws IOException {
    for (int i = 0; i < keyCount; i++) {
      TestDataUtil.createKey(bucket, keyPrefix + i, replicationConfig,
              "this is the content");
    }
  }

  /**
   * Retrieves the containerReplica set for a given container or fails the test
   * if the container cannot be found. This is a helper method to allow the
   * container replica count to be checked in a lambda expression.
   * @param c The container for which to retrieve replicas
   * @return
   */
  private Set<ContainerReplica> getContainerReplicas(ContainerInfo c) {
    return assertDoesNotThrow(() -> cm.getContainerReplicas(c.containerID()),
            "Unexpected exception getting the container replicas");
  }

  /**
   * Select any DN hosting a replica from the Replica Set.
   * @param replicas The set of ContainerReplica
   * @return Any datanode associated one of the replicas
   */
  private DatanodeDetails getOneDNHostingReplica(
          Set<ContainerReplica> replicas) {
    // Now Decommission a host with one of the replicas
    Iterator<ContainerReplica> iter = replicas.iterator();
    ContainerReplica c = iter.next();
    return c.getDatanodeDetails();
  }

  /**
   * Get any container present in the cluster and wait to ensure 3 replicas
   * have been reported before returning the container.
   * @return A single container present on the cluster
   * @throws Exception
   */
  private ContainerInfo waitForAndReturnContainer(ReplicationConfig repConfig,
                                                  int expectedReplicas) throws Exception {
    List<ContainerInfo> containers = cm.getContainers();
    ContainerInfo container = null;
    for (ContainerInfo c : containers) {
      if (c.getReplicationConfig().equals(repConfig)) {
        container = c;
        break;
      }
    }
    // Ensure expected replicas of the container have been reported via ICR
    waitForContainerReplicas(container, expectedReplicas);
    return container;
  }

  private void waitForContainerReplicas(ContainerInfo container, int count)
          throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(
            () -> getContainerReplicas(container).size() == count,
            200, 30000);
  }
}
