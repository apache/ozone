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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerConfiguration;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This class tests container balancer operations
 * from cblock clients.
 */
@Timeout(value = 300)
public class TestContainerBalancerOperations {

  private static ScmClient containerBalancerClient;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration ozoneConf;
  private static RatisReplicationConfig ratisRepConfig =
          RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);
  private static MiniOzoneClusterProvider clusterProvider;
  private static OzoneClient client;

  private static OzoneBucket bucket;
  private static String bucketName = "bucket1";
  private static String volName = "vol1";
  private static final long STORAGE_UNIT = OzoneConsts.GB;


  @BeforeAll
  public static void setup() throws Exception {
    ozoneConf = new OzoneConfiguration();
    ozoneConf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, PlacementPolicy.class);

    MiniOzoneCluster.Builder builder = MiniOzoneCluster.newBuilder(ozoneConf)
            .setNumDatanodes(3);

    clusterProvider = new MiniOzoneClusterProvider(builder, 7);
    cluster = clusterProvider.provide();
    client = cluster.newClient();
    bucket = TestDataUtil.createVolumeAndBucket(client, volName, bucketName);
    cluster = MiniOzoneCluster.newBuilder(ozoneConf).setNumDatanodes(4).build();
    containerBalancerClient = new ContainerOperationClient(ozoneConf);
    cluster.waitForClusterToBeReady();
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
//  @Unhealthy("Since the cluster doesn't have " +
//      "unbalanced nodes, ContainerBalancer stops before the assertion checks " +
//      "whether balancer is running.")
  public void testContainerBalancerCLIOperations() throws Exception {
    // Generate some data on the empty cluster to create some containers
    generateData(50, "key", ratisRepConfig);
    // Add a datanode to create imbalance in the utilization of the datanodes
    cluster = MiniOzoneCluster.newBuilder(ozoneConf).setNumDatanodes(2).build();


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
    Optional<Integer> balancingInterval = Optional.of(80);
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
    generateData(50, "key", ratisRepConfig);

    //Configurations added in ozone-site.xml
    ozoneConf.setInt("hdds.container.balancer.iterations", 40);
    ozoneConf.setInt("hdds.container.balancer.datanodes.involved.max.percentage.per.iteration", 30);

    boolean running = containerBalancerClient.getContainerBalancerStatus();
    assertFalse(running);

    cluster = MiniOzoneCluster.newBuilder(ozoneConf).setNumDatanodes(2).build();

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
    //assertEquals(40, config.getIterations());

    //If config value is added in ozone-site.xml and CLI option is passed
    //then it takes the CLI option.
    assertEquals(100, config.getMaxDatanodesPercentageToInvolvePerIteration());

    containerBalancerClient.stopContainerBalancer();
    running = containerBalancerClient.getContainerBalancerStatus();
    assertFalse(running);
  }

  private void generateData(int keyCount, String keyPrefix,
                            ReplicationConfig replicationConfig) throws IOException {
    for (int i = 0; i < keyCount; i++) {
      TestDataUtil.createKey(bucket, keyPrefix + i, replicationConfig,
              "this is the content");
      System.out.println(keyPrefix);
    }
  }
}
