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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerConfiguration;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;

import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This class tests container balancer operations
 * from cblock clients.
 */
@Timeout(value = 300, unit = TimeUnit.MILLISECONDS)
public class TestContainerBalancerOperations {

  private static ScmClient containerBalancerClient;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration ozoneConf;

  @BeforeAll
  public static void setup() throws Exception {
    ozoneConf = new OzoneConfiguration();
    ozoneConf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, PlacementPolicy.class);
    cluster = MiniOzoneCluster.newBuilder(ozoneConf).setNumDatanodes(3).build();
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
  @Unhealthy("Since the cluster doesn't have " +
      "unbalanced nodes, ContainerBalancer stops before the assertion checks " +
      "whether balancer is running.")
  public void testContainerBalancerCLIOperations() throws Exception {
    // test normally start and stop
    boolean running = containerBalancerClient.getContainerBalancerStatus();
    assertFalse(running);
    Optional<Double> threshold = Optional.of(0.1);
    Optional<Integer> iterations = Optional.of(10000);
    Optional<Integer> maxDatanodesPercentageToInvolvePerIteration =
        Optional.of(100);
    Optional<Long> maxSizeToMovePerIterationInGB = Optional.of(1L);
    Optional<Long> maxSizeEnteringTargetInGB = Optional.of(1L);
    Optional<Long> maxSizeLeavingSourceInGB = Optional.of(1L);
    Optional<Long> balancingInterval = Optional.of(1L);
    Optional<Long> moveTimeout = Optional.of(1L);
    Optional<Long> moveReplicationTimeout = Optional.of(1L);
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
   * Test if Container Balancer CLI options override the default configurations.
   */
  @Test
  public void testIfCBCLIOverridesConfigs() throws Exception {
    boolean running = containerBalancerClient.getContainerBalancerStatus();
    assertFalse(running);

    //CLI option for threshold is not passed
    Optional<Double> threshold = Optional.empty();

    //CLI options are passed
    Optional<Integer> maxDatanodesPercentageToInvolvePerIteration =
            Optional.of(100);
    Optional<Long> maxSizeToMovePerIterationInGB = Optional.of(1L);
    Optional<Long> maxSizeEnteringTargetInGB = Optional.of(6L);
    Optional<Long> maxSizeLeavingSourceInGB = Optional.of(6L);
    Optional<Integer> iterations = Optional.of(10000);
    Optional<Long> moveTimeout = Optional.of(65L);
    Optional<Long> moveReplicationTimeout = Optional.of(55L);
    Optional<Long> balancingInterval = Optional.of(70L);
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

    //If CLI option is not passed, it takes the default configuration
    assertEquals(config.getThreshold(), 10);

    //If CLI option is passed, it overrides the default configuration
    assertEquals(config.getIterations(), 10000);
    assertEquals(config.getNetworkTopologyEnable(), true);
  }
}
