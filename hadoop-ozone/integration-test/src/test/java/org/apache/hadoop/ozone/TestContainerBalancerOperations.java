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

package org.apache.hadoop.ozone;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerConfiguration;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * This class tests container balancer operations
 * from cblock clients.
 */
public class TestContainerBalancerOperations {

  private static ScmClient containerBalancerClient;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration ozoneConf;

  @BeforeAll
  public static void setup() throws Exception {
    ozoneConf = new OzoneConfiguration();
    ozoneConf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, PlacementPolicy.class);
    ozoneConf.setTimeDuration(HDDS_NODE_REPORT_INTERVAL, 5, SECONDS);
    ozoneConf.setBoolean("hdds.container.balancer.trigger.du.before.move.enable", true);
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
  public void testContainerBalancerCLIOperations() throws Exception {
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
    Optional<Integer> moveReplicationTimeout = Optional.of(50);
    Optional<Boolean> networkTopologyEnable = Optional.of(false);
    Optional<String> includeNodes = Optional.of("");
    Optional<String> excludeNodes = Optional.of("");
    Optional<String> excludeContainers = Optional.of("");
    containerBalancerClient.startContainerBalancer(threshold, iterations,
        maxDatanodesPercentageToInvolvePerIteration,
        maxSizeToMovePerIterationInGB, maxSizeEnteringTargetInGB,
        maxSizeLeavingSourceInGB, balancingInterval, moveTimeout,
        moveReplicationTimeout, networkTopologyEnable, includeNodes,
        excludeNodes, excludeContainers);
    running = containerBalancerClient.getContainerBalancerStatus();
    assertTrue(running);

    GenericTestUtils.waitFor(
        () -> {
          try {
            return !containerBalancerClient.getContainerBalancerStatus();
          } catch (IOException e) {
            return false;
          }
        },
        100,
        30000
    );

    // test normally start , and stop it before balance is completed
    containerBalancerClient.startContainerBalancer(threshold, iterations,
        maxDatanodesPercentageToInvolvePerIteration,
        maxSizeToMovePerIterationInGB, maxSizeEnteringTargetInGB,
        maxSizeLeavingSourceInGB, balancingInterval, moveTimeout,
        moveReplicationTimeout, networkTopologyEnable, includeNodes,
        excludeNodes, excludeContainers);
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
    //Configurations added in ozone-site.xml
    ozoneConf.setInt("hdds.container.balancer.iterations", 40);
    ozoneConf.setInt("hdds.container.balancer.datanodes.involved.max.percentage.per.iteration", 30);

    boolean running = containerBalancerClient.getContainerBalancerStatus();
    assertFalse(running);

    //CLI option for iterations and balancing interval is not passed
    Optional<Integer> iterations = Optional.empty();
    Optional<Integer> balancingInterval = Optional.empty();
    String excludedContainersList = "1,2,3";

    //CLI options are passed
    Optional<Double> threshold = Optional.of(0.1);
    Optional<Integer> maxDatanodesPercentageToInvolvePerIteration =
            Optional.of(100);
    Optional<Long> maxSizeToMovePerIterationInGB = Optional.of(1L);
    Optional<Long> maxSizeEnteringTargetInGB = Optional.of(6L);
    Optional<Long> maxSizeLeavingSourceInGB = Optional.of(6L);
    Optional<Integer> moveTimeout = Optional.of(65);
    Optional<Integer> moveReplicationTimeout = Optional.of(50);
    Optional<Boolean> networkTopologyEnable = Optional.of(true);
    Optional<String> includeNodes = Optional.of("");
    Optional<String> excludeNodes = Optional.of("");
    Optional<String> excludeContainers = Optional.of(excludedContainersList);
    containerBalancerClient.startContainerBalancer(threshold, iterations,
            maxDatanodesPercentageToInvolvePerIteration,
            maxSizeToMovePerIterationInGB, maxSizeEnteringTargetInGB,
            maxSizeLeavingSourceInGB, balancingInterval, moveTimeout,
            moveReplicationTimeout, networkTopologyEnable, includeNodes,
            excludeNodes, excludeContainers);
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

    //Verifies that the 'excludeContainers' passed via CLI overrides the default empty set
    assertEquals(parseContainerIDs(excludedContainersList), config.getExcludeContainers());

    containerBalancerClient.stopContainerBalancer();
    running = containerBalancerClient.getContainerBalancerStatus();
    assertFalse(running);
  }

  /**
   * Tests that stopBalancer is idempotent - once the balancer is in STOPPED state,
   * invoking stop again should be a no-op and return successfully with exit code 0.
   */
  @Test
  public void testStopBalancerIdempotent() throws IOException {
    boolean running = containerBalancerClient.getContainerBalancerStatus();
    assertFalse(running);
    assertDoesNotThrow(() -> containerBalancerClient.stopContainerBalancer());

    Optional<Double> threshold = Optional.of(0.1);
    Optional<Integer> iterations = Optional.of(10000);
    Optional<Integer> maxDatanodesPercentageToInvolvePerIteration =
        Optional.of(100);
    Optional<Long> maxSizeToMovePerIterationInGB = Optional.of(1L);
    Optional<Long> maxSizeEnteringTargetInGB = Optional.of(6L);
    Optional<Long> maxSizeLeavingSourceInGB = Optional.of(6L);
    Optional<Integer> balancingInterval = Optional.of(70);
    Optional<Integer> moveTimeout = Optional.of(65);
    Optional<Integer> moveReplicationTimeout = Optional.of(50);
    Optional<Boolean> networkTopologyEnable = Optional.of(false);
    Optional<String> includeNodes = Optional.of("");
    Optional<String> excludeNodes = Optional.of("");
    Optional<String> excludeContainers = Optional.of("");
    containerBalancerClient.startContainerBalancer(threshold, iterations,
        maxDatanodesPercentageToInvolvePerIteration,
        maxSizeToMovePerIterationInGB, maxSizeEnteringTargetInGB,
        maxSizeLeavingSourceInGB, balancingInterval, moveTimeout,
        moveReplicationTimeout, networkTopologyEnable, includeNodes,
        excludeNodes, excludeContainers);
    running = containerBalancerClient.getContainerBalancerStatus();
    assertTrue(running);

    containerBalancerClient.stopContainerBalancer();
    running = containerBalancerClient.getContainerBalancerStatus();
    assertFalse(running);

    // Calling stop balancer again should not throw an exception
    assertDoesNotThrow(() -> containerBalancerClient.stopContainerBalancer());
  }

  private Set<ContainerID> parseContainerIDs(String containerList) {
    return Arrays.stream(containerList.split(","))
        .map(Long::parseLong)
        .map(ContainerID::valueOf)
        .collect(Collectors.toSet());
  }
}
