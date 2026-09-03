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

package org.apache.hadoop.hdds.scm.container.balancer;

import static org.apache.hadoop.ozone.ClientVersion.DEFAULT_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeUsageInfoProto;
import org.junit.jupiter.api.Test;

/** Tests for {@link ContainerBalancerClusterAnalyzer}. */
public final class TestContainerBalancerClusterAnalyzer {

  private static final double THRESHOLD = 0.10;

  @Test
  void testCalculateAvgUtilizationFromTotals() {
    assertEquals(0.70d, ContainerBalancerClusterAnalyzer
        .calculateAvgUtilization(200, 60), 0.0001);
    assertEquals(0, ContainerBalancerClusterAnalyzer
        .calculateAvgUtilization(0, 0));
  }

  @Test
  void testBalancedClusterHasNoSourcesOrTargets() {
    List<DatanodeUsageInfoProto> nodes = Arrays.asList(
        proto("dn-1", 100, 30),
        proto("dn-2", 100, 30));

    ContainerBalancerClusterSnapshot snapshot =
        ContainerBalancerClusterAnalyzer.analyze(nodes, THRESHOLD,
            Collections.emptySet(), Collections.emptySet());

    assertEquals(2, snapshot.getTotalEligibleDatanodes());
    assertEquals(0.70d, snapshot.getClusterAvgUtilization(), 0.0001);
    assertEquals(0, snapshot.getSourceCount());
    assertEquals(0, snapshot.getTargetCount());
    assertEquals(0, snapshot.getBytesToMove());
    assertTrue(snapshot.getTopSourceNodeHostnames().isEmpty());
    assertTrue(snapshot.getBottomTargetNodeHostnames().isEmpty());
  }

  @Test
  void testOverAndUnderUtilizedClassification() {
    List<DatanodeUsageInfoProto> nodes = Arrays.asList(
        proto("source-1", 100, 10),
        proto("target-1", 100, 50));

    ContainerBalancerClusterSnapshot snapshot =
        ContainerBalancerClusterAnalyzer.analyze(nodes, THRESHOLD,
            Collections.emptySet(), Collections.emptySet());

    assertEquals(1, snapshot.getSourceCount());
    assertEquals(1, snapshot.getTargetCount());
    assertEquals(0.90d, snapshot.getMaxUtilization(), 0.0001);
    assertEquals(0.50d, snapshot.getMinUtilization(), 0.0001);
    assertEquals(0.40d, snapshot.getImbalance(), 0.0001);
    assertEquals(10, snapshot.getTotalOverUtilizedBytes());
    assertEquals(10, snapshot.getTotalUnderUtilizedBytes());
    assertEquals(snapshot.getTotalOverUtilizedBytes(), snapshot.getBytesToMove());
    assertEquals(Collections.singletonList("source-1"),
        snapshot.getTopSourceNodeHostnames());
    assertEquals(Collections.singletonList("target-1"),
        snapshot.getBottomTargetNodeHostnames());
  }

  @Test
  void testIncludeExcludeFilters() {
    List<DatanodeUsageInfoProto> nodes = Arrays.asList(
        proto("keep-me", 100, 10),    // 90%
        proto("also-keep", 100, 50),  // 50%
        proto("drop-me", 100, 30));   // filtered out

    Set<String> include = new HashSet<>(Arrays.asList("keep-me", "also-keep"));

    ContainerBalancerClusterSnapshot snapshot =
        ContainerBalancerClusterAnalyzer.analyze(nodes, THRESHOLD, include,
            Collections.emptySet());

    assertEquals(2, snapshot.getTotalEligibleDatanodes());
    assertEquals(1, snapshot.getSourceCount());  // keep-me
    assertEquals(1, snapshot.getTargetCount()); // also-keep
  }

  @Test
  void testEmptyAfterFilterReturnsEmptySnapshot() {
    List<DatanodeUsageInfoProto> nodes = Collections.singletonList(
        proto("excluded", 100, 10));

    ContainerBalancerClusterSnapshot snapshot =
        ContainerBalancerClusterAnalyzer.analyze(nodes, THRESHOLD,
            Collections.emptySet(), Collections.singleton("excluded"));

    assertEquals(0, snapshot.getTotalEligibleDatanodes());
    assertEquals(0, snapshot.getClusterCapacityBytes());
    assertEquals(0, snapshot.getBytesToMove());
  }

  @Test
  void testZeroCapacityNodeTreatedAsZeroUtilization() {
    List<DatanodeUsageInfoProto> nodes = Arrays.asList(
        proto("zero-cap", 0, 0),
        proto("normal", 100, 30));

    ContainerBalancerClusterSnapshot snapshot =
        ContainerBalancerClusterAnalyzer.analyze(nodes, THRESHOLD,
            Collections.emptySet(), Collections.emptySet());

    assertEquals(2, snapshot.getTotalEligibleDatanodes());
    assertEquals(0, snapshot.getMinUtilization(), 0.0001);
  }

  @Test
  void testProtoWithoutNodeIsSkipped() {
    DatanodeUsageInfoProto noNode = DatanodeUsageInfoProto.newBuilder()
        .setCapacity(100)
        .setRemaining(10)
        .setUsed(90)
        .build();

    ContainerBalancerClusterSnapshot snapshot =
        ContainerBalancerClusterAnalyzer.analyze(
            Collections.singletonList(noNode), THRESHOLD,
            Collections.emptySet(), Collections.emptySet());

    assertEquals(0, snapshot.getTotalEligibleDatanodes());
  }

  @Test
  void testEmptyInputListReturnsEmptySnapshot() {
    ContainerBalancerClusterSnapshot snapshot =
        ContainerBalancerClusterAnalyzer.analyze(
            Collections.emptyList(), THRESHOLD,
            Collections.emptySet(), Collections.emptySet());

    assertEquals(0, snapshot.getTotalEligibleDatanodes());
    assertEquals(0, snapshot.getClusterAvgUtilization(), 0.0001);
    assertEquals(0, snapshot.getBytesToMove());
  }

  @Test
  void testExcludeByIpAddress() {
    DatanodeDetails datanode = DatanodeDetails.newBuilder()
        .setHostName("dn-host")
        .setIpAddress("10.0.0.5")
        .setUuid(UUID.randomUUID())
        .build();
    DatanodeUsageInfoProto node = DatanodeUsageInfoProto.newBuilder()
        .setNode(datanode.toProto(DEFAULT_VERSION.toProtoValue()))
        .setCapacity(100)
        .setRemaining(10)
        .setUsed(90)
        .build();

    ContainerBalancerClusterSnapshot snapshot =
        ContainerBalancerClusterAnalyzer.analyze(
            Collections.singletonList(node), THRESHOLD,
            Collections.emptySet(), Collections.singleton("10.0.0.5"));

    assertEquals(0, snapshot.getTotalEligibleDatanodes());
  }

  @Test
  void testExcludeWinsOverInclude() {
    List<DatanodeUsageInfoProto> nodes = Collections.singletonList(
        proto("both-lists", 100, 10));

    Set<String> include = Collections.singleton("both-lists");
    Set<String> exclude = Collections.singleton("both-lists");

    ContainerBalancerClusterSnapshot snapshot =
        ContainerBalancerClusterAnalyzer.analyze(nodes, THRESHOLD, include, exclude);

    assertEquals(0, snapshot.getTotalEligibleDatanodes());
  }

  @Test
  void testTopFiveSourcesAndBottomFiveTargets() {
    List<DatanodeUsageInfoProto> nodes = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      nodes.add(proto(String.format("source-%02d", i), 100, 10));
    }
    for (int i = 0; i < 7; i++) {
      nodes.add(proto(String.format("target-%02d", i), 100, 50));
    }

    ContainerBalancerClusterSnapshot snapshot =
        ContainerBalancerClusterAnalyzer.analyze(nodes, THRESHOLD,
            Collections.emptySet(), Collections.emptySet());

    assertEquals(7, snapshot.getSourceCount());
    assertEquals(7, snapshot.getTargetCount());
    assertEquals(5, snapshot.getTopSourceNodeHostnames().size());
    assertEquals(5, snapshot.getBottomTargetNodeHostnames().size());
    Set<String> expectedSources = new HashSet<>();
    for (int i = 0; i < 7; i++) {
      expectedSources.add(String.format("source-%02d", i));
    }
    Set<String> expectedTargets = new HashSet<>();
    for (int i = 0; i < 7; i++) {
      expectedTargets.add(String.format("target-%02d", i));
    }
    assertTrue(expectedSources.containsAll(snapshot.getTopSourceNodeHostnames()));
    assertTrue(expectedTargets.containsAll(snapshot.getBottomTargetNodeHostnames()));
  }

  @Test
  void testShouldExcludeDatanodeDirectly() {
    DatanodeDetails datanode = DatanodeDetails.newBuilder()
        .setHostName("dn-1")
        .setIpAddress("10.0.0.1")
        .setUuid(UUID.randomUUID())
        .build();

    assertTrue(ContainerBalancerClusterAnalyzer.shouldExcludeDatanode(
        datanode, Collections.singleton("dn-1"), Collections.emptySet()));
    assertTrue(ContainerBalancerClusterAnalyzer.shouldExcludeDatanode(
        datanode, Collections.singleton("10.0.0.1"), Collections.emptySet()));
    assertTrue(ContainerBalancerClusterAnalyzer.shouldExcludeDatanode(
        datanode, Collections.emptySet(), Collections.singleton("other")));
    assertFalse(ContainerBalancerClusterAnalyzer.shouldExcludeDatanode(
        datanode, Collections.emptySet(), Collections.singleton("dn-1")));
  }

  private static DatanodeUsageInfoProto proto(String hostname, long capacity, long remaining) {
    DatanodeDetails datanode = DatanodeDetails.newBuilder()
        .setHostName(hostname)
        .setIpAddress("127.0.0.1")
        .setUuid(UUID.randomUUID())
        .build();
    long used = capacity - remaining;
    return DatanodeUsageInfoProto.newBuilder()
        .setNode(datanode.toProto(DEFAULT_VERSION.toProtoValue()))
        .setCapacity(capacity)
        .setRemaining(remaining)
        .setUsed(used)
        .build();
  }
}
