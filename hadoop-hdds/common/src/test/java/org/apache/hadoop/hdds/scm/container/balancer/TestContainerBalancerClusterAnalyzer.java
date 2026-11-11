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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.StorageTypeUtils;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeUsageInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeUsageInfoProto;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerClusterSnapshot.NodeUtilization;
import org.junit.jupiter.api.Test;

/** Tests for {@link ContainerBalancerClusterAnalyzer}. */
public final class TestContainerBalancerClusterAnalyzer {

  private static final double THRESHOLD = 0.10;

  @Test
  void testOppositeStorageTypeImbalancesAreNotHiddenByWholeNodeTotals() {
    List<DatanodeUsageInfoProto> nodes = Arrays.asList(typedProto("first", 5, 95), typedProto("second", 95, 5));
    ContainerBalancerClusterSnapshot snapshot = ContainerBalancerClusterAnalyzer.analyze(
        nodes, THRESHOLD, Collections.emptySet(), Collections.emptySet());

    assertEquals(0.5, snapshot.getClusterAvgUtilization());
    assertEquals(400, snapshot.getClusterCapacityBytes());
    assertEquals(2, snapshot.getSourceCount());
    assertEquals(2, snapshot.getTargetCount());
    assertEquals(snapshot.getSourceDatanodes(), snapshot.getTargetDatanodes());
    assertEquals(70, snapshot.getBytesToMove());
    assertFalse(snapshot.hasIncompleteStorageTypeUsage());
    for (StorageType type : Arrays.asList(StorageType.SSD, StorageType.DISK)) {
      StorageTypeSnapshot tier = snapshot.getStorageTypeSnapshots().get(type);
      assertEquals(0.5, tier.getAverageUtilization());
      assertEquals(1, tier.getSourceCount());
      assertEquals(1, tier.getTargetCount());
      assertEquals(35, tier.getBytesToMove());
      assertEquals(type, tier.getTopSourceNodes().get(0).getStorageType());
    }
  }

  @Test
  void testDatanodeIsCountedOnceWhenSourceForMultipleStorageTypes() {
    DatanodeUsageInfoProto source = typedProto("a", 5, 60);
    String sourceId = DatanodeDetails.getFromProtoBuf(source.getNode()).getID().toString();
    ContainerBalancerClusterSnapshot snapshot = ContainerBalancerClusterAnalyzer.analyze(
        Arrays.asList(source, typedProto("b", 20, 80), typedProto("c", 35, 100)),
        THRESHOLD, Collections.emptySet(), Collections.emptySet());

    // A is above both upper limits: SSD 95% > 90%, and DISK 40% > 30%.
    for (StorageType type : Arrays.asList(StorageType.SSD, StorageType.DISK)) {
      StorageTypeSnapshot typeSnapshot = snapshot.getStorageTypeSnapshots().get(type);
      assertEquals(1, typeSnapshot.getSourceCount());
      assertEquals(Collections.singleton(sourceId), typeSnapshot.getSourceDatanodes());
      assertEquals(1, typeSnapshot.getTopSourceNodes().size());
      NodeUtilization typeSource = typeSnapshot.getTopSourceNodes().get(0);
      assertEquals(sourceId, typeSource.getDatanodeId());
      assertEquals(type, typeSource.getStorageType());
    }

    assertEquals(1, snapshot.getSourceCount());
    assertEquals(Collections.singleton(sourceId), snapshot.getSourceDatanodes());
    assertEquals(1, snapshot.getTopSourceNodesAcrossStorageTypes().size());
    NodeUtilization topSource = snapshot.getTopSourceNodesAcrossStorageTypes().get(0);
    assertEquals("a", topSource.getHostname());
    assertEquals(sourceId, topSource.getDatanodeId());
    assertEquals(StorageType.SSD, topSource.getStorageType());
    assertEquals(0.95, topSource.getUtilization(), 0.0001);
  }

  @Test
  void testClusterUtilizationIsSeparateFromMostImbalancedStorageType() {
    ContainerBalancerClusterSnapshot snapshot = ContainerBalancerClusterAnalyzer.analyze(
        Arrays.asList(typedProto("first", 5, 50), typedProto("second", 35, 90)),
        THRESHOLD, Collections.emptySet(), Collections.emptySet());

    // Whole-datanode utilization is 72.5% and 37.5%; DISK is 50%/10% and SSD is 95%/65%.
    assertEquals(0.55, snapshot.getClusterAvgUtilization(), 0.0001);
    assertEquals(0.725, snapshot.getMaxUtilization(), 0.0001);
    assertEquals(0.375, snapshot.getMinUtilization(), 0.0001);
    assertEquals(0.35, snapshot.getImbalance(), 0.0001);
    assertEquals(0.40, snapshot.getMaxImbalance(), 0.0001);
    assertEquals(StorageType.DISK, snapshot.getMostImbalancedStorageType());
    StorageTypeSnapshot disk = snapshot.getStorageTypeSnapshots().get(StorageType.DISK);
    assertEquals(0.50, disk.getMaxUtilization(), 0.0001);
    assertEquals(0.10, disk.getMinUtilization(), 0.0001);
    assertEquals(0.40, disk.getImbalance(), 0.0001);
    StorageTypeSnapshot ssd = snapshot.getStorageTypeSnapshots().get(StorageType.SSD);
    assertEquals(0.95, ssd.getMaxUtilization(), 0.0001);
    assertEquals(0.65, ssd.getMinUtilization(), 0.0001);
    assertEquals(0.30, ssd.getImbalance(), 0.0001);
    assertEquals(disk.getBytesToMove() + ssd.getBytesToMove(), snapshot.getTotalBytesToMove());
    assertEquals(1, snapshot.getSourceCount());
    assertEquals(1, snapshot.getTargetCount());
    NodeUtilization source = snapshot.getTopSourceNodesAcrossStorageTypes().get(0);
    assertEquals(StorageType.SSD, source.getStorageType());
    assertEquals(0.95, source.getUtilization(), 0.0001);
    NodeUtilization target = snapshot.getBottomTargetNodesAcrossStorageTypes().get(0);
    assertEquals(StorageType.DISK, target.getStorageType());
    assertEquals(0.10, target.getUtilization(), 0.0001);
  }

  @Test
  void testEqualStorageTypeImbalancesSelectTypeInEnumOrder() {
    ContainerBalancerClusterSnapshot snapshot = ContainerBalancerClusterAnalyzer.analyze(
        Arrays.asList(typedProto("first", 5, 95), typedProto("second", 95, 5)),
        THRESHOLD, Collections.emptySet(), Collections.emptySet());

    assertEquals(snapshot.getStorageTypeSnapshots().keySet().iterator().next(),
        snapshot.getMostImbalancedStorageType());
    assertEquals(0.90, snapshot.getMaxImbalance(), 0.0001);
    assertEquals(0, snapshot.getImbalance(), 0.0001);
  }

  @Test
  void testSourcesAndTargetsOnDifferentStorageTypesCannotBePaired() {
    // SSD has a source but no target below its lower limit; DISK has the opposite.
    List<DatanodeUsageInfoProto> nodes = Arrays.asList(
        typedProto("first", 0, 100), typedProto("second", 40, 60), typedProto("third", 40, 60));
    ContainerBalancerClusterSnapshot snapshot = ContainerBalancerClusterAnalyzer.analyze(
        nodes, 0.2, Collections.emptySet(), Collections.emptySet());

    assertEquals(0, snapshot.getSourceCount());
    assertEquals(0, snapshot.getTargetCount());
    assertTrue(snapshot.getSourceDatanodes().isEmpty());
    assertTrue(snapshot.getTargetDatanodes().isEmpty());
    assertTrue(snapshot.getTopSourceNodesAcrossStorageTypes().isEmpty());
    assertTrue(snapshot.getBottomTargetNodesAcrossStorageTypes().isEmpty());
    assertTrue(snapshot.getTotalOverUtilizedBytes() > 0);
    assertTrue(snapshot.getTotalUnderUtilizedBytes() > 0);
    assertEquals(0, snapshot.getBytesToMove());
    assertEquals(0.40, snapshot.getMaxImbalance(), 0.0001);

    Set<String> firstNode = Collections.singleton(DatanodeDetails.getFromProtoBuf(nodes.get(0).getNode())
        .getID().toString());
    StorageTypeSnapshot ssd = snapshot.getStorageTypeSnapshots().get(StorageType.SSD);
    assertEquals(1, ssd.getSourceCount());
    assertEquals(firstNode, ssd.getSourceDatanodes());
    assertEquals(1, ssd.getTopSourceNodes().size());
    assertEquals(0, ssd.getTargetCount());
    assertEquals(0, ssd.getBytesToMove());
    StorageTypeSnapshot disk = snapshot.getStorageTypeSnapshots().get(StorageType.DISK);
    assertEquals(0, disk.getSourceCount());
    assertEquals(1, disk.getTargetCount());
    assertEquals(firstNode, disk.getTargetDatanodes());
    assertEquals(1, disk.getBottomTargetNodes().size());
    assertEquals(0, disk.getBytesToMove());
  }

  @Test
  void testCombinedNodesExcludeUnpairedTypeOnSameDatanode() {
    List<DatanodeUsageInfoProto> nodes = Arrays.asList(
        typedProto("first", 0, 20), typedProto("second", 40, 60), typedProto("third", 40, 100));
    ContainerBalancerClusterSnapshot snapshot = ContainerBalancerClusterAnalyzer.analyze(
        nodes, 0.2, Collections.emptySet(), Collections.emptySet());

    // First is a source for both types, but only DISK has a target. Its SSD entry must remain per-type only.
    StorageTypeSnapshot ssd = snapshot.getStorageTypeSnapshots().get(StorageType.SSD);
    StorageTypeSnapshot disk = snapshot.getStorageTypeSnapshots().get(StorageType.DISK);
    assertEquals(1, ssd.getSourceCount());
    assertEquals(0, ssd.getTargetCount());
    assertEquals(0, ssd.getBytesToMove());
    assertEquals(1.0, ssd.getTopSourceNodes().get(0).getUtilization(), 0.0001);
    assertTrue(disk.getBytesToMove() > 0);
    assertEquals(disk.getBytesToMove(), snapshot.getTotalBytesToMove());
    assertEquals(1, snapshot.getSourceCount());
    assertEquals(1, snapshot.getTargetCount());
    assertEquals(disk.getSourceDatanodes(), snapshot.getSourceDatanodes());
    assertEquals(disk.getTargetDatanodes(), snapshot.getTargetDatanodes());
    assertEquals(1, snapshot.getTopSourceNodesAcrossStorageTypes().size());
    assertEquals(1, snapshot.getBottomTargetNodesAcrossStorageTypes().size());
    NodeUtilization source = snapshot.getTopSourceNodesAcrossStorageTypes().get(0);
    assertEquals("first", source.getHostname());
    assertEquals(StorageType.DISK, source.getStorageType());
    assertEquals(0.8, source.getUtilization(), 0.0001);
    NodeUtilization target = snapshot.getBottomTargetNodesAcrossStorageTypes().get(0);
    assertEquals("third", target.getHostname());
    assertEquals(StorageType.DISK, target.getStorageType());
    assertEquals(0, target.getUtilization(), 0.0001);
  }

  @Test
  void testStorageTypeWithOnlyOneDatanodeIsNotPairedWithAnotherType() {
    DatanodeUsageInfoProto ssd = proto("ssd-only", 100, 5).toBuilder()
        .addStorageTypeUsage(storageTypeUsage(StorageType.SSD, 5)).build();
    DatanodeUsageInfoProto disk = proto("disk-only", 100, 95).toBuilder()
        .addStorageTypeUsage(storageTypeUsage(StorageType.DISK, 95)).build();
    ContainerBalancerClusterSnapshot snapshot = ContainerBalancerClusterAnalyzer.analyze(
        Arrays.asList(ssd, disk), THRESHOLD, Collections.emptySet(), Collections.emptySet());

    assertEquals(0, snapshot.getSourceCount());
    assertEquals(0, snapshot.getTargetCount());
    assertEquals(0, snapshot.getBytesToMove());
    assertEquals(2, snapshot.getStorageTypeSnapshots().size());
    assertFalse(snapshot.hasIncompleteStorageTypeUsage());
  }

  @Test
  void testStorageTypeNodesExactlyAtLimitsAreNotUnbalanced() {
    ContainerBalancerClusterSnapshot snapshot = ContainerBalancerClusterAnalyzer.analyze(
        Arrays.asList(typedProto("first", 40, 60), typedProto("second", 60, 40)),
        THRESHOLD, Collections.emptySet(), Collections.emptySet());
    assertEquals(0, snapshot.getSourceCount());
    assertEquals(0, snapshot.getTargetCount());
    assertEquals(0, snapshot.getBytesToMove());
  }

  @Test
  void testPartialReportingAndFiltersApplyBeforeStorageTypeAnalysis() {
    List<DatanodeUsageInfoProto> nodes = Arrays.asList(
        typedProto("first", 5, 95), typedProto("second", 95, 5), proto("legacy", 200, 100));
    ContainerBalancerClusterSnapshot partial = ContainerBalancerClusterAnalyzer.analyze(
        nodes, THRESHOLD, Collections.emptySet(), Collections.emptySet());
    assertTrue(partial.hasIncompleteStorageTypeUsage());

    ContainerBalancerClusterSnapshot filtered = ContainerBalancerClusterAnalyzer.analyze(
        nodes, THRESHOLD, Collections.emptySet(), Collections.singleton("legacy"));
    assertFalse(filtered.hasIncompleteStorageTypeUsage());
    assertEquals(2, filtered.getTotalEligibleDatanodes());
    assertEquals(70, filtered.getBytesToMove());
  }

  @Test
  void testMissingStorageTypeCapacityIsReportedAsIncomplete() {
    DatanodeUsageInfoProto partial = typedProto("partial", 5, 95).toBuilder().removeStorageTypeUsage(1).build();
    ContainerBalancerClusterSnapshot snapshot = ContainerBalancerClusterAnalyzer.analyze(
        Collections.singletonList(partial), THRESHOLD, Collections.emptySet(), Collections.emptySet());
    assertTrue(snapshot.hasIncompleteStorageTypeUsage());
  }

  @Test
  void testLegacyUsageRetainsAggregateAnalysis() {
    ContainerBalancerClusterSnapshot snapshot = ContainerBalancerClusterAnalyzer.analyze(
        Arrays.asList(proto("source", 100, 10), proto("target", 100, 50)),
        THRESHOLD, Collections.emptySet(), Collections.emptySet());
    assertTrue(snapshot.getStorageTypeSnapshots().isEmpty());
    assertFalse(snapshot.hasIncompleteStorageTypeUsage());
    assertEquals(10, snapshot.getBytesToMove());
    assertEquals(snapshot.getImbalance(), snapshot.getMaxImbalance());
    assertNull(snapshot.getMostImbalancedStorageType());
  }

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
    assertTrue(snapshot.getTopSourceNodes().isEmpty());
    assertTrue(snapshot.getBottomTargetNodes().isEmpty());
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
        snapshot.getTopSourceNodes().stream()
            .map(NodeUtilization::getHostname).collect(Collectors.toList()));
    assertEquals(Collections.singletonList("target-1"),
        snapshot.getBottomTargetNodes().stream()
            .map(NodeUtilization::getHostname).collect(Collectors.toList()));
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
        .setNode(datanode.toProto(DEFAULT_VERSION))
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
  void testIncludeAndExcludeByEquivalentIpv6Address() {
    DatanodeDetails datanode = DatanodeDetails.newBuilder()
        .setHostName("dn-host")
        .setIpAddress("2001:db8:0:0:0:0:0:1")
        .setUuid(UUID.randomUUID())
        .build();

    assertTrue(ContainerBalancerClusterAnalyzer.shouldExcludeDatanode(
        datanode, Collections.singleton("2001:db8::1"), Collections.emptySet()));
    assertFalse(ContainerBalancerClusterAnalyzer.shouldExcludeDatanode(
        datanode, Collections.emptySet(), Collections.singleton("2001:db8::1")));
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
    assertEquals(5, snapshot.getTopSourceNodes().size());
    assertEquals(5, snapshot.getBottomTargetNodes().size());
    Set<String> expectedSources = new HashSet<>();
    for (int i = 0; i < 7; i++) {
      expectedSources.add(String.format("source-%02d", i));
    }
    Set<String> expectedTargets = new HashSet<>();
    for (int i = 0; i < 7; i++) {
      expectedTargets.add(String.format("target-%02d", i));
    }
    assertTrue(expectedSources.containsAll(
        snapshot.getTopSourceNodes().stream()
            .map(NodeUtilization::getHostname).collect(Collectors.toList())));
    assertTrue(expectedTargets.containsAll(
        snapshot.getBottomTargetNodes().stream()
            .map(NodeUtilization::getHostname).collect(Collectors.toList())));
  }

  @Test
  void testTopSourceNodesCarryUtilization() {
    List<DatanodeUsageInfoProto> nodes = Arrays.asList(
        proto("source-hot", 100, 5),   // 95% used
        proto("source-warm", 100, 15), // 85% used
        proto("balanced", 100, 50),
        proto("target-cool", 100, 90)  // 10% used
    );

    ContainerBalancerClusterSnapshot snapshot =
        ContainerBalancerClusterAnalyzer.analyze(nodes, THRESHOLD,
            Collections.emptySet(), Collections.emptySet());

    List<NodeUtilization> top = snapshot.getTopSourceNodes();
    assertEquals("source-hot", top.get(0).getHostname());
    assertEquals(0.95, top.get(0).getUtilization(), 1e-9);
    assertEquals("source-warm", top.get(1).getHostname());
    assertEquals(0.85, top.get(1).getUtilization(), 1e-9);

    List<NodeUtilization> bottom = snapshot.getBottomTargetNodes();
    assertEquals("target-cool", bottom.get(0).getHostname());
    assertEquals(0.10, bottom.get(0).getUtilization(), 1e-9);
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

  private static DatanodeUsageInfoProto typedProto(String hostname, long ssdRemaining, long diskRemaining) {
    return proto(hostname, 200, ssdRemaining + diskRemaining).toBuilder()
        .addStorageTypeUsage(storageTypeUsage(StorageType.SSD, ssdRemaining))
        .addStorageTypeUsage(storageTypeUsage(StorageType.DISK, diskRemaining)).build();
  }

  private static StorageTypeUsageInfoProto storageTypeUsage(StorageType type, long remaining) {
    return StorageTypeUsageInfoProto.newBuilder().setStorageType(StorageTypeUtils.getStorageTypeProto(type))
        .setCapacity(100).setUsed(100 - remaining).setRemaining(remaining).build();
  }

  private static DatanodeUsageInfoProto proto(String hostname, long capacity, long remaining) {
    DatanodeDetails datanode = DatanodeDetails.newBuilder()
        .setHostName(hostname)
        .setIpAddress("127.0.0.1")
        .setUuid(UUID.randomUUID())
        .build();
    long used = capacity - remaining;
    return DatanodeUsageInfoProto.newBuilder()
        .setNode(datanode.toProto(DEFAULT_VERSION))
        .setCapacity(capacity)
        .setRemaining(remaining)
        .setUsed(used)
        .build();
  }
}
