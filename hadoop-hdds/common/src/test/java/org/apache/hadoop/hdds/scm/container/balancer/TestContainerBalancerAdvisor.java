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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeUsageInfoProto;
import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.jupiter.api.Test;

/** Tests for {@link ContainerBalancerAdvisor} dry-run estimation. */
public final class TestContainerBalancerAdvisor {

  @Test
  void testComputePerIterationBytesNeverExceedsBytesToMove() {
    int[] involved = {3, 3};
    long bytesToMove = 50L * OzoneConsts.GB;

    assertEquals(bytesToMove, ContainerBalancerAdvisor.computePerIterationBytes(
        bytesToMove,
        500L * OzoneConsts.GB,
        26 * OzoneConsts.GB,
        26 * OzoneConsts.GB,
        involved));
  }

  @Test
  void testEstimateDryRunDefaultReturnsMediumProfile() {
    OzoneConfiguration conf = new OzoneConfiguration();
    List<ContainerBalancerEstimation> results = ContainerBalancerAdvisor.estimateDryRun(
        conf,
        new ContainerBalancerAdvisor.AdvisorRequest().setNodes(buildCluster(70, 14, 14)));

    assertEquals(1, results.size());
    ContainerBalancerEstimation estimation = results.get(0);
    assertThat(estimation.succeeded()).isTrue();
    assertEquals(ContainerBalancerProfile.MEDIUM, estimation.getProfile());
    assertThat(estimation.getBytesToMove()).isPositive();
    assertEquals(26L * OzoneConsts.GB * 7, estimation.getPerIterationBytes());
  }

  @Test
  void testEstimateDryRunAllProfilesReturnsThreeProfiles() {
    OzoneConfiguration conf = new OzoneConfiguration();
    List<ContainerBalancerEstimation> results = ContainerBalancerAdvisor.estimateDryRun(
        conf,
        new ContainerBalancerAdvisor.AdvisorRequest()
            .setNodes(buildCluster(70, 14, 14))
            .setAllProfiles(true));

    assertEquals(3, results.size());
    assertEquals(ContainerBalancerProfile.SLOW, results.get(0).getProfile());
    assertEquals(ContainerBalancerProfile.MEDIUM, results.get(1).getProfile());
    assertEquals(ContainerBalancerProfile.FAST, results.get(2).getProfile());
    assertThat(results.get(0).succeeded()).isTrue();
    assertThat(results.get(1).succeeded()).isTrue();
    assertThat(results.get(2).succeeded()).isTrue();

    long bytesToMove = results.get(0).getBytesToMove();
    assertThat(bytesToMove).isPositive();
    for (ContainerBalancerEstimation result : results) {
      assertEquals(bytesToMove, result.getBytesToMove());
    }

    // buildCluster(70, 14, 14): 42 sources, 14 targets; SLOW [4,3], MEDIUM [7,7], FAST [14,14]
    assertEquals(30L * OzoneConsts.GB, results.get(0).getPerIterationBytes());
    assertEquals(26L * OzoneConsts.GB * 7, results.get(1).getPerIterationBytes());
    assertEquals(500L * OzoneConsts.GB, results.get(2).getPerIterationBytes());
  }

  @Test
  void testEstimateDryRunSingleProfileFast() {
    OzoneConfiguration conf = new OzoneConfiguration();
    ContainerBalancerConfiguration balancerConfig = conf.getObject(ContainerBalancerConfiguration.class);
    long expectedCycleTimeMillis = ContainerBalancerAdvisor.computeCycleTimeMillis(
        balancerConfig.getMoveTimeout().toMillis(),
        balancerConfig.getBalancingInterval().toMillis());

    List<ContainerBalancerEstimation> results = ContainerBalancerAdvisor.estimateDryRun(
        conf,
        new ContainerBalancerAdvisor.AdvisorRequest()
            .setNodes(buildCluster(70, 14, 14))
            .setProfile(ContainerBalancerProfile.FAST));

    assertEquals(1, results.size());
    ContainerBalancerEstimation estimation = results.get(0);
    assertThat(estimation.succeeded()).isTrue();
    assertEquals(ContainerBalancerProfile.FAST, estimation.getProfile());
    // 40% of 70 -> maxInvolved=28 -> [14 sources, 14 targets];
    // 14*100GB exceeds the 500GB iteration cap, so the cap binds.
    assertEquals(500L * OzoneConsts.GB, estimation.getPerIterationBytes());
    assertEquals(expectedCycleTimeMillis,
        estimation.getMoveTimeoutMillis() + estimation.getBalancingIntervalMillis());
    assertEquals(
        (long) Math.ceil((double) estimation.getBytesToMove() / estimation.getPerIterationBytes()),
        estimation.getEstimatedIterations());
    assertEquals(
        estimation.getEstimatedIterations() * expectedCycleTimeMillis,
        estimation.getEstimatedDurationMillis());
    assertEquals(40, estimation.getMaxDatanodesPercentage());
  }

  @Test
  void testEstimateDryRunRespectsThresholdOverride() {
    OzoneConfiguration conf = new OzoneConfiguration();
    List<DatanodeUsageInfoProto> nodes = buildCluster(70, 14, 14);

    long defaultThresholdBytesToMove = ContainerBalancerAdvisor.estimateDryRun(
            conf,
            new ContainerBalancerAdvisor.AdvisorRequest().setNodes(nodes))
        .get(0)
        .getBytesToMove();

    long tighterThresholdBytesToMove = ContainerBalancerAdvisor.estimateDryRun(
            conf,
            new ContainerBalancerAdvisor.AdvisorRequest()
                .setNodes(nodes)
                .setThresholdPercent(5.0))
        .get(0)
        .getBytesToMove();

    assertThat(tighterThresholdBytesToMove).isGreaterThan(defaultThresholdBytesToMove);
  }

  @Test
  void testEstimateDryRunRespectsExplicitMaxDatanodesPercentageOverride() {
    OzoneConfiguration conf = new OzoneConfiguration();
    List<ContainerBalancerEstimation> results = ContainerBalancerAdvisor.estimateDryRun(
        conf,
        new ContainerBalancerAdvisor.AdvisorRequest()
            .setNodes(buildCluster(70, 14, 14))
            .setProfile(ContainerBalancerProfile.SLOW)
            .setMaxDatanodesPercentageToInvolvePerIteration(1));
    assertEquals(1, results.size());
    assertThat(results.get(0).succeeded()).isFalse();
    assertThat(results.get(0).getFailureMessage()).contains("at least 2 are required");
  }

  @Test
  void testEstimateDryRunRespectsMaxSizeLeavingSourceOverride() {
    OzoneConfiguration conf = new OzoneConfiguration();
    List<DatanodeUsageInfoProto> nodes = buildCluster(70, 14, 14);

    ContainerBalancerEstimation baseline = ContainerBalancerAdvisor.estimateDryRun(
            conf,
            new ContainerBalancerAdvisor.AdvisorRequest()
                .setNodes(nodes)
                .setProfile(ContainerBalancerProfile.FAST))
        .get(0);

    long overriddenLeavingSource = 10L * OzoneConsts.GB;
    ContainerBalancerEstimation overridden = ContainerBalancerAdvisor.estimateDryRun(
            conf,
            new ContainerBalancerAdvisor.AdvisorRequest()
                .setNodes(nodes)
                .setProfile(ContainerBalancerProfile.FAST)
                .setMaxSizeLeavingSource(overriddenLeavingSource))
        .get(0);

    assertEquals(baseline.getBytesToMove(), overridden.getBytesToMove());
    assertEquals(500L * OzoneConsts.GB, baseline.getPerIterationBytes());
    assertEquals(14L * overriddenLeavingSource, overridden.getPerIterationBytes());

    assertThat(overridden.getEstimatedIterations()).isGreaterThan(baseline.getEstimatedIterations());
    assertThat(overridden.getEstimatedDurationMillis()).isGreaterThan(baseline.getEstimatedDurationMillis());
  }

  @Test
  void testEstimateDryRunReturnsFailedResultWhenMaxMoveOverrideConflictsWithFastPreset() {
    OzoneConfiguration conf = new OzoneConfiguration();
    List<ContainerBalancerEstimation> results = ContainerBalancerAdvisor.estimateDryRun(
        conf,
        new ContainerBalancerAdvisor.AdvisorRequest()
            .setNodes(buildCluster(70, 14, 14))
            .setAllProfiles(true)
            .setMaxSizeToMovePerIteration(70L * OzoneConsts.GB));

    assertEquals(3, results.size());
    assertThat(results.get(0).succeeded()).isTrue();
    assertThat(results.get(1).succeeded()).isTrue();
    assertThat(results.get(2).succeeded()).isFalse();
    assertEquals(ContainerBalancerProfile.FAST, results.get(2).getProfile());
    assertThat(results.get(2).getFailureMessage()).contains(
        "max-size-entering-target must be less than or equal to max-size-to-move-per-iteration.");
    assertEquals(100L * OzoneConsts.GB, results.get(2).getMaxSizeEnteringTarget());
    assertEquals(70L * OzoneConsts.GB, results.get(2).getMaxSizeToMovePerIteration());
  }

  @Test
  void testEstimateDryRunFailsWhenNodesNull() {
    OzoneConfiguration conf = new OzoneConfiguration();
    assertThrows(NullPointerException.class, () ->
        ContainerBalancerAdvisor.estimateDryRun(
            conf,
            new ContainerBalancerAdvisor.AdvisorRequest()));
  }

  @Test
  void testEstimateDryRunFailsWhenNodesEmpty() {
    OzoneConfiguration conf = new OzoneConfiguration();
    assertThrows(IllegalArgumentException.class, () ->
        ContainerBalancerAdvisor.estimateDryRun(
            conf,
            new ContainerBalancerAdvisor.AdvisorRequest().setNodes(new ArrayList<>())));
  }

  @Test
  void testEstimateDryRunFailsWhenClusterBalanced() {
    OzoneConfiguration conf = new OzoneConfiguration();
    List<DatanodeUsageInfoProto> balanced = new ArrayList<>();
    balanced.add(proto("dn-1", OzoneConsts.TB, (long) (0.70 * OzoneConsts.TB)));
    balanced.add(proto("dn-2", OzoneConsts.TB, (long) (0.70 * OzoneConsts.TB)));

    assertThrows(IllegalArgumentException.class, () ->
        ContainerBalancerAdvisor.estimateDryRun(
            conf,
            new ContainerBalancerAdvisor.AdvisorRequest().setNodes(balanced)));
  }

  @Test
  void testEstimateDryRunFailsWhenEnteringTargetTooSmall() {
    OzoneConfiguration conf = new OzoneConfiguration();
    List<ContainerBalancerEstimation> results = ContainerBalancerAdvisor.estimateDryRun(
        conf,
        new ContainerBalancerAdvisor.AdvisorRequest()
            .setNodes(buildCluster(70, 14, 14))
            .setProfile(ContainerBalancerProfile.SLOW)
            .setMaxSizeEnteringTarget(OzoneConsts.GB));
    assertEquals(1, results.size());
    assertThat(results.get(0).succeeded()).isFalse();
  }

  /**
   * Builds an imbalanced cluster for dry-run tests.
   *
   * <p>With default 10% threshold and {@code buildCluster(70, 14, 14)}:
   * <ul>
   *   <li>{@code under-*} — 5% used — 14 under-utilized targets</li>
   *   <li>{@code mid-*} — 40% used — 14 nodes near cluster average (neutral)</li>
   *   <li>{@code over-*} — 65% used — 42 over-utilized sources</li>
   * </ul>
   */
  private static List<DatanodeUsageInfoProto> buildCluster(
      int totalNodes, int underUtilNodeCount, int midUtilNodeCount) {
    List<DatanodeUsageInfoProto> nodes = new ArrayList<>(totalNodes);
    long capacity = OzoneConsts.TB;
    for (int i = 0; i < underUtilNodeCount; i++) {
      nodes.add(proto("under-" + i, capacity, (long) (capacity * 0.95)));
    }
    for (int i = 0; i < midUtilNodeCount; i++) {
      nodes.add(proto("mid-" + i, capacity, (long) (capacity * 0.60)));
    }
    int overUtil = totalNodes - underUtilNodeCount - midUtilNodeCount;
    for (int i = 0; i < overUtil; i++) {
      nodes.add(proto("over-" + i, capacity, (long) (capacity * 0.35)));
    }
    return nodes;
  }

  private static DatanodeUsageInfoProto proto(String hostname, long capacity, long remaining) {
    DatanodeDetails datanode = DatanodeDetails.newBuilder()
        .setHostName(hostname)
        .setIpAddress("127.0.0.1")
        .setUuid(UUID.randomUUID())
        .build();
    return DatanodeUsageInfoProto.newBuilder()
        .setNode(datanode.toProto(DEFAULT_VERSION.toProtoValue()))
        .setCapacity(capacity)
        .setRemaining(remaining)
        .setUsed(capacity - remaining)
        .build();
  }
}
