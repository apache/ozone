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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStorageTypeStatusInfoProto;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerStorageTypeMetrics.Outcome;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerStorageTypeMetrics.TypeMetrics;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsRecordImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests for {@link ContainerBalancerStorageTypeMetrics}.
 */
class TestContainerBalancerStorageTypeMetrics {
  private static final long GB = 1L << 30;
  private static final DatanodeID SOURCE = MockDatanodeDetails.randomDatanodeDetails().getID();
  private static final DatanodeID TARGET = MockDatanodeDetails.randomDatanodeDetails().getID();

  private final ContainerBalancerStorageTypeMetrics metrics = new ContainerBalancerStorageTypeMetrics();

  /** Every move result is counted the same way for the overall and the per storage type counters. */
  @ParameterizedTest
  @EnumSource(MoveManager.MoveResult.class)
  void testOverallAndStorageTypeCountersAgreeForEveryMoveResult(MoveManager.MoveResult result) {
    ContainerBalancerMetrics overall = ContainerBalancerMetrics.create();
    TypeMetrics ssd = metrics.forType(StorageType.SSD);
    ssd.initialize(0, 0);

    overall.incrementCurrentIterationContainerMoveMetric(result, 1);
    ssd.recordResult(Outcome.of(result), GB, ssd.generation());

    ContainerBalancerStorageTypeStatusInfoProto info = statusOf(StorageType.SSD);
    assertEquals(overall.getNumContainerMovesCompletedInLatestIteration(), info.getContainerMovesCompleted());
    assertEquals(overall.getNumContainerMovesFailedInLatestIteration(), info.getContainerMovesFailed());
    assertEquals(overall.getNumContainerMovesTimeoutInLatestIteration(), info.getContainerMovesTimeout());
    assertNotEquals(Outcome.NONE, Outcome.of(result), "every real move result must be counted somewhere");
  }

  @Test
  void testNullResultIsNotCounted() {
    TypeMetrics ssd = metrics.forType(StorageType.SSD);
    ssd.initialize(0, 0);
    assertEquals(Outcome.NONE, Outcome.of(null));
    assertFalse(ssd.recordResult(Outcome.NONE, GB, ssd.generation()));
    assertEquals(0, statusOf(StorageType.SSD).getContainerMovesFailed());
  }

  @Test
  void testResultsAreCountedUnderTheirOwnStorageType() {
    TypeMetrics ssd = metrics.forType(StorageType.SSD);
    TypeMetrics disk = metrics.forType(StorageType.DISK);
    ssd.initialize(2, 5 * GB);
    disk.initialize(3, 7 * GB);

    ssd.recordScheduled();
    ssd.recordTraffic(SOURCE, TARGET, 2 * GB);
    disk.recordScheduled();
    disk.recordScheduled();
    // The SSD result arrives while the DISK type is being balanced.
    assertTrue(disk.recordResult(Outcome.FAILED, 0, disk.generation()));
    assertTrue(ssd.recordResult(Outcome.COMPLETED, 2 * GB, ssd.generation()));

    ContainerBalancerStorageTypeStatusInfoProto ssdInfo = statusOf(StorageType.SSD);
    assertEquals(HddsProtos.StorageTypeProto.SSD, ssdInfo.getStorageType());
    assertEquals(1, ssdInfo.getContainerMovesScheduled());
    assertEquals(1, ssdInfo.getContainerMovesCompleted());
    assertEquals(0, ssdInfo.getContainerMovesFailed());
    assertEquals(2 * GB, ssdInfo.getSizeScheduledForMove());
    assertEquals(2 * GB, ssdInfo.getDataSizeMoved());
    assertEquals(2, ssdInfo.getNumDatanodesInvolved());
    assertEquals(2, ssdInfo.getNumDatanodesUnbalanced());
    assertEquals(5 * GB, ssdInfo.getDataSizeUnbalanced());

    ContainerBalancerStorageTypeStatusInfoProto diskInfo = statusOf(StorageType.DISK);
    assertEquals(2, diskInfo.getContainerMovesScheduled());
    assertEquals(0, diskInfo.getContainerMovesCompleted());
    assertEquals(1, diskInfo.getContainerMovesFailed());
    assertEquals(0, diskInfo.getDataSizeMoved());
    assertEquals(0, diskInfo.getNumDatanodesInvolved(), "no data was scheduled to move for DISK");
  }

  /** A result of an earlier iteration changes neither the new iteration nor the totals. */
  @Test
  void testResultFromEarlierIterationIsDropped() {
    TypeMetrics ssd = metrics.forType(StorageType.SSD);
    ssd.initialize(0, 0);
    long firstGeneration = ssd.generation();
    ssd.recordScheduled();
    metrics.finishIteration();

    metrics.startIteration();
    ssd.initialize(0, 0);
    assertNotEquals(firstGeneration, ssd.generation());
    ssd.recordScheduled();

    assertFalse(ssd.recordResult(Outcome.COMPLETED, GB, firstGeneration));
    assertEquals(0, statusOf(StorageType.SSD).getContainerMovesCompleted());
    assertEquals(0, statusOf(StorageType.SSD).getDataSizeMoved());
    assertTrue(ssd.recordResult(Outcome.COMPLETED, GB, ssd.generation()));
    assertEquals(1, statusOf(StorageType.SSD).getContainerMovesCompleted());
  }

  /** The result of a move after its iteration finished is dropped, so the finished iteration stays unchanged. */
  @Test
  void testResultAfterIterationFinishedIsDropped() {
    TypeMetrics ssd = metrics.forType(StorageType.SSD);
    ssd.initialize(0, 0);
    ssd.recordScheduled();
    assertTrue(ssd.recordResult(Outcome.TIMEOUT, 0, ssd.generation()));
    metrics.finishIteration();

    assertFalse(ssd.recordResult(Outcome.COMPLETED, GB, ssd.generation()));
    ContainerBalancerStorageTypeStatusInfoProto info = statusOf(StorageType.SSD);
    assertEquals(1, info.getContainerMovesTimeout());
    assertEquals(0, info.getContainerMovesCompleted());
    assertEquals(1, metric(StorageType.SSD, "NumContainerMovesTimeout"));
    assertEquals(0, metric(StorageType.SSD, "NumContainerMovesCompleted"));
  }

  /** A move that timed out is not counted a second time when it completes before the iteration is over. */
  @Test
  void testMoveIsCountedOnce() {
    TypeMetrics ssd = metrics.forType(StorageType.SSD);
    ssd.initialize(0, 0);
    ssd.recordScheduled();
    ContainerBalancerTask.MoveAccount account = new ContainerBalancerTask.MoveAccount(ssd, ssd.generation(), GB);

    assertTrue(account.count(Outcome.TIMEOUT));
    assertFalse(account.count(Outcome.COMPLETED));
    assertFalse(account.count(Outcome.TIMEOUT));

    ContainerBalancerStorageTypeStatusInfoProto info = statusOf(StorageType.SSD);
    assertEquals(1, info.getContainerMovesTimeout());
    assertEquals(0, info.getContainerMovesCompleted());
    assertEquals(0, info.getDataSizeMoved());
  }

  @Test
  void testMoveOfFinishedIterationIsNotCounted() {
    TypeMetrics ssd = metrics.forType(StorageType.SSD);
    ssd.initialize(0, 0);
    ContainerBalancerTask.MoveAccount account = new ContainerBalancerTask.MoveAccount(ssd, ssd.generation(), GB);
    metrics.finishIteration();

    assertFalse(account.count(Outcome.COMPLETED));
    assertEquals(0, statusOf(StorageType.SSD).getContainerMovesCompleted());
  }

  @Test
  void testFinishingTwiceAddsToTheTotalsOnce() {
    TypeMetrics ssd = metrics.forType(StorageType.SSD);
    ssd.initialize(0, 0);
    ssd.recordScheduled();
    ssd.recordResult(Outcome.COMPLETED, GB, ssd.generation());

    metrics.finishIteration();
    metrics.finishIteration();

    assertEquals(1, metric(StorageType.SSD, "NumContainerMovesScheduled"));
    assertEquals(1, metric(StorageType.SSD, "NumContainerMovesCompleted"));
    assertEquals(GB, metric(StorageType.SSD, "DataSizeMovedBytes"));
  }

  @Test
  void testTotalsGrowOverIterationsWhileLatestValuesReset() {
    TypeMetrics ssd = metrics.forType(StorageType.SSD);
    for (int i = 0; i < 2; i++) {
      metrics.startIteration();
      ssd.initialize(0, 0);
      ssd.recordScheduled();
      ssd.recordResult(Outcome.COMPLETED, GB, ssd.generation());
      metrics.finishIteration();
    }

    assertEquals(2, metric(StorageType.SSD, "NumContainerMovesCompleted"));
    assertEquals(2 * GB, metric(StorageType.SSD, "DataSizeMovedBytes"));
    assertEquals(1, metric(StorageType.SSD, "NumContainerMovesCompletedInLatestIteration"));
    assertEquals(GB, metric(StorageType.SSD, "DataSizeMovedBytesInLatestIteration"));

    metrics.startIteration();
    assertEquals(0, metric(StorageType.SSD, "NumContainerMovesCompletedInLatestIteration"));
    assertEquals(2, metric(StorageType.SSD, "NumContainerMovesCompleted"));
  }

  @Test
  void testUnknownStorageTypeIsReportedWithoutType() {
    TypeMetrics unknown = metrics.forType(null);
    unknown.initialize(1, GB);
    unknown.recordScheduled();

    List<ContainerBalancerStorageTypeStatusInfoProto> status = metrics.statusInfo();
    assertEquals(1, status.size());
    assertFalse(status.get(0).hasStorageType());
    assertEquals(1, status.get(0).getContainerMovesScheduled());
    assertEquals(ContainerBalancerStorageTypeMetrics.UNKNOWN, storageTypeTag(unknownRecord()));
  }

  @Test
  void testOnlyStorageTypesOfTheIterationAreReported() {
    metrics.forType(StorageType.SSD).initialize(0, 0);
    metrics.forType(StorageType.DISK).initialize(0, 0);
    assertEquals(2, metrics.statusInfo().size());

    metrics.startIteration();
    metrics.forType(StorageType.DISK).initialize(0, 0);
    assertThat(metrics.statusInfo()).extracting(ContainerBalancerStorageTypeStatusInfoProto::getStorageType)
        .containsExactly(HddsProtos.StorageTypeProto.DISK);
  }

  @Test
  void testNodeTrafficIsSummedPerNodeAndEveryNodeIsCountedOnce() {
    TypeMetrics ssd = metrics.forType(StorageType.SSD);
    ssd.initialize(0, 0);
    DatanodeID other = MockDatanodeDetails.randomDatanodeDetails().getID();
    ssd.recordTraffic(SOURCE, TARGET, GB);
    ssd.recordTraffic(SOURCE, other, 2 * GB);

    ContainerBalancerStorageTypeStatusInfoProto info = statusOf(StorageType.SSD);
    assertEquals(3 * GB, info.getSizeScheduledForMove());
    assertEquals(3, info.getNumDatanodesInvolved());
    Map<String, Long> leaving = new HashMap<>();
    info.getSizeLeavingNodesList().forEach(node -> leaving.put(node.getUuid(), node.getDataVolume()));
    assertEquals(Collections.singletonMap(SOURCE.toString(), 3 * GB), leaving);
    assertEquals(2, info.getSizeEnteringNodesCount());
  }

  /** The status of a finished iteration is a copy, so starting the next iteration does not change it. */
  @Test
  void testStatusInfoIsASnapshot() {
    TypeMetrics ssd = metrics.forType(StorageType.SSD);
    ssd.initialize(0, 0);
    ssd.recordScheduled();
    List<ContainerBalancerStorageTypeStatusInfoProto> snapshot = metrics.statusInfo();

    metrics.startIteration();
    ssd.initialize(0, 0);

    assertEquals(1, snapshot.get(0).getContainerMovesScheduled());
    assertEquals(0, statusOf(StorageType.SSD).getContainerMovesScheduled());
  }

  @Test
  void testMetricsAreTaggedWithTheStorageType() {
    metrics.forType(StorageType.SSD).initialize(0, 0);
    metrics.forType(StorageType.ARCHIVE).initialize(0, 0);

    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    metrics.getMetrics(collector, true);

    assertThat(collector.getRecords()).extracting(TestContainerBalancerStorageTypeMetrics::storageTypeTag)
        .containsExactly("ARCHIVE", "SSD");
    assertThat(collector.getRecords()).allSatisfy(record -> assertEquals(
        ContainerBalancerStorageTypeMetrics.NAME, record.name()));
  }

  private ContainerBalancerStorageTypeStatusInfoProto statusOf(StorageType type) {
    HddsProtos.StorageTypeProto proto = HddsProtos.StorageTypeProto.valueOf(type.name());
    List<ContainerBalancerStorageTypeStatusInfoProto> matching = metrics.statusInfo().stream()
        .filter(info -> info.hasStorageType() && info.getStorageType() == proto)
        .collect(Collectors.toList());
    assertEquals(1, matching.size(), "status of " + type);
    return matching.get(0);
  }

  private long metric(StorageType type, String name) {
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    metrics.getMetrics(collector, true);
    for (MetricsRecordImpl record : collector.getRecords()) {
      if (type.name().equals(storageTypeTag(record))) {
        for (AbstractMetric metric : record.metrics()) {
          if (metric.name().equals(name)) {
            return metric.value().longValue();
          }
        }
      }
    }
    throw new AssertionError("No metric " + name + " for " + type);
  }

  private MetricsRecordImpl unknownRecord() {
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    metrics.getMetrics(collector, true);
    return collector.getRecords().get(0);
  }

  private static String storageTypeTag(MetricsRecordImpl record) {
    for (MetricsTag tag : record.tags()) {
      if ("StorageType".equals(tag.name())) {
        return tag.value();
      }
    }
    throw new AssertionError("Record has no StorageType tag");
  }
}
