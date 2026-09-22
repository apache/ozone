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

package org.apache.hadoop.ozone.container.placement;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumMap;
import java.util.Map;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.junit.jupiter.api.Test;

/**
 * Tests that test Metrics that support placement.
 */
public class TestDatanodeMetrics {
  @Test
  public void testSCMNodeMetric() {
    SCMNodeStat stat = createSCMNodeStat(100L, 10L, 90L, 0, 80, 0);
    assertEquals((long) stat.getCapacity().get(), 100L);
    assertEquals(10L, (long) stat.getScmUsed().get());
    assertEquals(90L, (long) stat.getRemaining().get());
    SCMNodeMetric metric = new SCMNodeMetric(stat);

    SCMNodeStat newStat = createSCMNodeStat(100L, 10L, 90L, 0, 80, 0);
    assertEquals(100L, (long) stat.getCapacity().get());
    assertEquals(10L, (long) stat.getScmUsed().get());
    assertEquals(90L, (long) stat.getRemaining().get());

    SCMNodeMetric newMetric = new SCMNodeMetric(newStat);
    assertTrue(metric.isEqual(newMetric.get()));

    newMetric.add(stat);
    assertTrue(newMetric.isGreater(metric.get()));

    SCMNodeMetric zeroMetric = new SCMNodeMetric(new SCMNodeStat());
    // Assert we can handle zero capacity.
    assertTrue(metric.isGreater(zeroMetric.get()));

    // Another case when nodes have similar weight
    SCMNodeStat stat1 = createSCMNodeStat(10000000L, 50L, 9999950L, 0, 100000, 0);
    SCMNodeStat stat2 = createSCMNodeStat(10000000L, 51L, 9999949L, 0, 100000, 0);
    assertTrue(new SCMNodeMetric(stat2).isGreater(stat1));
  }

  /**
   * Test if the StorageType-aware comparisons rank nodes by the requested
   * type.
   */
  @Test
  public void testStorageTypeAwareComparison() {
    // nodeA: DISK heavily used (0.8), SSD lightly used (0.1)
    SCMNodeStat nodeA = capacityUsedByType(100L, 80L, 100L, 10L);
    // nodeB: DISK lightly used (0.1), SSD heavily used (0.8)
    SCMNodeStat nodeB = capacityUsedByType(100L, 10L, 100L, 80L);
    SCMNodeMetric metricA = new SCMNodeMetric(nodeA);

    // By DISK, nodeA is the greater (more used) node.
    assertThat(metricA.isGreater(nodeB, StorageType.DISK)).isTrue();
    assertThat(metricA.isLess(nodeB, StorageType.DISK)).isFalse();

    // By SSD, the ordering flips.
    assertThat(metricA.isGreater(nodeB, StorageType.SSD)).isFalse();
    assertThat(metricA.isLess(nodeB, StorageType.SSD)).isTrue();

    // Same per-type weight is equal for that type.
    SCMNodeStat nodeC = capacityUsedByType(100L, 80L, 100L, 10L);
    assertThat(metricA.isEqual(nodeC, StorageType.DISK)).isTrue();
    assertThat(metricA.isEqual(nodeB, StorageType.DISK)).isFalse();
  }

  /**
   * Test if per-StorageType add and subtract update both the per-type value
   * and the aggregate, and equals() distinguishes equal totals that have a
   * different per-storage-type split.
   */
  @Test
  public void testReservedArithmeticPerTypeAndEquals() {
    Map<StorageType, Long> reserved = new EnumMap<>(StorageType.class);
    reserved.put(StorageType.DISK, 300L);
    reserved.put(StorageType.SSD, 150L);
    SCMNodeStat stat = new SCMNodeStat(zeroMap(), zeroMap(), zeroMap(), zeroMap(),
        zeroMap(), reserved);
    assertThat((long) stat.getReserved(StorageType.DISK).get()).isEqualTo(300L);
    assertThat((long) stat.getReserved(StorageType.SSD).get()).isEqualTo(150L);
    assertThat((long) stat.getReserved().get()).isEqualTo(450L);

    stat.add(0, 0, 0, 0, 0, 50L, StorageType.DISK);
    assertThat((long) stat.getReserved(StorageType.DISK).get()).isEqualTo(350L);
    assertThat((long) stat.getReserved().get()).isEqualTo(500L);
    stat.subtract(new SCMNodeStat(zeroMap(), zeroMap(), zeroMap(), zeroMap(),
        zeroMap(), singletonMap(StorageType.DISK, 50L)));
    assertThat((long) stat.getReserved(StorageType.DISK).get()).isEqualTo(300L);
    assertThat((long) stat.getReserved().get()).isEqualTo(450L);

    // Equal totals, different per-StorageType distribution -> not equal.
    SCMNodeStat diskOnly = new SCMNodeStat(singletonMap(StorageType.DISK, 100L),
        zeroMap(), zeroMap(), zeroMap(), zeroMap(), zeroMap());
    SCMNodeStat ssdOnly = new SCMNodeStat(singletonMap(StorageType.SSD, 100L),
        zeroMap(), zeroMap(), zeroMap(), zeroMap(), zeroMap());
    assertThat((long) diskOnly.getCapacity().get()).isEqualTo((long) ssdOnly.getCapacity().get());
    assertThat(diskOnly).isNotEqualTo(ssdOnly);
  }

  /**
   * Test if a null StorageType falls back to the aggregate and set(NodeStat)
   * copies both the totals and the per-StorageType breakdown.
   */
  @Test
  public void testNullFallbackAndSetRoundTrip() {
    SCMNodeStat stat = capacityUsedByType(200L, 50L, 100L, 25L);
    // null StorageType returns the aggregate.
    assertThat(stat.getCapacity(null).get()).isEqualTo(stat.getCapacity().get());
    assertThat((long) stat.getCapacity(null).get()).isEqualTo(300L);
    assertThat(stat.getScmUsed(null).get()).isEqualTo(stat.getScmUsed().get());

    SCMNodeStat target = new SCMNodeStat();
    target.set(stat);
    assertThat(target).isEqualTo(stat);
    assertThat((long) target.getCapacity(StorageType.DISK).get()).isEqualTo(200L);
    assertThat((long) target.getCapacity(StorageType.SSD).get()).isEqualTo(100L);
    assertThat((long) target.getScmUsed(StorageType.SSD).get()).isEqualTo(25L);
  }

  /**
   * Test if add(NodeStat) accumulates both the per-StorageType maps and the
   * aggregate totals when the operands each carry more than one StorageType.
   */
  @Test
  public void testAddMultiTypeAccumulation() {
    SCMNodeStat target = new SCMNodeStat();
    target.add(capacityUsedByType(100L, 20L, 200L, 30L));
    target.add(capacityUsedByType(300L, 40L, 400L, 50L));

    assertThat((long) target.getCapacity(StorageType.DISK).get()).isEqualTo(400L);
    assertThat((long) target.getCapacity(StorageType.SSD).get()).isEqualTo(600L);
    assertThat((long) target.getScmUsed(StorageType.DISK).get()).isEqualTo(60L);
    assertThat((long) target.getScmUsed(StorageType.SSD).get()).isEqualTo(80L);
    // Aggregate totals are the sum across all storage types.
    assertThat((long) target.getCapacity().get()).isEqualTo(1000L);
    assertThat((long) target.getScmUsed().get()).isEqualTo(140L);
  }

  /**
   * Test if the copy constructor deep-copies the per-StorageType maps, so
   * mutating the original leaves the copy unchanged.
   */
  @Test
  public void testCopyConstructorIsDeepCopy() {
    SCMNodeStat original = capacityUsedByType(100L, 20L, 200L, 30L);
    SCMNodeStat copy = new SCMNodeStat(original);

    original.add(50L, 10L, 0, 0, 0, 0, StorageType.DISK);

    assertThat((long) copy.getCapacity(StorageType.DISK).get()).isEqualTo(100L);
    assertThat((long) copy.getScmUsed(StorageType.DISK).get()).isEqualTo(20L);
    assertThat((long) copy.getCapacity().get()).isEqualTo(300L);
    assertThat((long) copy.getScmUsed().get()).isEqualTo(50L);
    // The original reflects the mutation.
    assertThat((long) original.getCapacity(StorageType.DISK).get()).isEqualTo(150L);
  }

  private static SCMNodeStat createSCMNodeStat(long capacity, long used, long remaining,
      long committed, long freeSpaceToSpare, long reserved) {
    return new SCMNodeStat(
        singletonMap(StorageType.DEFAULT, capacity),
        singletonMap(StorageType.DEFAULT, used),
        singletonMap(StorageType.DEFAULT, remaining),
        singletonMap(StorageType.DEFAULT, committed),
        singletonMap(StorageType.DEFAULT, freeSpaceToSpare),
        singletonMap(StorageType.DEFAULT, reserved));
  }

  private static SCMNodeStat capacityUsedByType(long diskCapacity, long diskUsed,
      long ssdCapacity, long ssdUsed) {
    Map<StorageType, Long> capacity = new EnumMap<>(StorageType.class);
    capacity.put(StorageType.DISK, diskCapacity);
    capacity.put(StorageType.SSD, ssdCapacity);
    Map<StorageType, Long> used = new EnumMap<>(StorageType.class);
    used.put(StorageType.DISK, diskUsed);
    used.put(StorageType.SSD, ssdUsed);
    return new SCMNodeStat(capacity, used, zeroMap(), zeroMap(), zeroMap(), zeroMap());
  }

  private static Map<StorageType, Long> zeroMap() {
    return new EnumMap<>(StorageType.class);
  }
}
