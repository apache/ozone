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

package org.apache.hadoop.hdds.scm.node;

import static java.util.Collections.singletonMap;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeUsageInfoProto;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.ozone.ClientVersion;
import org.junit.jupiter.api.Test;

class TestDatanodeUsageInfo {

  @Test
  void testToProtoDoesNotIncludeFilesystemFieldsByDefault() {
    DatanodeDetails dn = randomDatanodeDetails();
    SCMNodeStat stat = new SCMNodeStat(
        singletonMap(StorageType.DEFAULT, 1000L),  // capacity
        singletonMap(StorageType.DEFAULT, 100L),   // scmUsed
        singletonMap(StorageType.DEFAULT, 900L),   // remaining
        singletonMap(StorageType.DEFAULT, 10L),    // committed
        singletonMap(StorageType.DEFAULT, 5L),     // freeSpaceToSpare
        singletonMap(StorageType.DEFAULT, 0L)      // reserved
    );

    DatanodeUsageInfo info = new DatanodeUsageInfo(dn, stat);
    DatanodeUsageInfoProto proto = info.toProto(ClientVersion.CURRENT_VERSION);

    assertThat(proto.hasFsCapacity()).isFalse();
    assertThat(proto.hasFsAvailable()).isFalse();

    assertThat(proto.getCapacity()).isEqualTo(1000L);
    assertThat(proto.getUsed()).isEqualTo(100L);
    assertThat(proto.getRemaining()).isEqualTo(900L);
  }

  @Test
  void testToProtoIncludesFilesystemFieldsWhenPresent() {
    DatanodeDetails dn = randomDatanodeDetails();
    SCMNodeStat stat = new SCMNodeStat(
        singletonMap(StorageType.DEFAULT, 1000L),
        singletonMap(StorageType.DEFAULT, 100L),
        singletonMap(StorageType.DEFAULT, 900L),
        singletonMap(StorageType.DEFAULT, 10L),
        singletonMap(StorageType.DEFAULT, 5L),
        singletonMap(StorageType.DEFAULT, 0L));

    DatanodeUsageInfo info = new DatanodeUsageInfo(dn, stat);
    info.setFilesystemUsage(2000L, 1500L);

    DatanodeUsageInfoProto proto = info.toProto(ClientVersion.CURRENT_VERSION);

    assertThat(proto.hasFsCapacity()).isTrue();
    assertThat(proto.hasFsAvailable()).isTrue();
    assertThat(proto.getFsCapacity()).isEqualTo(2000L);
    assertThat(proto.getFsAvailable()).isEqualTo(1500L);
  }

  /**
   * The balancer uses this to decide which tiers a node can take part in, so it
   * must report only storage types the node actually has capacity for.
   */
  @Test
  void testGetStorageTypesReportsOnlyTypesWithCapacity() {
    DatanodeUsageInfo info = new DatanodeUsageInfo(randomDatanodeDetails(),
        twoTierStat());

    assertThat(info.getStorageTypes())
        .containsExactlyInAnyOrder(StorageType.SSD, StorageType.DISK);
  }

  /**
   * Utilization must be measured per storage type, otherwise the balancer cannot
   * tell a node that is full on one tier from one that is full overall.
   */
  @Test
  void testCalculateUtilizationPerStorageType() {
    DatanodeUsageInfo info = new DatanodeUsageInfo(randomDatanodeDetails(),
        twoTierStat());

    // SSD: 100 capacity, 10 remaining -> 90% used.
    assertThat(info.calculateUtilization(StorageType.SSD))
        .isEqualTo(0.9, offset(0.0001));
    // DISK: 100 capacity, 80 remaining -> 20% used.
    assertThat(info.calculateUtilization(StorageType.DISK))
        .isEqualTo(0.2, offset(0.0001));
    // Whole node: 200 capacity, 90 remaining -> 55% used.
    assertThat(info.calculateUtilization(null))
        .isEqualTo(info.calculateUtilization());
    // A tier the node does not have reports no usage rather than failing.
    assertThat(info.calculateUtilization(StorageType.ARCHIVE)).isEqualTo(0.0);
  }

  /**
   * A node with one unevenly used tier: SSD nearly full, DISK mostly free.
   */
  private static SCMNodeStat twoTierStat() {
    Map<StorageType, Long> capacity = new HashMap<>();
    capacity.put(StorageType.SSD, 100L);
    capacity.put(StorageType.DISK, 100L);
    Map<StorageType, Long> used = new HashMap<>();
    used.put(StorageType.SSD, 90L);
    used.put(StorageType.DISK, 20L);
    Map<StorageType, Long> remaining = new HashMap<>();
    remaining.put(StorageType.SSD, 10L);
    remaining.put(StorageType.DISK, 80L);
    Map<StorageType, Long> zeros = new HashMap<>();
    zeros.put(StorageType.SSD, 0L);
    zeros.put(StorageType.DISK, 0L);
    return new SCMNodeStat(capacity, used, remaining, zeros, zeros, zeros);
  }
}

