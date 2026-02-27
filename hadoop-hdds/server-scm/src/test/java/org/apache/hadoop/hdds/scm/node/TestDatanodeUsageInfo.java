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

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.assertj.core.api.Assertions.assertThat;

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
        1000L,  // capacity
        100L,   // scmUsed
        900L,   // remaining
        10L,    // committed
        5L,     // freeSpaceToSpare
        0L      // reserved
    );

    DatanodeUsageInfo info = new DatanodeUsageInfo(dn, stat);
    DatanodeUsageInfoProto proto = info.toProto(ClientVersion.CURRENT.serialize());

    assertThat(proto.hasFsCapacity()).isFalse();
    assertThat(proto.hasFsAvailable()).isFalse();

    assertThat(proto.getCapacity()).isEqualTo(1000L);
    assertThat(proto.getUsed()).isEqualTo(100L);
    assertThat(proto.getRemaining()).isEqualTo(900L);
  }

  @Test
  void testToProtoIncludesFilesystemFieldsWhenPresent() {
    DatanodeDetails dn = randomDatanodeDetails();
    SCMNodeStat stat = new SCMNodeStat(1000L, 100L, 900L, 10L, 5L, 0L);

    DatanodeUsageInfo info = new DatanodeUsageInfo(dn, stat);
    info.setFilesystemUsage(2000L, 1500L);

    DatanodeUsageInfoProto proto = info.toProto(ClientVersion.CURRENT.serialize());

    assertThat(proto.hasFsCapacity()).isTrue();
    assertThat(proto.hasFsAvailable()).isTrue();
    assertThat(proto.getFsCapacity()).isEqualTo(2000L);
    assertThat(proto.getFsAvailable()).isEqualTo(1500L);
  }
}

