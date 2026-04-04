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

package org.apache.hadoop.ozone.dn;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.BooleanSupplier;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Integration tests: For min free space as hard and soft limit.
 */
@Timeout(300)
public class TestDatanodeMinFreeSpaceIntegration {

  @Test
  public void storageReportsAtScmMatchSoftMinFreeSpaceFromConfig() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.unset(DatanodeConfiguration.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE);
    conf.setFloat(DatanodeConfiguration.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT, 0.03f);
    conf.setFloat(DatanodeConfiguration.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_HARD_LIMIT_PERCENT, 0.015f);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 2, SECONDS);

    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);

    try (MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .build()) {
      cluster.waitForClusterToBeReady();
      cluster.waitTobeOutOfSafeMode();

      HddsDatanodeService dnService = cluster.getHddsDatanodes().get(0);
      DatanodeDetails dn = dnService.getDatanodeDetails();

      StorageContainerManager scm = cluster.getStorageContainerManager();
      NodeManager nm = scm.getScmNodeManager();

      BooleanSupplier softSpareVisibleAtScm =
          () -> storageReportsMatchSoftMinFree(nm, dn, dnConf);
      GenericTestUtils.waitFor(softSpareVisibleAtScm, 500, 120_000);

      DatanodeInfo info = nm.getDatanodeInfo(dn);
      assertNotNull(info);
      assertTrue(info.getStorageReports().size() > 0);

      for (StorageReportProto report : info.getStorageReports()) {
        if (report.getFailed()) {
          continue;
        }
        long capacity = report.getCapacity();
        assertTrue(capacity > 0, "data volume should have positive capacity");

        long expectedSoft = dnConf.getMinFreeSpace(capacity);
        long expectedHard = dnConf.getHardLimitMinFreeSpace(capacity);
        long expectedBand = dnConf.getSoftBandMinFreeSpaceWidth(capacity);

        assertEquals(expectedSoft, report.getFreeSpaceToSpare(),
            "freeSpaceToSpare in SCM storage report should match soft min-free for capacity");
        assertThat(expectedSoft).isGreaterThanOrEqualTo(expectedHard);
        assertThat(expectedBand).isGreaterThan(0L);
        assertEquals(expectedBand, expectedSoft - expectedHard);
      }
    }
  }

  /**
   * SCM has caught up with DN heartbeats: every non-failed data report's {@code freeSpaceToSpare}
   * equals the configured soft min-free for that volume capacity.
   */
  private static boolean storageReportsMatchSoftMinFree(
      NodeManager nm, DatanodeDetails dn, DatanodeConfiguration dnConf) {
    DatanodeInfo info = nm.getDatanodeInfo(dn);
    if (info == null || info.getStorageReports().isEmpty()) {
      return false;
    }
    boolean anyDataVolume = false;
    for (StorageReportProto r : info.getStorageReports()) {
      if (r.getFailed() || r.getCapacity() <= 0) {
        continue;
      }
      anyDataVolume = true;
      long expectedSoft = dnConf.getMinFreeSpace(r.getCapacity());
      if (expectedSoft != r.getFreeSpaceToSpare()) {
        return false;
      }
    }
    return anyDataVolume;
  }
}
