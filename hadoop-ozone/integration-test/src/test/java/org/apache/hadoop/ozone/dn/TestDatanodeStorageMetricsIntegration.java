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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION;
import static org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory.Conf.configKeyForClassName;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.ozone.test.MetricsAsserts.getDoubleGauge;
import static org.apache.ozone.test.MetricsAsserts.getLongGauge;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

import java.util.HashMap;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.DUFactory;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.volume.DatanodeStorageMetrics;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Integration tests for {@link DatanodeStorageMetrics}.
 *
 * <p>Verifies that the live registered metrics source on a real DataNode
 * reflects actual storage usage: capacity is positive, used space increases
 * after writing data, and the percentage arithmetic holds.
 */
@Timeout(300)
public class TestDatanodeStorageMetricsIntegration {

  private MiniOzoneCluster cluster;

  @BeforeEach
  void startCluster() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CONTAINER_SIZE, "1GB");
    conf.setBoolean(HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);
    conf.setClass(configKeyForClassName(), DUFactory.class, SpaceUsageCheckFactory.class);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(ONE, 30000);
  }

  @AfterEach
  void stopCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  void storageMetricsReflectWrittenData() throws Exception {
    // Baseline before any write.
    long baselineUsed = getLongGauge("OzoneUsed", storageMetrics());

    // Write a key to generate real used space.
    try (OzoneClient client = cluster.newClient()) {
      client.getObjectStore().createVolume("vol");
      client.getObjectStore().getVolume("vol").createBucket("bucket");
      OzoneOutputStream key = client.getObjectStore().getVolume("vol")
          .getBucket("bucket")
          .createKey("key", 4096,
              RatisReplicationConfig.getInstance(ONE), new HashMap<>());
      key.write(new byte[4096]);
      key.close();
    }

    // Force DU refresh so the in-memory usage cache reflects the write.
    MutableVolumeSet volumeSet = cluster.getHddsDatanodes().get(0)
        .getDatanodeStateMachine().getContainer().getVolumeSet();
    volumeSet.getVolumesList().get(0).getVolumeUsage().refreshNow();

    // Wait until OzoneUsed is reported as greater than the baseline.
    GenericTestUtils.waitFor(
        () -> getLongGauge("OzoneUsed", storageMetrics()) > baselineUsed,
        500, 10_000);

    // Read all three gauges from one storageMetrics() call so they come from
    // the same getStorageReport() iteration and are mutually consistent.
    MetricsRecordBuilder rb = storageMetrics();
    long capacity = getLongGauge("OzoneCapacity", rb);
    long used = getLongGauge("OzoneUsed", rb);
    double usedPercentage = getDoubleGauge("OzoneUsedPercentage", rb);

    assertThat(capacity).isGreaterThan(0L);
    assertThat(used).isGreaterThan(baselineUsed);
    assertThat(usedPercentage).isBetween(0.0, 100.0);

    // Arithmetic invariant: usedPercentage == 100 * used / capacity.
    assertThat(usedPercentage).isCloseTo(100.0 * used / capacity, offset(0.001));
  }

  /**
   * Returns a fresh snapshot of the live {@link DatanodeStorageMetrics} source.
   * Each call re-reads the underlying storage reports — do not mix values
   * from different calls when checking invariants across gauges.
   */
  private MetricsRecordBuilder storageMetrics() {
    // In mini-cluster mode the source name is made unique per datanode (to keep
    // metrics registration and unregistration symmetric and avoid the
    // shared-JVM source leak), so look it up by the per-datanode name.
    MutableVolumeSet volumeSet = cluster.getHddsDatanodes().get(0)
        .getDatanodeStateMachine().getContainer().getVolumeSet();
    return getMetrics(DatanodeStorageMetrics.SOURCE_NAME + '-' + volumeSet.getDatanodeUuid());
  }
}
