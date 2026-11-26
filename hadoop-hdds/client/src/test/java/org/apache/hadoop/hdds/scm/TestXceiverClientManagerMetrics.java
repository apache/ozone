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

package org.apache.hadoop.hdds.scm;

import static org.apache.ozone.test.MetricsAsserts.assertCounter;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.Test;

/**
 * Tests the shared metrics lifecycle across XceiverClientManagers.
 */
class TestXceiverClientManagerMetrics {

  @Test
  void testMetricsRemainRegisteredAfterManagerClose() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    XceiverClientMetrics metrics = XceiverClientManager.getXceiverClientMetrics();
    long totalOps = metrics.getTotalOpCount();

    try (XceiverClientManager manager = new XceiverClientManager(conf)) {
      try (XceiverClientManager otherManager = new XceiverClientManager(conf)) {
        assertNotNull(DefaultMetricsSystem.instance().getSource(XceiverClientMetrics.SOURCE_NAME));
      }

      assertNotNull(DefaultMetricsSystem.instance().getSource(XceiverClientMetrics.SOURCE_NAME));
      assertSame(metrics, XceiverClientManager.getXceiverClientMetrics());
      metrics.incrPendingContainerOpsMetrics(ContainerProtos.Type.ReadChunk);
      metrics.decrPendingContainerOpsMetrics(ContainerProtos.Type.ReadChunk);
      assertCounter("TotalOps", totalOps + 1, getMetrics(XceiverClientMetrics.SOURCE_NAME));
    }

    try (XceiverClientManager manager = new XceiverClientManager(conf)) {
      assertSame(metrics, XceiverClientManager.getXceiverClientMetrics());
      assertNotNull(DefaultMetricsSystem.instance().getSource(XceiverClientMetrics.SOURCE_NAME));
      assertCounter("TotalOps", totalOps + 1, getMetrics(XceiverClientMetrics.SOURCE_NAME));
    }
  }
}
