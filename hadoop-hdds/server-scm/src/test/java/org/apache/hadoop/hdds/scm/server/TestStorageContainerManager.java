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

package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_FULL_CONTAINER_REPORT_RATE_LIMIT_ENABLED;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_MAX_FULL_CONTAINER_REPORT_LEASES;
import static org.apache.hadoop.ozone.container.upgrade.UpgradeUtils.defaultVersionProto;
import static org.apache.ozone.test.MetricsAsserts.assertGauge;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestStorageContainerManager {

  @ParameterizedTest
  @ValueSource(strings = {"true", "false", "", "flase", "0", "truefalse"})
  void fcrRateLimitConfiguredAtStartup(String value, @TempDir File tempDir) throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(tempDir);
    conf.set(OZONE_SCM_FULL_CONTAINER_REPORT_RATE_LIMIT_ENABLED, value);
    conf.setInt(OZONE_SCM_MAX_FULL_CONTAINER_REPORT_LEASES, 1);
    StorageContainerManager scm = HddsTestUtils.getScm(conf);
    try {
      boolean enabled = !"false".equals(value);
      assertGauge("FCRRateLimitEnabled", enabled ? 1 : 0,
          getMetrics(SCMFullContainerReportLeaseMetrics.class.getSimpleName()));
      SCMHeartbeatRequestProto request = SCMHeartbeatRequestProto.newBuilder()
          .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails().getProtoBufMessage())
          .setRequestFullContainerReportLease(true).build();

      assertTrue(scm.getDatanodeProtocolServer().sendHeartbeat(request).hasFullContainerReportLease());
      assertEquals(!enabled, scm.getDatanodeProtocolServer().sendHeartbeat(request.toBuilder()
          .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails().getProtoBufMessage()).build())
          .hasFullContainerReportLease());
    } finally {
      scm.stop();
    }
  }

  @Test
  void reconfigureFcrRateLimitValidatesValuesAndRestoresDefault(@TempDir File tempDir) throws Exception {
    StorageContainerManager scm = HddsTestUtils.getScm(SCMTestUtils.getConf(tempDir));
    String source = SCMFullContainerReportLeaseMetrics.class.getSimpleName();
    ReconfigurationHandler handler = scm.getReconfigurationHandler();
    try {
      assertGauge("FCRRateLimitEnabled", 1, getMetrics(source));
      for (boolean enabled : new boolean[] {true, false}) {
        handler.reconfigureProperty(OZONE_SCM_FULL_CONTAINER_REPORT_RATE_LIMIT_ENABLED, Boolean.toString(enabled));
        for (String invalid : new String[] {"", "flase", "0", "truefalse"}) {
          assertThrows(ReconfigurationException.class,
              () -> handler.reconfigureProperty(OZONE_SCM_FULL_CONTAINER_REPORT_RATE_LIMIT_ENABLED, invalid));
          assertGauge("FCRRateLimitEnabled", enabled ? 1 : 0, getMetrics(source));
          assertEquals(Boolean.toString(enabled),
              scm.getConfiguration().get(OZONE_SCM_FULL_CONTAINER_REPORT_RATE_LIMIT_ENABLED));
        }
      }
      handler.reconfigureProperty(OZONE_SCM_FULL_CONTAINER_REPORT_RATE_LIMIT_ENABLED, " FALSE ");
      assertGauge("FCRRateLimitEnabled", 0, getMetrics(source));
      handler.reconfigureProperty(OZONE_SCM_FULL_CONTAINER_REPORT_RATE_LIMIT_ENABLED, null);
      assertGauge("FCRRateLimitEnabled", 1, getMetrics(source));
      assertTrue(scm.getConfiguration().getBoolean(OZONE_SCM_FULL_CONTAINER_REPORT_RATE_LIMIT_ENABLED, true));
    } finally {
      scm.stop();
    }
  }

  @Test
  void removingDatanodeReleasesItsFcrGrant(@TempDir File tempDir) throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(tempDir);
    conf.setInt(OZONE_SCM_MAX_FULL_CONTAINER_REPORT_LEASES, 1);
    StorageContainerManager scm = HddsTestUtils.getScm(conf);
    try {
      NodeManager nodes = scm.getScmNodeManager();
      DatanodeDetails datanode = MockDatanodeDetails.randomDatanodeDetails();
      nodes.register(datanode, NodeReportProto.getDefaultInstance(), PipelineReportsProto.getDefaultInstance(),
          defaultVersionProto());
      SCMHeartbeatRequestProto request = SCMHeartbeatRequestProto.newBuilder()
          .setDatanodeDetails(datanode.getProtoBufMessage()).setRequestFullContainerReportLease(true).build();
      assertTrue(scm.getDatanodeProtocolServer().sendHeartbeat(request).hasFullContainerReportLease());
      SCMHeartbeatRequestProto nextRequest = request.toBuilder()
          .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails().getProtoBufMessage()).build();
      assertFalse(scm.getDatanodeProtocolServer().sendHeartbeat(nextRequest).hasFullContainerReportLease());

      nodes.setNodeOperationalState(datanode, NodeOperationalState.DECOMMISSIONED);
      datanode.setPersistedOpState(NodeOperationalState.DECOMMISSIONED);
      nodes.removeNode(datanode);

      assertFalse(nodes.isNodeRegistered(datanode));
      assertTrue(scm.getDatanodeProtocolServer().sendHeartbeat(nextRequest).hasFullContainerReportLease());
    } finally {
      scm.stop();
    }
  }

  @Test
  void fcrLeaseMetricsFollowScmLifecycle(@TempDir File tempDir) throws Exception {
    StorageContainerManager scm = HddsTestUtils.getScm(SCMTestUtils.getConf(tempDir));
    String source = SCMFullContainerReportLeaseMetrics.class.getSimpleName();
    try {
      scm.getDatanodeProtocolServer().stop();

      assertNotNull(DefaultMetricsSystem.instance().getSource(source));
    } finally {
      scm.stop();
    }
    assertNull(DefaultMetricsSystem.instance().getSource(source));
  }

  @Test
  void defaultMappingKeepsCachedBehavior() {
    DNSToSwitchMapping mapping =
        StorageContainerManager.createDNSToSwitchMapping(
            new OzoneConfiguration());

    assertInstanceOf(ScriptBasedMapping.class, mapping);
    assertInstanceOf(CachedDNSToSwitchMapping.class, mapping);
  }

  @Test
  void configuredMappingIsUsedDirectly() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setClass(ScmConfigKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        StaticMapping.class, DNSToSwitchMapping.class);

    DNSToSwitchMapping mapping =
        StorageContainerManager.createDNSToSwitchMapping(conf);

    assertInstanceOf(StaticMapping.class, mapping);
    assertFalse(mapping instanceof CachedDNSToSwitchMapping,
        "Configured mapping should not be wrapped in CachedDNSToSwitchMapping");
  }
}
