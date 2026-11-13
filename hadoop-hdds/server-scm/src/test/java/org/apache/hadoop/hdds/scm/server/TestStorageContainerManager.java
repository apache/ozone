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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.util.MetricUtil;
import org.apache.ozone.test.HddsWhiteboxTestUtils;
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
          getMetrics(MetricUtil.getMetricsSourceName(SCMFullContainerReportLeaseMetrics.class.getSimpleName(),
              scm.getScmId())));
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
    String source = MetricUtil.getMetricsSourceName(SCMFullContainerReportLeaseMetrics.class.getSimpleName(),
        scm.getScmId());
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

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("unchecked")
  void testRemovalDoesNotLeaveGrantOrBlockConcurrentRegistration(boolean registerDuringRemoval, @TempDir File tempDir)
      throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(tempDir);
    conf.setInt(OZONE_SCM_MAX_FULL_CONTAINER_REPORT_LEASES, 1);
    StorageContainerManager scm = HddsTestUtils.getScm(conf);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch callbackStarted = new CountDownLatch(1);
    CountDownLatch finishRemoval = new CountDownLatch(1);
    try {
      SCMNodeManager nodes = (SCMNodeManager) scm.getScmNodeManager();
      DatanodeDetails datanode = MockDatanodeDetails.randomDatanodeDetails();
      nodes.register(datanode, NodeReportProto.getDefaultInstance(), PipelineReportsProto.getDefaultInstance(),
          defaultVersionProto());
      SCMHeartbeatRequestProto request = SCMHeartbeatRequestProto.newBuilder()
          .setDatanodeDetails(datanode.getProtoBufMessage()).setRequestFullContainerReportLease(true).build();
      assertTrue(scm.getDatanodeProtocolServer().sendHeartbeat(request).hasFullContainerReportLease());
      Consumer<DatanodeDetails> callback = (Consumer<DatanodeDetails>)
          HddsWhiteboxTestUtils.getInternalState(nodes, "datanodeRemovedCallback");
      nodes.setDatanodeRemovedCallback(removed -> {
        if (!registerDuringRemoval) {
          callback.accept(removed);
        }
        callbackStarted.countDown();
        try {
          assertTrue(finishRemoval.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          throw new AssertionError(ex);
        }
        if (registerDuringRemoval) {
          callback.accept(removed);
        }
      });
      Future<?> removal = executor.submit(() -> {
        datanode.setPersistedOpState(NodeOperationalState.DECOMMISSIONED);
        nodes.removeNode(datanode);
        return null;
      });
      assertTrue(callbackStarted.await(10, TimeUnit.SECONDS));
      assertFalse(nodes.isNodeRegistered(datanode), "Node state must be removed before its callback");
      if (!registerDuringRemoval) {
        assertFalse(scm.getDatanodeProtocolServer().sendHeartbeat(request).hasFullContainerReportLease());
        finishRemoval.countDown();
        removal.get(10, TimeUnit.SECONDS);
        assertFalse(scm.getDatanodeProtocolServer().sendHeartbeat(request).hasFullContainerReportLease());
      }
      register(scm, datanode, true);
      if (registerDuringRemoval) {
        finishRemoval.countDown();
        removal.get(10, TimeUnit.SECONDS);
      }
      assertTrue(nodes.isNodeRegistered(datanode));
      assertTrue(scm.getDatanodeProtocolServer().sendHeartbeat(request).hasFullContainerReportLease());
    } finally {
      finishRemoval.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      scm.stop();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testDelayedRegistrationCannotUndoCompletedRemoval(boolean deferred, @TempDir File tempDir) throws Exception {
    StorageContainerManager scm = HddsTestUtils.getScm(SCMTestUtils.getConf(tempDir));
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch registered = new CountDownLatch(1);
    CountDownLatch finishRegistration = new CountDownLatch(1);
    try {
      SCMNodeManager nodes = spy((SCMNodeManager) scm.getScmNodeManager());
      HddsWhiteboxTestUtils.setInternalState(scm, "scmNodeManager", nodes);
      AtomicBoolean pause = new AtomicBoolean(true);
      doAnswer(invocation -> {
        Object response = invocation.callRealMethod();
        if (pause.getAndSet(false)) {
          registered.countDown();
          assertTrue(finishRegistration.await(10, TimeUnit.SECONDS));
        }
        return response;
      }).when(nodes).register(any(), any(), any(), any());
      DatanodeDetails datanode = MockDatanodeDetails.randomDatanodeDetails();
      Future<?> registration = executor.submit(() -> register(scm, datanode, deferred));
      assertTrue(registered.await(10, TimeUnit.SECONDS));
      datanode.setPersistedOpState(NodeOperationalState.DECOMMISSIONED);
      nodes.removeNode(datanode);
      assertFalse(nodes.isNodeRegistered(datanode));

      finishRegistration.countDown();

      ExecutionException failure = assertThrows(ExecutionException.class,
          () -> registration.get(10, TimeUnit.SECONDS));
      assertTrue(failure.getCause() instanceof IOException);
      SCMHeartbeatRequestProto request = SCMHeartbeatRequestProto.newBuilder()
          .setDatanodeDetails(datanode.getProtoBufMessage()).setRequestFullContainerReportLease(true).build();
      assertFalse(scm.getDatanodeProtocolServer().sendHeartbeat(request).hasFullContainerReportLease());
      register(scm, datanode, deferred);
      assertTrue(scm.getDatanodeProtocolServer().sendHeartbeat(request).hasFullContainerReportLease());
    } finally {
      finishRegistration.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      scm.stop();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("unchecked")
  void testRegistrationPresenceCheckIsAtomicWithRemovalTracking(boolean deferred, @TempDir File tempDir)
      throws Exception {
    StorageContainerManager scm = HddsTestUtils.getScm(SCMTestUtils.getConf(tempDir));
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch checked = new CountDownLatch(1);
    CountDownLatch removed = new CountDownLatch(1);
    CountDownLatch finishCheck = new CountDownLatch(1);
    try {
      SCMNodeManager nodes = spy((SCMNodeManager) scm.getScmNodeManager());
      HddsWhiteboxTestUtils.setInternalState(scm, "scmNodeManager", nodes);
      AtomicReference<Thread> registeringThread = new AtomicReference<>();
      AtomicBoolean nodeUpdated = new AtomicBoolean();
      AtomicBoolean pause = new AtomicBoolean(true);
      doAnswer(invocation -> {
        Object response = invocation.callRealMethod();
        nodeUpdated.set(true);
        return response;
      }).when(nodes).register(any(), any(), any(), any());
      doAnswer(invocation -> {
        Object result = invocation.callRealMethod();
        if (nodeUpdated.get() && Thread.currentThread() == registeringThread.get() && pause.getAndSet(false)) {
          checked.countDown();
          assertTrue(finishCheck.await(10, TimeUnit.SECONDS));
        }
        return result;
      }).when(nodes).isNodeRegistered(any());
      Consumer<DatanodeDetails> callback = (Consumer<DatanodeDetails>)
          HddsWhiteboxTestUtils.getInternalState(nodes, "datanodeRemovedCallback");
      nodes.setDatanodeRemovedCallback(datanode -> {
        removed.countDown();
        callback.accept(datanode);
      });
      DatanodeDetails datanode = MockDatanodeDetails.randomDatanodeDetails();
      Future<?> registration = executor.submit(() -> {
        registeringThread.set(Thread.currentThread());
        return register(scm, datanode, deferred);
      });
      assertTrue(checked.await(10, TimeUnit.SECONDS));
      Future<?> removal = executor.submit(() -> {
        datanode.setPersistedOpState(NodeOperationalState.DECOMMISSIONED);
        nodes.removeNode(datanode);
        return null;
      });
      assertTrue(removed.await(10, TimeUnit.SECONDS));
      finishCheck.countDown();
      registration.get(10, TimeUnit.SECONDS);
      removal.get(10, TimeUnit.SECONDS);

      assertFalse(scm.getDatanodeProtocolServer().sendHeartbeat(SCMHeartbeatRequestProto.newBuilder()
          .setDatanodeDetails(datanode.getProtoBufMessage()).setRequestFullContainerReportLease(true).build())
          .hasFullContainerReportLease());
    } finally {
      finishCheck.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      scm.stop();
    }
  }

  private static SCMRegisteredResponseProto register(StorageContainerManager scm, DatanodeDetails datanode,
      boolean deferred) throws IOException {
    return scm.getDatanodeProtocolServer().register(datanode.getExtendedProtoBufMessage(),
        NodeReportProto.getDefaultInstance(),
        ContainerReportsProto.newBuilder().setFullContainerReportDeferred(deferred).build(),
        PipelineReportsProto.getDefaultInstance(), defaultVersionProto());
  }

  @Test
  void fcrLeaseMetricsFollowScmLifecycle(@TempDir File tempDir) throws Exception {
    StorageContainerManager scm = HddsTestUtils.getScm(SCMTestUtils.getConf(tempDir));
    String source = MetricUtil.getMetricsSourceName(SCMFullContainerReportLeaseMetrics.class.getSimpleName(),
        scm.getScmId());
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
