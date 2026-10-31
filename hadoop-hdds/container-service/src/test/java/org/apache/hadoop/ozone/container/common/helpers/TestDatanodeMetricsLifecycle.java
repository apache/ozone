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

package org.apache.hadoop.ozone.container.common.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.CSMMetrics;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.common.volume.BackgroundVolumeScannerMetrics;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.StorageVolumeChecker;
import org.apache.hadoop.ozone.container.common.volume.VolumeHealthMetrics;
import org.apache.hadoop.ozone.container.common.volume.VolumeIOStats;
import org.apache.hadoop.ozone.container.common.volume.VolumeInfoMetrics;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionMetrics;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerDataScannerMetrics;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerMetadataScannerMetrics;
import org.apache.hadoop.ozone.container.ozoneimpl.OnDemandScannerMetrics;
import org.apache.hadoop.ozone.container.replication.MeasuredReplicator;
import org.apache.hadoop.ozone.container.replication.ReplicationSupervisor;
import org.apache.hadoop.ozone.container.replication.ReplicationSupervisorMetrics;
import org.apache.hadoop.util.Timer;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.util.function.CheckedConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests isolation and cleanup of metrics owned by separate datanodes. */
class TestDatanodeMetricsLifecycle {

  @Test
  void testIndependentSources() throws Exception {
    boolean previous = DefaultMetricsSystem.inMiniClusterMode();
    DefaultMetricsSystem.setMiniClusterMode(true);
    try {
      assertIndependent(BlockDeletingServiceMetrics::create, BlockDeletingServiceMetrics::unRegister);
      assertIndependent(BackgroundVolumeScannerMetrics::create, BackgroundVolumeScannerMetrics::unregister);
      assertIndependent(ECReconstructionMetrics::create, ECReconstructionMetrics::unRegister);
      assertIndependent(() -> CommandHandlerMetrics.create(Collections.emptyMap()), CommandHandlerMetrics::unRegister);
      assertIndependent(() -> ReplicationSupervisorMetrics.create(mock(ReplicationSupervisor.class)),
          ReplicationSupervisorMetrics::unRegister);
      assertIndependent(ContainerMetadataScannerMetrics::create, ContainerMetadataScannerMetrics::unregister);
      assertIndependent(OnDemandScannerMetrics::create, OnDemandScannerMetrics::unregister);
      assertIndependent(() -> VolumeHealthMetrics.create(StorageVolume.VolumeType.DATA_VOLUME),
          VolumeHealthMetrics::unregister);
      assertIndependent(() -> ContainerMetrics.create(new OzoneConfiguration()), ContainerMetrics::close);
      assertIndependent(() -> ContainerDataScannerMetrics.create("volume"), ContainerDataScannerMetrics::unregister);
      assertIndependent(() -> new VolumeIOStats("volume", "volume", new int[] {60}), VolumeIOStats::unregister);
      assertIndependent(() -> new VolumeInfoMetrics("volume", mock(HddsVolume.class)), VolumeInfoMetrics::unregister);
      RaftGroupId groupId = RaftGroupId.randomId();
      assertIndependent(() -> CSMMetrics.create(groupId), CSMMetrics::unRegister);
      assertIndependent(() -> new MeasuredReplicator(task -> { }, "push"), MeasuredReplicator::close);
    } finally {
      DefaultMetricsSystem.setMiniClusterMode(previous);
    }
  }

  @Test
  void testVolumeCheckerShutdownBeforeStart() throws Exception {
    boolean previous = DefaultMetricsSystem.inMiniClusterMode();
    DefaultMetricsSystem.setMiniClusterMode(true);
    try {
      assertIndependent(() -> new StorageVolumeChecker(new OzoneConfiguration(), new Timer(), ""),
          checker -> checker.shutdownAndWait(0, TimeUnit.SECONDS));
    } finally {
      DefaultMetricsSystem.setMiniClusterMode(previous);
    }
  }

  @Test
  void testServerReleasesStateMachineMissingFromRatis(@TempDir Path directory) throws Exception {
    boolean previous = DefaultMetricsSystem.inMiniClusterMode();
    DefaultMetricsSystem.setMiniClusterMode(true);
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, directory.toString());
    conf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_RANDOM_PORT, true);
    conf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED, false);
    Set<String> initial = sources();
    XceiverServerRatis server = XceiverServerRatis.newXceiverServerRatis(null,
        MockDatanodeDetails.randomDatanodeDetails(), conf, mock(ContainerDispatcher.class),
        mock(ContainerController.class), null, null);
    try {
      // Ratis may remove a division from its map while its state machine is still initializing.
      Method factory = XceiverServerRatis.class.getDeclaredMethod("getStateMachine", RaftGroupId.class);
      factory.setAccessible(true);
      factory.invoke(server, RaftGroupId.randomId());
      assertNotEquals(initial, sources());
      XceiverServerRatis stoppedServer = server;
      server = null;
      stoppedServer.stop();
      assertEquals(initial, sources(), "The server must release state machines missing from the Ratis division map");
    } finally {
      if (server != null) {
        server.stop();
      }
      DefaultMetricsSystem.setMiniClusterMode(previous);
    }
  }

  private static <T> void assertIndependent(Supplier<T> create, CheckedConsumer<T, Exception> close) throws Exception {
    Set<String> initial = sources();
    T first = create.get();
    T second = null;
    try {
      Set<String> firstNames = sources();
      firstNames.removeAll(initial);
      assertEquals(1, firstNames.size());
      second = create.get();
      Set<String> secondNames = sources();
      secondNames.removeAll(initial);
      secondNames.removeAll(firstNames);
      assertEquals(1, secondNames.size(), "Each datanode must own a distinct source");
      T closing = second;
      second = null;
      close.accept(closing);
      assertTrue(sources().containsAll(firstNames), "Stopping a datanode must preserve its live peer's source");
      closing = first;
      first = null;
      close.accept(closing);
      assertEquals(initial, sources(), "All sources must be released after both datanodes stop");
    } finally {
      if (first != null) {
        close.accept(first);
      }
      if (second != null) {
        close.accept(second);
      }
    }
  }

  private static Set<String> sources() throws Exception {
    Field field = DefaultMetricsSystem.instance().getClass().getDeclaredField("allSources");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, MetricsSource> sources = (Map<String, MetricsSource>) field.get(DefaultMetricsSystem.instance());
    return new HashSet<>(sources.keySet());
  }
}
