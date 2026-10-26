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

package org.apache.hadoop.ozone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.ozone.test.LocalJobRunnerMetricsCleanup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

/**
 * Tests detection of leaked Hadoop metrics sources.
 */
class TestMetricsLeakAssertion {

  private MetricsSystem originalMetricsSystem;
  private MetricsSystem metricsSystem;
  private final List<String> registeredSources = new ArrayList<>();

  @BeforeEach
  void setUp() {
    originalMetricsSystem = DefaultMetricsSystem.instance();
    // A subclass also verifies that the assertion finds inherited fields.
    metricsSystem = new MetricsSystemImpl() { };
    DefaultMetricsSystem.setInstance(metricsSystem);
  }

  @AfterEach
  void tearDown() {
    try {
      registeredSources.forEach(metricsSystem::unregisterSource);
      metricsSystem.shutdown();
    } finally {
      DefaultMetricsSystem.setInstance(originalMetricsSystem);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"LeakedMetricsSource", "HttpServer2Metrics-service1", "RpcActivityForPorttest",
      "RpcDetailedActivityForPorttest", "LocalJobRunnerMetricstest", "StorageContainerMetrics",
      "ContainerDataScannerMetricstest", "ContainerMetadataScannerMetrics", "On-demand container scanner metrics",
      "BackgroundVolumeScannerMetrics", "VolumeHealthMetrics-test", "VolumeIOStats-test", "VolumeInfoMetrics-test",
      "CommandHandlerMetrics", "HddsDispatcher", "ECReconstructionMetrics", "ReplicationSupervisorMetrics",
      "ContainerReplicator/push", "BlockDeletingService", "GrpcMetrics", "SCMNodeMetrics",
      "SCMContainerManagerMetrics", "SafeModeMetrics", "ContainerBalancerMetrics", "NodeDecommissionMetrics",
      "SCMDatanodeProtocol", "ScmBlockLocationProtocol", "ScmContainerLocationProtocol", "ScmSecurityProtocol",
      "EventQueuetest", "OMMetrics", "OMPerformanceMetrics", "OMLockMetrics", "OMHAMetrics",
      "OMSnapshotDirectoryMetrics", "OmSnapshotMetrics", "OmClientProtocol", "DeletingServiceMetrics",
      "KeyLifecycleServiceMetrics", "BucketUtilizationMetrics", "Rocksdb_test", "SSTFilePruningMetrics-test",
      "DBCheckpointMetrics", "keyTableCache", "CSMMetricsgroup-test", "CacheMetrics-test", "NettyMetricstest",
      "ContainerClientMetrics1", "ReconTaskMetrics", "ReconTaskControllerMetrics", "keyTableCache-1"})
  void testUnexpectedSourceFails(String sourceName) {
    registerSource("ContainerCacheMetrics");
    registerSource(sourceName);

    AssertionError error = assertThrows(AssertionError.class, MetricsLeakAssertion::assertNoLeaks);
    assertThat(error).hasMessageContaining(sourceName);
    assertNull(metricsSystem.getSource(sourceName));
    assertNotNull(metricsSystem.getSource("ContainerCacheMetrics"));
    assertDoesNotThrow(MetricsLeakAssertion::assertNoLeaks);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ContainerCacheMetrics", "ContainerCacheMetrics-1", "ContainerCacheMetrics-123",
      "JvmMetricsOM", "DelegationTokenSecretManagerMetrics"})
  void testAllowedSourcePasses(String sourceName) {
    registerSource(sourceName);

    assertDoesNotThrow(MetricsLeakAssertion::assertNoLeaks);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ContainerCacheMetrics-unexpected", "ContainerCacheMetrics-", "keyTableCache-unexpected"})
  void testUnexpectedSuffixFails(String sourceName) {
    registerSource(sourceName);

    AssertionError error = assertThrows(AssertionError.class, MetricsLeakAssertion::assertNoLeaks);
    assertThat(error).hasMessageContaining(sourceName);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testLocalJobRunnerMetricsCleanupPreservesExistingSources() throws Exception {
    boolean miniClusterMode = DefaultMetricsSystem.inMiniClusterMode();
    DefaultMetricsSystem.setMiniClusterMode(true);
    try {
      metricsSystem.init(getClass().getSimpleName());
      registerSource("LocalJobRunnerMetrics-existing");
      Field sourcesField = MetricsSystemImpl.class.getDeclaredField("allSources");
      sourcesField.setAccessible(true);
      Map<String, ?> sources = (Map<String, ?>) sourcesField.get(metricsSystem);
      Field referenceCountField = MetricsSystemImpl.class.getDeclaredField("refCount");
      referenceCountField.setAccessible(true);
      int references = referenceCountField.getInt(metricsSystem);
      try (LocalJobRunnerMetricsCleanup cleanup = new LocalJobRunnerMetricsCleanup()) {
        new LocalJobRunner(new Configuration());
        new LocalJobRunner(new Configuration());
        assertThat(sources.keySet()).filteredOn(name -> name.startsWith("LocalJobRunnerMetrics-")).hasSize(3);
        cleanup.close();
        assertThat(sources.keySet()).filteredOn(name -> name.startsWith("LocalJobRunnerMetrics-"))
            .containsExactly("LocalJobRunnerMetrics-existing");
        assertThat(referenceCountField.getInt(metricsSystem)).isEqualTo(references);
        cleanup.close();
        assertThat(referenceCountField.getInt(metricsSystem)).isEqualTo(references);
      }
    } finally {
      DefaultMetricsSystem.setMiniClusterMode(miniClusterMode);
    }
  }

  @Test
  void testMissingSourcesFieldFails() {
    DefaultMetricsSystem.setInstance(mock(MetricsSystem.class));

    AssertionError error = assertThrows(AssertionError.class, MetricsLeakAssertion::assertNoLeaks);
    assertThat(error).hasMessageContaining("field not found");
  }

  @Test
  void testInvalidSourcesFieldFails() {
    DefaultMetricsSystem.setInstance(new MetricsSystemImpl() {
      @SuppressWarnings("PMD.UnusedPrivateField") // Accessed reflectively by MetricsLeakAssertion.
      private final String allSources = "not a map";
    });

    AssertionError error = assertThrows(AssertionError.class, MetricsLeakAssertion::assertNoLeaks);
    assertThat(error).hasMessageContaining("is not a Map");
  }

  @Test
  void testMetricsSystemAccessFailureFails() {
    SecurityException failure = new SecurityException("metrics access denied");
    try (MockedStatic<DefaultMetricsSystem> defaultMetricsSystem = mockStatic(DefaultMetricsSystem.class)) {
      defaultMetricsSystem.when(DefaultMetricsSystem::instance).thenThrow(failure);

      AssertionError error = assertThrows(AssertionError.class, MetricsLeakAssertion::assertNoLeaks);
      assertSame(failure, error.getCause());
    }
  }

  private void registerSource(String name) {
    metricsSystem.register(name, name, (MetricsSource) (collector, all) -> { });
    registeredSources.add(name);
  }
}
