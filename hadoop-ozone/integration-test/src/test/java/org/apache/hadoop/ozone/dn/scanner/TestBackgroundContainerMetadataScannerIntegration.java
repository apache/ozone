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

package org.apache.hadoop.ozone.dn.scanner;

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.OPEN;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.UNHEALTHY;
import static org.apache.hadoop.ozone.container.keyvalue.TestContainerCorruptions.MISSING_CONTAINER_DIR;
import static org.apache.hadoop.ozone.container.keyvalue.TestContainerCorruptions.MISSING_METADATA_DIR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.utils.ContainerLogger;
import org.apache.hadoop.ozone.container.keyvalue.TestContainerCorruptions;
import org.apache.hadoop.ozone.container.ozoneimpl.BackgroundContainerMetadataScanner;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration tests for the background container metadata scanner. This
 * scanner does a quick check of container metadata to find obvious failures
 * faster than a full data scan.
 */
class TestBackgroundContainerMetadataScannerIntegration
    extends TestContainerScannerIntegrationAbstract {

  private final GenericTestUtils.LogCapturer logCapturer =
      GenericTestUtils.LogCapturer.log4j2(ContainerLogger.LOG_NAME);

  static Collection<TestContainerCorruptions> supportedCorruptionTypes() {
    return TestContainerCorruptions.getAllParamsExcept(
        TestContainerCorruptions.MISSING_BLOCK,
        TestContainerCorruptions.CORRUPT_BLOCK,
        TestContainerCorruptions.TRUNCATED_BLOCK);
  }

  @BeforeAll
  static void init() throws Exception {
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    // Speed up SCM closing of open container when an unhealthy replica is
    // reported.
    ReplicationManager.ReplicationManagerConfiguration rmConf = ozoneConfig
        .getObject(ReplicationManager.ReplicationManagerConfiguration.class);
    rmConf.setInterval(Duration.ofSeconds(1));
    ozoneConfig.setFromObject(rmConf);

    ozoneConfig.setBoolean(
        ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_ENABLED, true);
    // Make sure the background data scanner does not detect failures
    // before the metadata scanner under test does.
    ozoneConfig.setBoolean(
        ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_DEV_DATA_ENABLED,
        false);
    // Make the background metadata scanner run frequently to reduce test time.
    ozoneConfig.setTimeDuration(
        ContainerScannerConfiguration.METADATA_SCAN_INTERVAL_KEY,
        SCAN_INTERVAL.getSeconds(), TimeUnit.SECONDS);
    buildCluster(ozoneConfig);
  }

  /**
   * {@link BackgroundContainerMetadataScanner} should detect corrupted metadata
   * in open or closed containers without client interaction.
   */
  @ParameterizedTest
  @MethodSource("supportedCorruptionTypes")
  void testCorruptionDetected(TestContainerCorruptions corruption)
      throws Exception {
    // Write data to an open and closed container.
    long closedContainerID = writeDataThenCloseContainer();
    Container<?> closedContainer = getDnContainer(closedContainerID);
    assertEquals(State.CLOSED, closedContainer.getContainerState());
    assertTrue(containerChecksumFileExists(closedContainerID));
    waitForScmToSeeReplicaState(closedContainerID, CLOSED);
    long initialClosedChecksum = getContainerReplica(closedContainerID).getDataChecksum();
    assertNotEquals(0, initialClosedChecksum);

    long openContainerID = writeDataToOpenContainer();
    Container<?> openContainer = getDnContainer(openContainerID);
    assertEquals(State.OPEN, openContainer.getContainerState());
    waitForScmToSeeReplicaState(openContainerID, OPEN);
    // Open containers should not yet have a checksum generated.
    assertEquals(0, getContainerReplica(openContainerID).getDataChecksum());

    // Corrupt both containers.
    corruption.applyTo(closedContainer);
    corruption.applyTo(openContainer);

    // Wait for the scanner to detect corruption.
    GenericTestUtils.waitFor(
        () -> closedContainer.getContainerState() == State.UNHEALTHY,
        500, 5000);
    GenericTestUtils.waitFor(
        () -> openContainer.getContainerState() == State.UNHEALTHY,
        500, 5000);

    // Wait for SCM to get reports of the unhealthy replicas.
    // The metadata scanner does not generate data checksums and the other scanners have been turned off for this
    // test, so the data checksums should not change.
    waitForScmToSeeReplicaState(closedContainerID, UNHEALTHY);
    assertEquals(initialClosedChecksum, getContainerReplica(closedContainerID).getDataChecksum());
    waitForScmToSeeReplicaState(openContainerID, UNHEALTHY);
    if (corruption == MISSING_METADATA_DIR || corruption == MISSING_CONTAINER_DIR) {
      // In these cases the tree cannot be generated when the container is marked unhealthy and the checksum should
      // remain at 0.
      // The tree is generated from metadata by the container changing to unhealthy, not by the metadata scanner.
      assertEquals(0, getContainerReplica(openContainerID).getDataChecksum());
    } else {
      // The checksum will be generated for the first time when the container is marked unhealthy.
      // The tree is generated from metadata by the container changing to unhealthy, not by the metadata scanner.
      assertNotEquals(0, getContainerReplica(openContainerID).getDataChecksum());
    }

    // Once the unhealthy replica is reported, the open container's lifecycle
    // state in SCM should move to closed.
    waitForScmToCloseContainer(openContainerID);
    corruption.assertLogged(openContainerID, 1, logCapturer);
    corruption.assertLogged(closedContainerID, 1, logCapturer);
  }
}
