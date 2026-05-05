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

import static org.apache.hadoop.hdds.HddsUtils.checksumToString;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.UNHEALTHY;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.readChecksumFile;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.verifyAllDataChecksumsMatch;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.scm.container.ContainerChecksums;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.TestContainerCorruptions;
import org.apache.hadoop.ozone.container.ozoneimpl.BackgroundContainerDataScanner;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Integration tests for the background container data scanner. This scanner
 * checks all data and metadata in the container.
 */
class TestBackgroundContainerDataScannerIntegration
    extends TestContainerScannerIntegrationAbstract {

  @BeforeAll
  static void init() throws Exception {
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    ozoneConfig.setBoolean(
        ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_ENABLED, true);
    // Make sure the background metadata scanner does not detect failures
    // before the data scanner under test does.
    ozoneConfig.setBoolean(
        ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_DEV_METADATA_ENABLED,
        false);
    // Make the background data scanner run frequently to reduce test time.
    ozoneConfig.setTimeDuration(
        ContainerScannerConfiguration.DATA_SCAN_INTERVAL_KEY,
        SCAN_INTERVAL.getSeconds(), TimeUnit.SECONDS);
    buildCluster(ozoneConfig);
  }

  /**
   * {@link BackgroundContainerDataScanner} should detect corrupted blocks
   * in a closed container without client interaction.
   */
  @ParameterizedTest
  // Background container data scanner should be able to detect all errors.
  @EnumSource
  void testCorruptionDetected(TestContainerCorruptions corruption)
      throws Exception {
    pauseScanner();

    long containerID = writeDataThenCloseContainer();
    // Container corruption has not yet been introduced.
    Container<?> container = getDnContainer(containerID);
    assertEquals(State.CLOSED, container.getContainerState());
    assertTrue(containerChecksumFileExists(containerID));
    assertFalse(container.getContainerData().needsDataChecksum());
    assertNotEquals(0, container.getContainerData().getDataChecksum());

    waitForScmToSeeReplicaState(containerID, CLOSED);
    ContainerChecksums initialReportedChecksum = getContainerReplica(containerID).getChecksums();
    assertNotEquals(ContainerChecksums.unknown(), initialReportedChecksum);
    corruption.applyTo(container);

    resumeScanner();

    // Wait for the scanner to detect corruption.
    GenericTestUtils.waitFor(
        () -> container.getContainerState() == State.UNHEALTHY,
        500, 15_000);

    // Wait for SCM to get a report of the unhealthy replica with a different checksum than before.
    waitForScmToSeeReplicaState(containerID, UNHEALTHY);
    ContainerChecksums newReportedChecksum = getContainerReplica(containerID).getChecksums();
    if (corruption == TestContainerCorruptions.MISSING_METADATA_DIR ||
        corruption == TestContainerCorruptions.MISSING_CONTAINER_DIR) {
      // In these cases, the new tree will not be able to be written since it exists in the metadata directory.
      // When the tree write fails, the in-memory checksum should remain at its original value.
      assertEquals(initialReportedChecksum, newReportedChecksum);
    } else {
      assertNotEquals(initialReportedChecksum, newReportedChecksum);
      // Test that the scanner wrote updated checksum info to the disk.
      assertReplicaChecksumMatches(container, newReportedChecksum);
      assertFalse(container.getContainerData().needsDataChecksum());
      KeyValueContainerData containerData = (KeyValueContainerData) container.getContainerData();
      verifyAllDataChecksumsMatch(containerData, getConf());
    }

    if (corruption == TestContainerCorruptions.TRUNCATED_BLOCK ||
        corruption == TestContainerCorruptions.CORRUPT_BLOCK) {
      // These errors will affect multiple chunks and result in multiple log messages.
      corruption.assertLogged(containerID, getContainerLogCapturer());
    } else {
      // Other corruption types will only lead to a single error.
      corruption.assertLogged(containerID, 1, getContainerLogCapturer());
    }
  }

  private void assertReplicaChecksumMatches(
      Container<?> container, ContainerChecksums expectedChecksum) throws Exception {
    assertTrue(containerChecksumFileExists(container.getContainerData().getContainerID()));
    long dataChecksumFromFile = readChecksumFile(container.getContainerData())
        .getContainerMerkleTree().getDataChecksum();
    assertEquals(checksumToString(expectedChecksum.getDataChecksum()), checksumToString(dataChecksumFromFile));
  }
}
