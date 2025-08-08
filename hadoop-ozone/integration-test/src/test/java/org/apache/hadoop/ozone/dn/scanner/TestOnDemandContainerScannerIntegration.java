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
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.UNHEALTHY;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.readChecksumFile;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.verifyAllDataChecksumsMatch;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.utils.ContainerLogger;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.TestContainerCorruptions;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.apache.hadoop.ozone.container.ozoneimpl.OnDemandContainerScanner;
import org.apache.hadoop.ozone.container.ozoneimpl.OnDemandScannerMetrics;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration tests for the on demand container data scanner. This scanner
 * is triggered when there is an error while a client interacts with a
 * container.
 */
class TestOnDemandContainerScannerIntegration
    extends TestContainerScannerIntegrationAbstract {

  private final GenericTestUtils.LogCapturer logCapturer =
      GenericTestUtils.LogCapturer.log4j2(ContainerLogger.LOG_NAME);

  /**
   The on-demand container scanner is triggered by errors on the block read
   path. Since this may not touch all parts of the container, the scanner is
   limited in what errors it can detect:
   - The container file is not on the read path, so any errors in this file
   will not trigger an on-demand scan.
   - With container schema v3 (one RocksDB per volume), RocksDB is not in
   the container metadata directory, therefore nothing in this directory is on
   the read path.
   - Block checksums are verified on the client side. If there is a checksum
   error during read, the datanode will not learn about it.
   */
  static Collection<TestContainerCorruptions> supportedCorruptionTypes() {
    return TestContainerCorruptions.getAllParamsExcept(
        TestContainerCorruptions.MISSING_METADATA_DIR,
        TestContainerCorruptions.MISSING_CONTAINER_FILE,
        TestContainerCorruptions.CORRUPT_CONTAINER_FILE,
        TestContainerCorruptions.TRUNCATED_CONTAINER_FILE,
        TestContainerCorruptions.CORRUPT_BLOCK,
        TestContainerCorruptions.TRUNCATED_BLOCK);
  }

  static Collection<TestContainerCorruptions> supportedCorruptionTypesForOpen() {
    Set<TestContainerCorruptions> set = EnumSet.copyOf(supportedCorruptionTypes());
    // Open containers will be checked only for metadata corruption, so missing block is not a valid corruption type.
    set.remove(TestContainerCorruptions.MISSING_BLOCK);
    return set;
  }

  @BeforeAll
  static void init() throws Exception {
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    ozoneConfig.setBoolean(
        ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_ENABLED,
        true);
    // Disable both background container scanners to make sure only the
    // on-demand scanner is detecting failures.
    ozoneConfig.setBoolean(
        ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_DEV_DATA_ENABLED,
        false);
    ozoneConfig.setBoolean(
        ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_DEV_METADATA_ENABLED,
        false);
    buildCluster(ozoneConfig);
  }

  /**
   * {@link OnDemandContainerScanner} should detect corrupted blocks
   * in a closed container when a client reads from it.
   */
  @ParameterizedTest
  @MethodSource("supportedCorruptionTypes")
  void testCorruptionDetected(TestContainerCorruptions corruption)
      throws Exception {
    String keyName = "testKey";
    long containerID = writeDataThenCloseContainer(keyName);
    // Container corruption has not yet been introduced.
    Container<?> container = getDnContainer(containerID);
    assertEquals(State.CLOSED, container.getContainerState());
    assertTrue(containerChecksumFileExists(containerID));

    waitForScmToSeeReplicaState(containerID, CLOSED);
    long initialReportedDataChecksum = getContainerReplica(containerID).getDataChecksum();

    // Corrupt the container.
    corruption.applyTo(container);
    // This method will check that reading from the corrupted key returns an
    // error to the client.
    readFromCorruptedKey(keyName);
    // Reading from the corrupted key should have triggered an on-demand scan
    // of the container, which will detect the corruption.
    GenericTestUtils.waitFor(
        () -> container.getContainerState() == State.UNHEALTHY,
        500, 5000);

    // Wait for SCM to get a report of the unhealthy replica.
    waitForScmToSeeReplicaState(containerID, UNHEALTHY);
    corruption.assertLogged(containerID, 1, logCapturer);
    long newReportedDataChecksum = getContainerReplica(containerID).getDataChecksum();

    if (corruption == TestContainerCorruptions.MISSING_METADATA_DIR ||
        corruption == TestContainerCorruptions.MISSING_CONTAINER_DIR) {
      // In these cases, the new tree will not be able to be written since it exists in the metadata directory.
      // When the tree write fails, the in-memory checksum should remain at its original value.
      assertEquals(initialReportedDataChecksum, newReportedDataChecksum);
      assertFalse(containerChecksumFileExists(containerID));
    } else {
      assertNotEquals(initialReportedDataChecksum, newReportedDataChecksum);
      // Test that the scanner wrote updated checksum info to the disk.
      assertTrue(containerChecksumFileExists(containerID));
      ContainerProtos.ContainerChecksumInfo updatedChecksumInfo = readChecksumFile(container.getContainerData());
      assertEquals(newReportedDataChecksum, updatedChecksumInfo.getContainerMerkleTree().getDataChecksum());
      KeyValueContainerData containerData = (KeyValueContainerData) container.getContainerData();
      verifyAllDataChecksumsMatch(containerData, getConf());
    }
  }

  /**
   * {@link OnDemandContainerScanner} should detect corrupted blocks
   * in an open container when a client reads from it.
   */
  @ParameterizedTest
  @MethodSource("supportedCorruptionTypesForOpen")
  void testCorruptionDetectedForOpenContainers(TestContainerCorruptions corruption)
      throws Exception {
    String keyName = "keyName";

    long openContainerID = writeDataToOpenContainer();
    Container<?> openContainer = getDnContainer(openContainerID);
    assertEquals(State.OPEN, openContainer.getContainerState());

    // Corrupt the container.
    corruption.applyTo(openContainer);
    // This method will check that reading from the corrupted key returns an
    // error to the client.
    readFromCorruptedKey(keyName);

    // Reading from the corrupted key should have triggered an on-demand scan
    // of the container, which will detect the corruption.
    GenericTestUtils.waitFor(
        () -> openContainer.getContainerState() == State.UNHEALTHY,
        500, 5000);

    // Wait for SCM to get a report of the unhealthy replica.
    waitForScmToSeeReplicaState(openContainerID, UNHEALTHY);
    corruption.assertLogged(openContainerID, 1, logCapturer);
  }

  /**
   * Test that {@link OnDemandContainerScanner} is triggered when the HddsDispatcher
   * detects write failures and automatically triggers on-demand scans.
   */
  @Test
  void testOnDemandScanTriggeredByUnhealthyContainer() throws Exception {
    long containerID = writeDataToOpenContainer();
    Container<?> container = getDnContainer(containerID);
    assertEquals(State.OPEN, container.getContainerState());

    Optional<Instant> initialScanTime = container.getContainerData().lastDataScanTime();
    HddsDatanodeService dn = getDatanode();
    ContainerDispatcher dispatcher = dn.getDatanodeStateMachine().getContainer().getDispatcher();
    OnDemandScannerMetrics scannerMetrics = dn.getDatanodeStateMachine().getContainer()
        .getOnDemandScanner().getMetrics();
    int initialScannedCount = scannerMetrics.getNumContainersScanned();

    // Create a PutBlock request with malformed block data to trigger internal error
    ContainerProtos.ContainerCommandRequestProto writeFailureRequest =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.PutBlock)
            .setContainerID(containerID)
            .setDatanodeUuid(dn.getDatanodeDetails().getUuidString())
            .setPutBlock(ContainerProtos.PutBlockRequestProto.newBuilder()
                .setBlockData(ContainerProtos.BlockData.newBuilder()
                    .setBlockID(ContainerProtos.DatanodeBlockID.newBuilder()
                        .setContainerID(containerID)
                        .setLocalID(999L)
                        .setBlockCommitSequenceId(1)
                        .build())
                    .setSize(1024) // Size mismatch with chunks
                    .build())
                .build())
            .build();

    ContainerProtos.ContainerCommandResponseProto response = dispatcher.dispatch(writeFailureRequest, null);
    assertNotEquals(ContainerProtos.Result.SUCCESS, response.getResult());
    assertEquals(State.UNHEALTHY, container.getContainerState());

    // The dispatcher should have called containerSet.scanContainerWithoutGap due to the failure
    GenericTestUtils.waitFor(() -> {
      Optional<Instant> currentScanTime = container.getContainerData().lastDataScanTime();
      return currentScanTime.isPresent() && currentScanTime.get().isAfter(initialScanTime.orElse(Instant.EPOCH));
    }, 500, 5000);

    int finalScannedCount = scannerMetrics.getNumContainersScanned();
    assertTrue(finalScannedCount > initialScannedCount);
  }

}
