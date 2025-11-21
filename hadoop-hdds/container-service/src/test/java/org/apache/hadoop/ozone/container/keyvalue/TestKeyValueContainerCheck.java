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

package org.apache.hadoop.ozone.container.keyvalue;

import static org.apache.hadoop.ozone.container.keyvalue.TestContainerCorruptions.CORRUPT_BLOCK;
import static org.apache.hadoop.ozone.container.keyvalue.TestContainerCorruptions.CORRUPT_CONTAINER_FILE;
import static org.apache.hadoop.ozone.container.keyvalue.TestContainerCorruptions.MISSING_BLOCK;
import static org.apache.hadoop.ozone.container.keyvalue.TestContainerCorruptions.MISSING_CHUNKS_DIR;
import static org.apache.hadoop.ozone.container.keyvalue.TestContainerCorruptions.MISSING_CONTAINER_DIR;
import static org.apache.hadoop.ozone.container.keyvalue.TestContainerCorruptions.MISSING_CONTAINER_FILE;
import static org.apache.hadoop.ozone.container.keyvalue.TestContainerCorruptions.MISSING_METADATA_DIR;
import static org.apache.hadoop.ozone.container.keyvalue.TestContainerCorruptions.TRUNCATED_BLOCK;
import static org.apache.hadoop.ozone.container.keyvalue.TestContainerCorruptions.TRUNCATED_CONTAINER_FILE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.checksum.ContainerDiffReport;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.interfaces.ScanResult;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScanError;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScanError.FailureType;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.apache.hadoop.ozone.container.ozoneimpl.DataScanResult;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the KeyValueContainerCheck class's ability to detect container errors.
 */
public class TestKeyValueContainerCheck
    extends TestKeyValueContainerIntegrityChecks {

  private static final Logger LOG = LoggerFactory.getLogger(TestKeyValueContainerCheck.class);

  /**
   * Container fault injection is not supported with the old file per chunk layout.
   * @return The container versions that should be tested with fault injection.
   */
  private static Stream<ContainerTestVersionInfo> provideContainerVersions() {
    return ContainerTestVersionInfo.getLayoutList().stream()
        .filter(c -> c.getLayout() != ContainerLayoutVersion.FILE_PER_CHUNK);
  }

  /**
   * @return A matrix of the container versions that should be tested with fault injection paired with each type of
   * metadata fault.
   */
  private static Stream<Arguments> provideMetadataCorruptions() {
    List<TestContainerCorruptions> metadataCorruptions = Arrays.asList(
        MISSING_CHUNKS_DIR,
        MISSING_METADATA_DIR,
        MISSING_CONTAINER_DIR,
        MISSING_CONTAINER_FILE,
        CORRUPT_CONTAINER_FILE,
        TRUNCATED_CONTAINER_FILE
    );
    return provideContainerVersions()
        .flatMap(version -> metadataCorruptions.stream().map(corruption -> Arguments.of(version, corruption)));
  }

  /**
   * When the scanner encounters an issue with container metadata, it should fail the scan immediately.
   * Metadata is required before reading the data.
   */
  @ParameterizedTest
  @MethodSource("provideMetadataCorruptions")
  public void testExitEarlyOnMetadataError(ContainerTestVersionInfo versionInfo,
      TestContainerCorruptions metadataCorruption) throws Exception {
    initTestData(versionInfo);
    long containerID = 101;
    int deletedBlocks = 0;
    int normalBlocks = 3;
    OzoneConfiguration conf = getConf();
    ContainerScannerConfiguration c = conf.getObject(ContainerScannerConfiguration.class);
    DataTransferThrottler throttler = new DataTransferThrottler(c.getBandwidthPerVolume());

    KeyValueContainer container = createContainerWithBlocks(containerID,
        normalBlocks, deletedBlocks, true);
    KeyValueContainerCheck kvCheck = new KeyValueContainerCheck(conf, container);

    DataScanResult result = kvCheck.fullCheck(throttler, null);
    assertFalse(result.hasErrors());

    // Inject a metadata and a data error.
    metadataCorruption.applyTo(container);
    // All other metadata failures are independent of the block files, so we can add a data failure later in the scan.
    if (metadataCorruption != MISSING_CHUNKS_DIR && metadataCorruption != MISSING_CONTAINER_DIR) {
      CORRUPT_BLOCK.applyTo(container);
    }

    result = kvCheck.fullCheck(throttler, null);
    assertTrue(result.hasErrors());
    // Scan should have failed after the first metadata error and not made it to the data error.
    assertEquals(1, result.getErrors().size());
    assertEquals(metadataCorruption.getExpectedResult(), result.getErrors().get(0).getFailureType());
  }

  /**
   * When the scanner encounters an issues with container data, it should continue scanning to collect all issues
   * among all blocks.
   */
  @ParameterizedTest
  @MethodSource("provideContainerVersions")
  public void testAllDataErrorsCollected(ContainerTestVersionInfo versionInfo) throws Exception {
    initTestData(versionInfo);

    long containerID = 101;
    int deletedBlocks = 0;
    int normalBlocks = 6;
    OzoneConfiguration conf = getConf();
    ContainerScannerConfiguration c = conf.getObject(ContainerScannerConfiguration.class);
    DataTransferThrottler throttler = new DataTransferThrottler(c.getBandwidthPerVolume());
    KeyValueContainer container = createContainerWithBlocks(containerID,
        normalBlocks, deletedBlocks, true);
    KeyValueContainerCheck kvCheck = new KeyValueContainerCheck(conf, container);

    DataScanResult result = kvCheck.fullCheck(throttler, null);
    assertFalse(result.hasErrors());
    // The scanner would write the checksum file to disk. `KeyValueContainerCheck` does not, so we will create the
    // result here.
    ContainerProtos.ContainerChecksumInfo healthyChecksumInfo = ContainerProtos.ContainerChecksumInfo.newBuilder()
        .setContainerID(containerID)
        .setContainerMerkleTree(result.getDataTree().toProto())
        .build();

    // Put different types of block failures in the middle of the container.
    long corruptBlockID = 1;
    long missingBlockID = 2;
    long truncatedBlockID = 4;
    CORRUPT_BLOCK.applyTo(container, corruptBlockID);
    MISSING_BLOCK.applyTo(container, missingBlockID);
    TRUNCATED_BLOCK.applyTo(container, truncatedBlockID);
    List<FailureType> expectedErrors = new ArrayList<>();
    // Corruption is applied to two different chunks within the block, so the error will be raised twice.
    expectedErrors.add(CORRUPT_BLOCK.getExpectedResult());
    expectedErrors.add(CORRUPT_BLOCK.getExpectedResult());
    expectedErrors.add(MISSING_BLOCK.getExpectedResult());
    // When a block file is truncated, all chunks in the block will be reported as missing.
    // This is expected since reconciliation will do the repair at the chunk level.
    for (int i = 0; i < CHUNKS_PER_BLOCK; i++) {
      expectedErrors.add(TRUNCATED_BLOCK.getExpectedResult());
    }

    result = kvCheck.fullCheck(throttler, null);
    result.getErrors().forEach(e -> LOG.info("Error detected: {}", e));

    assertTrue(result.hasErrors());
    // Check that all data errors were detected in order.
    assertEquals(expectedErrors.size(), result.getErrors().size());
    List<FailureType> actualErrors = result.getErrors().stream()
        .map(ContainerScanError::getFailureType)
        .collect(Collectors.toList());
    assertEquals(expectedErrors, actualErrors);

    // Write the new tree into the container, as the scanner would do.
    ContainerChecksumTreeManager checksumManager = new ContainerChecksumTreeManager(conf);
    KeyValueContainerData containerData = container.getContainerData();
    // This will read the corrupted tree from the disk, which represents the current state of the container, and
    // compare it against the original healthy tree. The diff we get back should match the failures we injected.
    ContainerProtos.ContainerChecksumInfo generatedChecksumInfo =
        checksumManager.updateTree(containerData, result.getDataTree());
    ContainerDiffReport diffReport = checksumManager.diff(generatedChecksumInfo, healthyChecksumInfo);

    LOG.info("Diff of healthy container with actual container {}", diffReport);

    // Check that the new tree identified all the expected errors by checking the diff.
    Map<Long, List<ContainerProtos.ChunkMerkleTree>> corruptChunks = diffReport.getCorruptChunks();
    // One block had corrupted chunks.
    assertEquals(1, corruptChunks.size());
    List<ContainerProtos.ChunkMerkleTree> corruptChunksInBlock = corruptChunks.get(corruptBlockID);
    assertEquals(2, corruptChunksInBlock.size());

    // One block was truncated which resulted in all of its chunks being reported as missing.
    Map<Long, List<ContainerProtos.ChunkMerkleTree>> missingChunks = diffReport.getMissingChunks();
    assertEquals(1, missingChunks.size());
    List<ContainerProtos.ChunkMerkleTree> missingChunksInBlock = missingChunks.get(truncatedBlockID);
    assertEquals(CHUNKS_PER_BLOCK, missingChunksInBlock.size());

    // Check missing block was correctly identified in the tree diff.
    List<ContainerProtos.BlockMerkleTree> missingBlocks = diffReport.getMissingBlocks();
    assertEquals(1, missingBlocks.size());
    assertEquals(missingBlockID, missingBlocks.get(0).getBlockID());

  }

  /**
   * Sanity test, when there are no corruptions induced.
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testKeyValueContainerCheckNoCorruption(
      ContainerTestVersionInfo versionInfo) throws Exception {
    initTestData(versionInfo);
    long containerID = 101;
    int deletedBlocks = 1;
    int normalBlocks = 3;
    OzoneConfiguration conf = getConf();
    ContainerScannerConfiguration c = conf.getObject(
        ContainerScannerConfiguration.class);

    // test Closed Container
    KeyValueContainer container = createContainerWithBlocks(containerID,
        normalBlocks, deletedBlocks, true);
    KeyValueContainerCheck kvCheck = new KeyValueContainerCheck(conf, container);

    // first run checks on a Open Container
    boolean valid = !kvCheck.fastCheck().hasErrors();
    assertTrue(valid);

    container.close();

    // next run checks on a Closed Container
    valid = !kvCheck.fullCheck(new DataTransferThrottler(
        c.getBandwidthPerVolume()), null).hasErrors();
    assertTrue(valid);
  }

  /**
   * Sanity test, when there are corruptions induced.
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testKeyValueContainerCheckCorruption(
      ContainerTestVersionInfo versionInfo) throws Exception {
    initTestData(versionInfo);
    long containerID = 102;
    int deletedBlocks = 1;
    int normalBlocks = 3;
    OzoneConfiguration conf = getConf();
    ContainerScannerConfiguration sc = conf.getObject(
        ContainerScannerConfiguration.class);

    // test Closed Container
    KeyValueContainer container = createContainerWithBlocks(containerID,
        normalBlocks, deletedBlocks, true);
    KeyValueContainerData containerData = container.getContainerData();

    container.close();

    KeyValueContainerCheck kvCheck = new KeyValueContainerCheck(conf, container);

    File dbFile = KeyValueContainerLocationUtil
        .getContainerDBFile(containerData);
    containerData.setDbFile(dbFile);
    try (DBHandle ignored = BlockUtils.getDB(containerData, conf);
        BlockIterator<BlockData> kvIter =
                ignored.getStore().getBlockIterator(containerID)) {
      BlockData block = kvIter.nextBlock();
      assertFalse(block.getChunks().isEmpty());
      ContainerProtos.ChunkInfo c = block.getChunks().get(0);
      BlockID blockID = block.getBlockID();
      File chunkFile = getChunkLayout().getChunkFile(containerData, blockID, c.getChunkName());
      long length = chunkFile.length();
      assertThat(length).isGreaterThan(0);
      // forcefully truncate the file to induce failure.
      try (RandomAccessFile file = new RandomAccessFile(chunkFile, "rws")) {
        file.setLength(length / 2);
      }
      assertEquals(length / 2, chunkFile.length());
    }

    // metadata check should pass.
    boolean valid = !kvCheck.fastCheck().hasErrors();
    assertTrue(valid);

    // checksum validation should fail.
    valid = !kvCheck.fullCheck(new DataTransferThrottler(
            sc.getBandwidthPerVolume()), null).hasErrors();
    assertFalse(valid);
  }

  @ContainerTestVersionInfo.ContainerTest
  void testKeyValueContainerCheckDeletedContainer(ContainerTestVersionInfo versionInfo) throws Exception {
    initTestData(versionInfo);

    KeyValueContainer container = createContainerWithBlocks(3,
        3, 3, true);
    container.close();
    KeyValueContainerCheck kvCheck = new KeyValueContainerCheck(getConf(), container);
    // The full container should exist and pass a scan.
    ScanResult result = kvCheck.fullCheck(mock(DataTransferThrottler.class), mock(Canceler.class));
    assertFalse(result.hasErrors());
    assertFalse(result.isDeleted());

    // When a container is not marked for deletion and it has pieces missing, the scan should fail.
    File metadataDir = new File(container.getContainerData().getChunksPath());
    FileUtils.deleteDirectory(metadataDir);
    assertFalse(metadataDir.exists());
    result = kvCheck.fullCheck(mock(DataTransferThrottler.class), mock(Canceler.class));
    assertTrue(result.hasErrors());
    assertFalse(result.isDeleted());

    // Once the container is marked for deletion, the scan should pass even if some of the internal pieces are missing.
    // Here the metadata directory has been deleted.
    container.markContainerForDelete();
    result = kvCheck.fullCheck(mock(DataTransferThrottler.class), mock(Canceler.class));
    assertFalse(result.hasErrors());
    assertTrue(result.isDeleted());

    // Now the data directory is deleted.
    File chunksDir = new File(container.getContainerData().getChunksPath());
    FileUtils.deleteDirectory(chunksDir);
    assertFalse(chunksDir.exists());
    result = kvCheck.fullCheck(mock(DataTransferThrottler.class), mock(Canceler.class));
    assertFalse(result.hasErrors());
    assertTrue(result.isDeleted());

    // Now the whole container directory is gone.
    File containerDir = new File(container.getContainerData().getContainerPath());
    FileUtils.deleteDirectory(containerDir);
    assertFalse(containerDir.exists());
    result = kvCheck.fullCheck(mock(DataTransferThrottler.class), mock(Canceler.class));
    assertFalse(result.hasErrors());
    assertTrue(result.isDeleted());
  }
}
