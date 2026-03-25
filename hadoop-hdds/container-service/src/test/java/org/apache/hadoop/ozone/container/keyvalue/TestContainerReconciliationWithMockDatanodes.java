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

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.HddsUtils.checksumToString;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.verifyAllDataChecksumsMatch;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.WRITE_STAGE;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.createDbInstancesForTestIfNeeded;
import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import  static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.checksum.DNContainerOperationClient;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.apache.hadoop.ozone.container.ozoneimpl.OnDemandContainerScanner;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This unit test simulates three datanodes with replicas of a container that need to be reconciled.
 * It creates three KeyValueHandler instances to represent each datanode, and each instance is working on a container
 * replica that is stored in a local directory. The reconciliation client is mocked to return the corresponding local
 * container for each datanode peer.
 */
public class TestContainerReconciliationWithMockDatanodes {

  public static final Logger LOG = LoggerFactory.getLogger(TestContainerReconciliationWithMockDatanodes.class);

  // All container replicas will be placed in this directory, and the same replicas will be re-used for each test run.
  @TempDir
  private static Path containerDir;
  private static DNContainerOperationClient dnClient;
  private static MockedStatic<ContainerProtocolCalls> containerProtocolMock;
  private static List<MockDatanode> datanodes;
  private static long healthyDataChecksum;

  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static final long CONTAINER_ID = 100L;
  private static final int CHUNK_LEN = 3 * (int) OzoneConsts.KB;
  private static final int CHUNKS_PER_BLOCK = 4;
  private static final int NUM_DATANODES = 3;

  private static final String TEST_SCAN = "Test Scan";

  /**
   * Number of corrupt blocks and chunks.
   *
   * TODO HDDS-11942 support more combinations of corruptions.
   */
  public static Stream<Arguments> corruptionValues() {
    return Stream.of(
        Arguments.of(5, 0),
        Arguments.of(0, 5),
        Arguments.of(0, 10),
        Arguments.of(10, 0),
        Arguments.of(5, 10),
        Arguments.of(10, 5),
        Arguments.of(2, 3),
        Arguments.of(3, 2),
        Arguments.of(4, 6),
        Arguments.of(6, 4),
        Arguments.of(6, 9),
        Arguments.of(9, 6)
    );
  }

  /**
   * Use the same container instances throughout the tests. Each reconciliation should make a full repair, resetting
   * the state for the next test.
   */
  @BeforeAll
  public static void setup() throws Exception {
    LOG.info("Data written to {}", containerDir);
    dnClient = new DNContainerOperationClient(new OzoneConfiguration(), null, null);
    datanodes = new ArrayList<>();

    // Create a container with 15 blocks and 3 replicas.
    for (int i = 0; i < NUM_DATANODES; i++) {
      DatanodeDetails dnDetails = randomDatanodeDetails();
      // Use this fake host name to track the node through the test since it's easier to visualize than a UUID.
      dnDetails.setHostName("dn" + (i + 1));
      MockDatanode dn = new MockDatanode(dnDetails, containerDir);
      // This will close the container and build a data checksum based on the chunk checksums in the metadata.
      dn.addContainerWithBlocks(CONTAINER_ID, 15);
      datanodes.add(dn);
    }
    long dataChecksumFromMetadata = assertUniqueChecksumCount(CONTAINER_ID, datanodes, 1);
    assertNotEquals(0, dataChecksumFromMetadata);

    datanodes.forEach(d -> d.scanContainer(CONTAINER_ID));
    healthyDataChecksum = assertUniqueChecksumCount(CONTAINER_ID, datanodes, 1);
    assertEquals(dataChecksumFromMetadata, healthyDataChecksum);
    // Do not count the initial synchronous scan to build the merkle tree towards the scan count in the tests.
    // This lets each test run start counting the number of scans from zero.
    datanodes.forEach(MockDatanode::resetOnDemandScanCount);

    containerProtocolMock = Mockito.mockStatic(ContainerProtocolCalls.class);
    mockContainerProtocolCalls();
  }

  @AfterEach
  public void reset() {
    datanodes.forEach(MockDatanode::resetOnDemandScanCount);
  }

  @AfterAll
  public static void teardown() {
    if (containerProtocolMock != null) {
      containerProtocolMock.close();
    }
  }

  @ParameterizedTest
  @MethodSource("corruptionValues")
  public void testContainerReconciliation(int numBlocksToRemove, int numChunksToCorrupt) throws Exception {
    LOG.info("Healthy data checksum for container {} in this test is {}", CONTAINER_ID,
        checksumToString(healthyDataChecksum));
    // Introduce corruption in each container on different replicas.
    List<MockDatanode> dnsToCorrupt = datanodes.stream().limit(2).collect(Collectors.toList());

    dnsToCorrupt.get(0).introduceCorruption(CONTAINER_ID, numBlocksToRemove, numChunksToCorrupt, false);
    dnsToCorrupt.get(1).introduceCorruption(CONTAINER_ID, numBlocksToRemove, numChunksToCorrupt, true);
    // Use synchronous on-demand scans to re-build the merkle trees after corruption.
    datanodes.forEach(d -> d.scanContainer(CONTAINER_ID));
    // Without reconciliation, checksums should be different because of the corruption.
    assertUniqueChecksumCount(CONTAINER_ID, datanodes, 3);

    // Each datanode should have had one on-demand scan during test setup, and a second one after corruption was
    // introduced.
    waitForExpectedScanCount(1);

    // Reconcile each datanode with its peers.
    // In a real cluster, SCM will not send a command to reconcile a datanode with itself.
    for (MockDatanode current : datanodes) {
      List<DatanodeDetails> peers = datanodes.stream()
          .map(MockDatanode::getDnDetails)
          .filter(other -> !current.getDnDetails().equals(other))
          .collect(Collectors.toList());
      current.reconcileContainerSuccess(dnClient, peers, CONTAINER_ID);
    }
    // Reconciliation should have triggered a second on-demand scan for each replica. Wait for them to finish before
    // checking the results.
    waitForExpectedScanCount(2);
    // After reconciliation, checksums should be the same for all containers.
    long repairedDataChecksum = assertUniqueChecksumCount(CONTAINER_ID, datanodes, 1);
    assertEquals(healthyDataChecksum, repairedDataChecksum);
  }

  /**
   * Enum to represent different failure modes for container protocol calls.
   */
  public enum FailureLocation {
    GET_CONTAINER_CHECKSUM_INFO("getContainerChecksumInfo"),
    GET_BLOCK("getBlock"),
    READ_CHUNK("readChunk");

    private final String methodName;

    FailureLocation(String methodName) {
      this.methodName = methodName;
    }

    public String getMethodName() {
      return methodName;
    }
  }

  /**
   * Provides test parameters for different failure modes.
   */
  public static Stream<Arguments> failureLocations() {
    return Stream.of(
        Arguments.of(FailureLocation.GET_CONTAINER_CHECKSUM_INFO),
        Arguments.of(FailureLocation.GET_BLOCK),
        Arguments.of(FailureLocation.READ_CHUNK)
    );
  }

  @ParameterizedTest
  @MethodSource("failureLocations")
  public void testContainerReconciliationWithPeerFailure(FailureLocation failureLocation) throws Exception {
    LOG.info("Testing container reconciliation with peer failure in {} for container {}",
        failureLocation.getMethodName(), CONTAINER_ID);

    // Introduce corruption in the first datanode
    MockDatanode corruptedNode = datanodes.get(0);
    MockDatanode healthyNode1 = datanodes.get(1);
    MockDatanode healthyNode2 = datanodes.get(2);
    corruptedNode.introduceCorruption(CONTAINER_ID, 1, 1, false);

    // Use synchronous on-demand scans to re-build the merkle trees after corruption.
    datanodes.forEach(d -> d.scanContainer(CONTAINER_ID));

    // Without reconciliation, checksums should be different.
    assertUniqueChecksumCount(CONTAINER_ID, datanodes, 2);
    waitForExpectedScanCount(1);

    // Create a failing peer - we'll make the second datanode fail during the specified operation
    DatanodeDetails failingPeerDetails = healthyNode1.getDnDetails();
    // Mock the failure for the specific method based on the failure mode
    mockContainerProtocolCalls(failureLocation, failingPeerDetails);

    // Now reconcile the corrupted node with its peers (including the failing one)
    List<DatanodeDetails> peers = Arrays.asList(failingPeerDetails, healthyNode2.getDnDetails());
    corruptedNode.reconcileContainer(dnClient, peers, CONTAINER_ID);

    // Wait for scan to complete - but this time we only expect the corrupted node to have a scan
    // triggered by reconciliation, so we wait specifically for that one
    try {
      GenericTestUtils.waitFor(() -> corruptedNode.getOnDemandScanCount() == 2, 100, 5_000);
    } catch (TimeoutException ex) {
      LOG.warn("Timed out waiting for on-demand scan after reconciliation. Current count: {}",
          corruptedNode.getOnDemandScanCount());
    }

    // The corrupted node should still be repaired because it was able to reconcile with the healthy peer
    // even though one peer failed
    long repairedDataChecksum = assertUniqueChecksumCount(CONTAINER_ID, datanodes, 1);
    assertEquals(healthyDataChecksum, repairedDataChecksum);

    // Restore the original mock behavior for other tests
    mockContainerProtocolCalls();
  }

  @Test
  public void testContainerReconciliationFailureContainerScan()
      throws Exception {
    // Use synchronous on-demand scans to re-build the merkle trees after corruption.
    datanodes.forEach(d -> d.scanContainer(CONTAINER_ID));

    // Each datanode should have had one on-demand scan during test setup, and a second one after corruption was
    // introduced.
    waitForExpectedScanCount(1);

    for (MockDatanode current : datanodes) {
      doThrow(IOException.class).when(current.getHandler().getChecksumManager()).read(any());
      List<DatanodeDetails> peers = datanodes.stream()
          .map(MockDatanode::getDnDetails)
          .filter(other -> !current.getDnDetails().equals(other))
          .collect(Collectors.toList());
      // Reconciliation should fail for each datanode, since the checksum info cannot be retrieved.
      assertThrows(IOException.class, () -> current.reconcileContainer(dnClient, peers, CONTAINER_ID));
      Mockito.reset(current.getHandler().getChecksumManager());
    }
    // Even failure of Reconciliation should have triggered a second on-demand scan for each replica.
    waitForExpectedScanCount(2);
  }

  /**
   * Uses the on-demand container scanner metrics to wait for the expected number of on-demand scans to complete on
   * every datanode.
   */
  private void waitForExpectedScanCount(int expectedCountPerDatanode) throws Exception {
    for (MockDatanode datanode: datanodes) {
      try {
        GenericTestUtils.waitFor(() -> datanode.getOnDemandScanCount() == expectedCountPerDatanode, 100, 10_000);
      } catch (TimeoutException ex) {
        LOG.error("Timed out waiting for on-demand scan count {} to reach expected count {} on datanode {}",
            datanode.getOnDemandScanCount(), expectedCountPerDatanode, datanode);
        throw ex;
      }
    }
  }

  /**
   * Checks for the expected number of unique checksums among a container on the provided datanodes.
   * @return The data checksum from one of the nodes. Useful if expectedUniqueChecksums = 1.
   */
  private static long assertUniqueChecksumCount(long containerID, Collection<MockDatanode> nodes,
      long expectedUniqueChecksums) {
    long actualUniqueChecksums = nodes.stream()
        .mapToLong(d -> d.checkAndGetDataChecksum(containerID))
        .distinct()
        .count();
    assertEquals(expectedUniqueChecksums, actualUniqueChecksums);
    return nodes.stream().findAny().get().checkAndGetDataChecksum(containerID);
  }

  private static void mockContainerProtocolCalls() {
    // Mock network calls without injecting failures.
    mockContainerProtocolCalls(null, null);
  }

  private static void mockContainerProtocolCalls(FailureLocation failureLocation, DatanodeDetails failingPeerDetails) {
    Map<DatanodeDetails, MockDatanode> dnMap = datanodes.stream()
        .collect(Collectors.toMap(MockDatanode::getDnDetails, Function.identity()));

    // Mock getContainerChecksumInfo
    containerProtocolMock.when(() -> ContainerProtocolCalls.getContainerChecksumInfo(any(), anyLong(), any()))
        .thenAnswer(inv -> {
          XceiverClientSpi xceiverClientSpi = inv.getArgument(0);
          long containerID = inv.getArgument(1);
          Pipeline pipeline = xceiverClientSpi.getPipeline();
          assertEquals(1, pipeline.size());
          DatanodeDetails dn = pipeline.getFirstNode();

          if (failureLocation == FailureLocation.GET_CONTAINER_CHECKSUM_INFO && dn.equals(failingPeerDetails)) {
            throw new IOException("Simulated peer failure for testing in getContainerChecksumInfo");
          }

          return dnMap.get(dn).getChecksumInfo(containerID);
        });

    // Mock getBlock
    containerProtocolMock.when(() -> ContainerProtocolCalls.getBlock(any(), any(), any(), any(), anyMap()))
        .thenAnswer(inv -> {
          XceiverClientSpi xceiverClientSpi = inv.getArgument(0);
          BlockID blockID = inv.getArgument(2);
          Pipeline pipeline = xceiverClientSpi.getPipeline();
          assertEquals(1, pipeline.size());
          DatanodeDetails dn = pipeline.getFirstNode();

          if (failureLocation == FailureLocation.GET_BLOCK && dn.equals(failingPeerDetails)) {
            throw new IOException("Simulated peer failure for testing in getBlock");
          }

          return dnMap.get(dn).getBlock(blockID);
        });

    // Mock readChunk
    containerProtocolMock.when(() -> ContainerProtocolCalls.readChunk(any(), any(), any(), any(), any()))
        .thenAnswer(inv -> {
          XceiverClientSpi xceiverClientSpi = inv.getArgument(0);
          ContainerProtos.ChunkInfo chunkInfo = inv.getArgument(1);
          ContainerProtos.DatanodeBlockID blockId = inv.getArgument(2);
          List<XceiverClientSpi.Validator> checksumValidators = inv.getArgument(3);
          Pipeline pipeline = xceiverClientSpi.getPipeline();
          assertEquals(1, pipeline.size());
          DatanodeDetails dn = pipeline.getFirstNode();

          if (failureLocation == FailureLocation.READ_CHUNK && dn.equals(failingPeerDetails)) {
            throw new IOException("Simulated peer failure for testing in readChunk");
          }

          return dnMap.get(dn).readChunk(blockId, chunkInfo, checksumValidators);
        });

    containerProtocolMock.when(() -> ContainerProtocolCalls.toValidatorList(any())).thenCallRealMethod();
  }

  /**
   * This class wraps a KeyValueHandler instance with just enough features to test its reconciliation functionality.
   */
  private static class MockDatanode {
    private final KeyValueHandler handler;
    private final DatanodeDetails dnDetails;
    private final OnDemandContainerScanner onDemandScanner;
    private final ContainerSet containerSet;
    private final OzoneConfiguration conf;

    private final Logger log;

    MockDatanode(DatanodeDetails dnDetails, Path tempDir) throws IOException {
      this.dnDetails = dnDetails;
      log = LoggerFactory.getLogger("mock-datanode-" + dnDetails.getHostName());
      Path dataVolume = Paths.get(tempDir.toString(), dnDetails.getHostName(), "data");
      Path metadataVolume = Paths.get(tempDir.toString(), dnDetails.getHostName(), "metadata");

      this.conf = new OzoneConfiguration();
      conf.set(HDDS_DATANODE_DIR_KEY, dataVolume.toString());
      conf.set(OZONE_METADATA_DIRS, metadataVolume.toString());

      containerSet = newContainerSet();
      MutableVolumeSet volumeSet = createVolumeSet();
      handler = ContainerTestUtils.getKeyValueHandler(conf, dnDetails.getUuidString(), containerSet, volumeSet,
          spy(new ContainerChecksumTreeManager(conf)));
      handler.setClusterID(CLUSTER_ID);

      ContainerController controller = new ContainerController(containerSet,
          Collections.singletonMap(ContainerProtos.ContainerType.KeyValueContainer, handler));
      onDemandScanner = new OnDemandContainerScanner(
          conf.getObject(ContainerScannerConfiguration.class), controller);
      // Register the on-demand container scanner with the container set used by the KeyValueHandler.
      containerSet.registerOnDemandScanner(onDemandScanner);
    }

    public DatanodeDetails getDnDetails() {
      return dnDetails;
    }

    public KeyValueHandler getHandler() {
      return handler;
    }

    /**
     * @throws IOException for general IO errors accessing the checksum file
     * @throws java.io.FileNotFoundException When the checksum file does not exist.
     */
    public ContainerProtos.GetContainerChecksumInfoResponseProto getChecksumInfo(long containerID) throws IOException {
      KeyValueContainer container = getContainer(containerID);
      ByteString checksumInfo = handler.getChecksumManager().getContainerChecksumInfo(container.getContainerData());
      return ContainerProtos.GetContainerChecksumInfoResponseProto.newBuilder()
          .setContainerID(containerID)
          .setContainerChecksumInfo(checksumInfo)
          .build();
    }

    /**
     * Verifies that the data checksum on disk matches the one in memory, and returns the data checksum.
     */
    public long checkAndGetDataChecksum(long containerID) {
      KeyValueContainer container = getContainer(containerID);
      KeyValueContainerData containerData = container.getContainerData();
      long dataChecksum = 0;
      try {
        ContainerProtos.ContainerChecksumInfo containerChecksumInfo = handler.getChecksumManager()
            .read(containerData);
        dataChecksum = containerChecksumInfo.getContainerMerkleTree().getDataChecksum();
        verifyAllDataChecksumsMatch(containerData, conf);
      } catch (IOException ex) {
        fail("Failed to read container checksum from disk", ex);
      }
      log.info("Retrieved data checksum {} from container {}", checksumToString(dataChecksum), containerID);
      return dataChecksum;
    }

    public ContainerProtos.GetBlockResponseProto getBlock(BlockID blockID) throws IOException {
      KeyValueContainer container = getContainer(blockID.getContainerID());
      ContainerProtos.BlockData blockData = handler.getBlockManager().getBlock(container, blockID).getProtoBufMessage();
      return ContainerProtos.GetBlockResponseProto.newBuilder()
          .setBlockData(blockData)
          .build();
    }

    public ContainerProtos.ReadChunkResponseProto readChunk(ContainerProtos.DatanodeBlockID blockId,
        ContainerProtos.ChunkInfo chunkInfo, List<XceiverClientSpi.Validator> validators) throws IOException {
      KeyValueContainer container = getContainer(blockId.getContainerID());
      ContainerProtos.ReadChunkResponseProto readChunkResponseProto =
          ContainerProtos.ReadChunkResponseProto.newBuilder()
              .setBlockID(blockId)
              .setChunkData(chunkInfo)
              .setData(handler.getChunkManager().readChunk(container, BlockID.getFromProtobuf(blockId),
                  ChunkInfo.getFromProtoBuf(chunkInfo), null).toByteString())
              .build();
      verifyChecksums(readChunkResponseProto, blockId, chunkInfo, validators);
      return readChunkResponseProto;
    }

    public void verifyChecksums(ContainerProtos.ReadChunkResponseProto readChunkResponseProto,
        ContainerProtos.DatanodeBlockID blockId, ContainerProtos.ChunkInfo chunkInfo,
        List<XceiverClientSpi.Validator> validators) throws IOException {
      assertFalse(validators.isEmpty());
      ContainerProtos.ContainerCommandRequestProto requestProto =
          ContainerProtos.ContainerCommandRequestProto.newBuilder()
              .setCmdType(ContainerProtos.Type.ReadChunk)
              .setContainerID(blockId.getContainerID())
              .setDatanodeUuid(dnDetails.getUuidString())
              .setReadChunk(
                  ContainerProtos.ReadChunkRequestProto.newBuilder()
                      .setBlockID(blockId)
                      .setChunkData(chunkInfo)
                      .build())
              .build();
      ContainerProtos.ContainerCommandResponseProto responseProto =
          ContainerProtos.ContainerCommandResponseProto.newBuilder()
              .setCmdType(ContainerProtos.Type.ReadChunk)
              .setResult(ContainerProtos.Result.SUCCESS)
              .setReadChunk(readChunkResponseProto).build();
      for (XceiverClientSpi.Validator function : validators) {
        function.accept(requestProto, responseProto);
      }
    }

    public KeyValueContainer getContainer(long containerID) {
      return (KeyValueContainer) containerSet.getContainer(containerID);
    }

    /**
     * Triggers a synchronous scan of the container. This method will block until the scan completes.
     */
    public void scanContainer(long containerID) {
      Optional<Future<?>> scanFuture = onDemandScanner.scanContainerWithoutGap(containerSet.getContainer(containerID),
          TEST_SCAN);
      assertTrue(scanFuture.isPresent());

      try {
        scanFuture.get().get();
      } catch (InterruptedException | ExecutionException e) {
        fail("On demand container scan failed", e);
      }
    }

    public int getOnDemandScanCount() {
      return onDemandScanner.getMetrics().getNumContainersScanned();
    }

    public void resetOnDemandScanCount() {
      onDemandScanner.getMetrics().resetNumContainersScanned();
    }

    public void reconcileContainerSuccess(DNContainerOperationClient client, Collection<DatanodeDetails> peers,
        long containerID) {
      try {
        reconcileContainer(client, peers, containerID);
      } catch (IOException ex) {
        fail("Container reconciliation failed", ex);
      }
    }

    public void reconcileContainer(DNContainerOperationClient client, Collection<DatanodeDetails> peers,
        long containerID) throws IOException {
      log.info("Beginning reconciliation on this mock datanode");
      handler.reconcileContainer(client, containerSet.getContainer(containerID), peers);
    }

    /**
     * Create a container with the specified number of blocks. Block data is human-readable so the block files can be
     * inspected when debugging the test.
     */
    public void addContainerWithBlocks(long containerId, int blocks) throws Exception {
      ContainerProtos.CreateContainerRequestProto createRequest =
          ContainerProtos.CreateContainerRequestProto.newBuilder()
              .setContainerType(ContainerProtos.ContainerType.KeyValueContainer)
              .build();
      ContainerProtos.ContainerCommandRequestProto request =
          ContainerProtos.ContainerCommandRequestProto.newBuilder()
              .setCmdType(ContainerProtos.Type.CreateContainer)
              .setCreateContainer(createRequest)
              .setContainerID(containerId)
              .setDatanodeUuid(dnDetails.getUuidString())
              .build();

      handler.handleCreateContainer(request, null);
      KeyValueContainer container = getContainer(containerId);

      // Verify container is initially empty.
      File chunksPath = new File(container.getContainerData().getChunksPath());
      ContainerLayoutTestInfo.FILE_PER_BLOCK.validateFileCount(chunksPath, 0, 0);

      // Create data to put in the container.
      // Seed using the container ID so that all replicas are identical.
      RandomStringGenerator generator = new RandomStringGenerator.Builder()
          .withinRange('a', 'z')
          .usingRandom(new Random(containerId)::nextInt)
          .get();

      // This array will keep getting populated with new bytes for each chunk.
      byte[] chunkData = new byte[CHUNK_LEN];
      int bytesPerChecksum = 2 * (int) OzoneConsts.KB;

      // Add data to the container.
      List<ContainerProtos.ChunkInfo> chunkList = new ArrayList<>();
      for (int i = 0; i < blocks; i++) {
        BlockID blockID = new BlockID(containerId, i);
        BlockData blockData = new BlockData(blockID);

        chunkList.clear();
        for (long chunkCount = 0; chunkCount < CHUNKS_PER_BLOCK; chunkCount++) {
          String chunkName = "chunk" + chunkCount;
          long offset = chunkCount * chunkData.length;
          ChunkInfo info = new ChunkInfo(chunkName, offset, chunkData.length);

          // Generate data for the chunk and compute its checksum.
          // Data is generated as one ascii character per line, so block files are human-readable if further
          // debugging is needed.
          for (int c = 0; c < chunkData.length; c += 2) {
            chunkData[c] = (byte)generator.generate(1).charAt(0);
            chunkData[c + 1] = (byte)'\n';
          }

          Checksum checksum = new Checksum(ContainerProtos.ChecksumType.CRC32, bytesPerChecksum);
          ChecksumData checksumData = checksum.computeChecksum(chunkData);
          info.setChecksumData(checksumData);
          // Write chunk and checksum into the container.
          chunkList.add(info.getProtoBufMessage());
          handler.getChunkManager().writeChunk(container, blockID, info,
              ByteBuffer.wrap(chunkData), WRITE_STAGE);
        }
        handler.getChunkManager().finishWriteChunks(container, blockData);
        blockData.setChunks(chunkList);
        blockData.setBlockCommitSequenceId(i);
        handler.getBlockManager().putBlock(container, blockData);
      }
      ContainerLayoutTestInfo.FILE_PER_BLOCK.validateFileCount(chunksPath, blocks, (long) blocks * CHUNKS_PER_BLOCK);
      container.markContainerForClose();
      handler.closeContainer(container);
    }

    @Override
    public String toString() {
      return dnDetails.toString();
    }

    /**
     * Returns a list of all blocks in the container sorted numerically by blockID.
     * For example, the unsorted list would have the first blocks as 1, 10, 11...
     * The list returned by this method would have the first blocks as 1, 2, 3...
     */
    private List<BlockData> getSortedBlocks(KeyValueContainer container) throws IOException {
      List<BlockData> blockDataList = handler.getBlockManager().listBlock(container, -1, 100);
      blockDataList.sort(Comparator.comparingLong(BlockData::getLocalID));
      return blockDataList;
    }

    /**
     * Introduce corruption in the container.
     * 1. Delete blocks from the container.
     * 2. Corrupt chunks at an offset.
     * If revers is true, the blocks and chunks are deleted in reverse order.
     */
    public void introduceCorruption(long containerID, int numBlocksToRemove, int numChunksToCorrupt, boolean reverse)
        throws IOException {
      KeyValueContainer container = getContainer(containerID);
      KeyValueContainerData containerData = container.getContainerData();
      // Simulate missing blocks
      try (DBHandle handle = BlockUtils.getDB(containerData, conf);
           BatchOperation batch = handle.getStore().getBatchHandler().initBatchOperation()) {
        List<BlockData> blockDataList = getSortedBlocks(container);
        int size = blockDataList.size();
        for (int i = 0; i < numBlocksToRemove; i++) {
          BlockData blockData = reverse ? blockDataList.get(size - 1 - i) : blockDataList.get(i);
          File blockFile = TestContainerCorruptions.getBlock(container, blockData.getBlockID().getLocalID());
          Assertions.assertTrue(blockFile.delete());
          handle.getStore().getBlockDataTable().deleteWithBatch(batch,
              containerData.getBlockKey(blockData.getLocalID()));
          log.info("Deleting block {} from container {}", blockData.getBlockID().getLocalID(), containerID);
        }
        handle.getStore().getBatchHandler().commitBatchOperation(batch);
        // Check that the correct number of blocks were deleted.
        blockDataList = getSortedBlocks(container);
        assertEquals(numBlocksToRemove, size - blockDataList.size());
      }

      // Corrupt chunks at an offset.
      List<BlockData> blockDataList = getSortedBlocks(container);
      int size = blockDataList.size();
      for (int i = 0; i < numChunksToCorrupt; i++) {
        int blockIndex = reverse ? size - 1 - (i % size) : i % size;
        BlockData blockData = blockDataList.get(blockIndex);
        int chunkIndex = i / size;
        File blockFile = TestContainerCorruptions.getBlock(container, blockData.getBlockID().getLocalID());
        List<ContainerProtos.ChunkInfo> chunks = new ArrayList<>(blockData.getChunks());
        ContainerProtos.ChunkInfo chunkInfo = chunks.remove(chunkIndex);
        corruptFileAtOffset(blockFile, chunkInfo.getOffset(), chunkInfo.getLen());
        log.info("Corrupting block {} at offset {} in container {}", blockData.getBlockID().getLocalID(),
            chunkInfo.getOffset(), containerID);
      }
    }

    private MutableVolumeSet createVolumeSet() throws IOException {
      MutableVolumeSet volumeSet = new MutableVolumeSet(dnDetails.getUuidString(), conf, null,
          StorageVolume.VolumeType.DATA_VOLUME, null);
      createDbInstancesForTestIfNeeded(volumeSet, CLUSTER_ID, CLUSTER_ID, conf);
      return volumeSet;
    }

    /**
     * Overwrite the file with random bytes at an offset within the given length.
     */
    private static void corruptFileAtOffset(File file, long offset, long chunkLength) {
      try {
        final int fileLength = (int) file.length();
        assertTrue(fileLength >= offset + chunkLength);
        final int chunkEnd = (int)(offset + chunkLength);

        Path path = file.toPath();
        final byte[] original = IOUtils.readFully(Files.newInputStream(path), fileLength);

        // Corrupt the last byte and middle bytes of the block. The scanner should log this as two errors.
        final byte[] corruptedBytes = Arrays.copyOf(original, fileLength);
        corruptedBytes[chunkEnd - 1] = (byte) (original[chunkEnd - 1] << 1);
        final long chunkMid = offset + (chunkLength - offset) / 2;
        corruptedBytes[(int) (chunkMid / 2)] = (byte) (original[(int) (chunkMid / 2)] << 1);


        Files.write(path, corruptedBytes,
            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);

        assertThat(IOUtils.readFully(Files.newInputStream(path), fileLength))
            .isEqualTo(corruptedBytes)
            .isNotEqualTo(original);
      } catch (IOException ex) {
        // Fail the test.
        throw new UncheckedIOException(ex);
      }
    }
  }
}
