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
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.WRITE_STAGE;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.createDbInstancesForTestIfNeeded;
import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockInputStream;
import org.apache.hadoop.hdds.scm.storage.ChunkInputStream;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactoryImpl;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeWriter;
import org.apache.hadoop.ozone.container.checksum.DNContainerOperationClient;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression test for the BCSID high-water bug on the holed-block reconciliation path
 * (KeyValueHandler.reconcileChunksPerBlock).
 *
 * <p>Scenario reproduced here:
 * <ul>
 *   <li>A closed local replica holds block L with only the offset-0 chunk; its block and container
 *       blockCommitSequenceId (BCSID) are both 1.</li>
 *   <li>A peer is ahead at BCSID 99 and advertises a chunk merkle list {CHUNK_LEN, 3*CHUNK_LEN}.
 *       The chunk at 2*CHUNK_LEN is absent, so 3*CHUNK_LEN sits past a hole. (A peer's scanner
 *       legitimately omits missing chunks from its merkle tree, so a healthy peer can advertise a
 *       gapped list.)</li>
 * </ul>
 *
 * <p>Reconciliation ingests the chunk at CHUNK_LEN (its predecessor, offset 0, is present locally),
 * then reaches 3*CHUNK_LEN whose predecessor 2*CHUNK_LEN is missing and stops at the hole break.
 * The block is therefore incomplete. The method contract states the BCSID is advanced to the peer's
 * value only when the entire block is read and written successfully, so on a holed repair the
 * BCSID must stay at the local value.
 *
 * <p>This test mocks only the peer side (the BlockInputStream and its single served chunk) and
 * exercises the real reconcileChunksPerBlock against a real closed container. Before the fix the
 * BCSID is advanced to 99 and the assertions below fail; after the fix the BCSID stays at 1.
 */
public class TestReconcileChunksPerBlockHoleBcsId {

  @TempDir
  private Path tempDir;

  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static final long CONTAINER_ID = 100L;
  private static final long LOCAL_ID = 0L;
  // 2 KiB chunks so the offsets line up with the description: ingested chunk at 2048,
  // hole at 4096, skipped chunk past the hole at 6144.
  private static final int CHUNK_LEN = 2 * (int) OzoneConsts.KB;
  private static final int BYTES_PER_CHECKSUM = 2 * (int) OzoneConsts.KB;
  private static final long LOCAL_BCSID = 1L;
  private static final long PEER_BCSID = 99L;

  // conf and volumeSet are fields (not locals) because teardown needs them to release the
  // RocksDB cache and the volumes opened in setup.
  private OzoneConfiguration conf;
  private MutableVolumeSet volumeSet;
  private ContainerSet containerSet;
  private KeyValueHandler handler;
  private KeyValueContainer container;
  private DNContainerOperationClient dnClient;
  private Pipeline peerPipeline;

  @BeforeEach
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    Path dataVolume = Paths.get(tempDir.toString(), "data");
    Path metadataVolume = Paths.get(tempDir.toString(), "metadata");
    conf.set(HDDS_DATANODE_DIR_KEY, dataVolume.toString());
    conf.set(OZONE_METADATA_DIRS, metadataVolume.toString());

    containerSet = newContainerSet();
    DatanodeDetails localDn = randomDatanodeDetails();
    volumeSet = new MutableVolumeSet(localDn.getUuidString(), conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    createDbInstancesForTestIfNeeded(volumeSet, CLUSTER_ID, CLUSTER_ID, conf);

    handler = ContainerTestUtils.getKeyValueHandler(conf, localDn.getUuidString(), containerSet, volumeSet,
        new org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager(conf));
    handler.setClusterID(CLUSTER_ID);

    container = createClosedContainerWithOffsetZeroChunk();

    dnClient = new DNContainerOperationClient(conf, null, null);
    peerPipeline = singleNodePipeline(randomDatanodeDetails());
  }

  @AfterEach
  public void teardown() throws Exception {
    // Release everything setup opened so threads, clients, and RocksDB handles do not leak across the
    // suite. Guarded because setup may have failed partway. DNContainerOperationClient owns an
    // XceiverClientManager; the handler owns chunk/block managers; the volume set owns the RocksDB cache.
    if (dnClient != null) {
      dnClient.close();
    }
    if (handler != null) {
      handler.stop();
    }
    if (volumeSet != null) {
      volumeSet.shutdown();
    }
    if (conf != null) {
      BlockUtils.shutdownCache(conf);
    }
  }

  /**
   * Builds a real closed container holding block L with a single chunk at offset 0, BCSID 1.
   */
  private KeyValueContainer createClosedContainerWithOffsetZeroChunk() throws Exception {
    ContainerProtos.ContainerCommandRequestProto createRequest =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.CreateContainer)
            .setContainerID(CONTAINER_ID)
            .setDatanodeUuid(UUID.randomUUID().toString())
            .setCreateContainer(ContainerProtos.CreateContainerRequestProto.newBuilder()
                .setContainerType(ContainerProtos.ContainerType.KeyValueContainer)
                .build())
            .build();
    handler.handleCreateContainer(createRequest, null);
    KeyValueContainer kvContainer =
        (KeyValueContainer) containerSet.getContainer(CONTAINER_ID);

    BlockID blockID = new BlockID(CONTAINER_ID, LOCAL_ID);
    byte[] chunkData = new byte[CHUNK_LEN];
    Arrays.fill(chunkData, (byte) 'a');

    ChunkInfo chunkAtZero = new ChunkInfo("chunk0", 0, CHUNK_LEN);
    chunkAtZero.setChecksumData(checksumOf(chunkData));
    handler.getChunkManager().writeChunk(kvContainer, blockID, chunkAtZero,
        ByteBuffer.wrap(chunkData), WRITE_STAGE);
    handler.getChunkManager().finishWriteChunks(kvContainer, new BlockData(blockID));

    BlockData blockData = new BlockData(blockID);
    blockData.setChunks(Collections.singletonList(chunkAtZero.getProtoBufMessage()));
    blockData.setBlockCommitSequenceId(LOCAL_BCSID);
    handler.getBlockManager().putBlock(kvContainer, blockData);

    kvContainer.markContainerForClose();
    handler.closeContainer(kvContainer);
    return kvContainer;
  }

  @Test
  public void holeExitMustNotAdvanceBcsIdToPeerValue() throws Exception {
    // Precondition: local replica is at BCSID 1, well below the peer's 99.
    BlockData localBefore = handler.getBlockManager().getBlock(container, new BlockID(CONTAINER_ID, LOCAL_ID));
    assertEquals(LOCAL_BCSID, localBefore.getBlockCommitSequenceId());
    assertEquals(LOCAL_BCSID, container.getContainerData().getBlockCommitSequenceId());

    // The peer advertises a merkle list with a hole: {CHUNK_LEN, 3*CHUNK_LEN}. 2*CHUNK_LEN is omitted,
    // so 3*CHUNK_LEN sits past a hole relative to what the local replica can place contiguously.
    List<ContainerProtos.ChunkMerkleTree> peerChunkList = Arrays.asList(
        chunkMerkleTree(CHUNK_LEN),
        chunkMerkleTree(3L * CHUNK_LEN));

    // Mock only the peer side. getStreamBlockData advertises BCSID 99; the single chunk stream serves the
    // contiguous chunk at CHUNK_LEN that reconciliation ingests before it reaches the hole.
    installMockedPeerStream();

    ByteBuffer chunkByteBuffer = ByteBuffer.allocate(CHUNK_LEN);
    handler.reconcileChunksPerBlock(container, peerPipeline, dnClient, LOCAL_ID, peerChunkList,
        new ContainerMerkleTreeWriter(), chunkByteBuffer);

    // A hole remains, so the block is incomplete: the BCSID must stay at the local value, not advance to
    // the peer's. Asserting the exact local value (not merely "not the peer value") also catches a BCSID
    // that drifts to any other wrong value, e.g. 0.
    BlockData localAfter = handler.getBlockManager().getBlock(container, new BlockID(CONTAINER_ID, LOCAL_ID));
    assertEquals(LOCAL_BCSID, localAfter.getBlockCommitSequenceId(),
        "block BCSID must stay at the local value (" + LOCAL_BCSID + ") because the chunk at offset "
            + (3L * CHUNK_LEN) + " past the hole at offset " + (2L * CHUNK_LEN) + " was never ingested");
    assertEquals(LOCAL_BCSID, container.getContainerData().getBlockCommitSequenceId(),
        "container BCSID must stay at the local value (" + LOCAL_BCSID + ") on a holed, incomplete block");
  }

  /**
   * Stubs the block input stream factory to return a mocked peer stream that advertises BCSID 99 and
   * serves one contiguous chunk at offset CHUNK_LEN.
   */
  private void installMockedPeerStream() throws Exception {
    ContainerProtos.BlockData peerBlockData = ContainerProtos.BlockData.newBuilder()
        .setBlockID(ContainerProtos.DatanodeBlockID.newBuilder()
            .setContainerID(CONTAINER_ID)
            .setLocalID(LOCAL_ID)
            .setBlockCommitSequenceId(PEER_BCSID)
            .build())
        .build();

    byte[] peerChunkData = new byte[CHUNK_LEN];
    Arrays.fill(peerChunkData, (byte) 'b');
    ChunkInfo peerChunkAtChunkLen = new ChunkInfo("peer-chunk", CHUNK_LEN, CHUNK_LEN);
    peerChunkAtChunkLen.setChecksumData(checksumOf(peerChunkData));

    ChunkInputStream mockChunkStream = mock(ChunkInputStream.class);
    when(mockChunkStream.getChunkInfo()).thenReturn(peerChunkAtChunkLen.getProtoBufMessage());

    BlockInputStream mockStream = mock(BlockInputStream.class);
    when(mockStream.getStreamBlockData()).thenReturn(peerBlockData);
    when(mockStream.getChunkStreams()).thenReturn(Collections.singletonList(mockChunkStream));
    when(mockStream.getChunkIndex()).thenReturn(0);
    when(mockStream.read(any(ByteBuffer.class))).thenAnswer(invocation -> {
      ByteBuffer buffer = invocation.getArgument(0);
      int remaining = buffer.remaining();
      buffer.put(peerChunkData, 0, remaining);
      return remaining;
    });

    BlockInputStreamFactoryImpl mockFactory = mock(BlockInputStreamFactoryImpl.class);
    when(mockFactory.createBlockInputStream(any(), any(), any(), any(), any(), any()))
        .thenReturn(mockStream);
    handler.setBlockInputStreamFactory(mockFactory);
  }

  private static ContainerProtos.ChunkMerkleTree chunkMerkleTree(long offset) {
    return ContainerProtos.ChunkMerkleTree.newBuilder()
        .setOffset(offset)
        .setLength(CHUNK_LEN)
        .setChecksumMatches(true)
        .build();
  }

  private static ChecksumData checksumOf(byte[] data) throws Exception {
    Checksum checksum = new Checksum(ContainerProtos.ChecksumType.CRC32, BYTES_PER_CHECKSUM);
    return checksum.computeChecksum(data);
  }

  private static Pipeline singleNodePipeline(DatanodeDetails dn) {
    return Pipeline.newBuilder()
        .setId(org.apache.hadoop.hdds.scm.pipeline.PipelineID.randomId())
        .setReplicationConfig(
            org.apache.hadoop.hdds.client.StandaloneReplicationConfig.getInstance(
                org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE))
        .setState(Pipeline.PipelineState.CLOSED)
        .setNodes(Collections.singletonList(dn))
        .build();
  }
}
