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
import static org.junit.jupiter.api.Assertions.assertNull;
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
import org.apache.hadoop.ozone.container.checksum.ContainerDiffReport;
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
 * <p>Two sibling tests cover the trailing-hole variants of the same bug, where the incompleteness is
 * invisible to the repair loop: a trailing unhealthy peer chunk dropped from the diff by
 * reportChunkIfHealthy, and a trailing unhealthy chunk skipped in-loop via continue. In both, every
 * listed chunk repairs cleanly, so only the commit-time comparison against the peer's committed
 * BlockData chunk list keeps the BCSID at the local value. A positive-control test asserts adoption
 * still occurs when the local block fully covers the peer's committed chunk list.
 *
 * <p>A cross-block test covers the container-level variant: block coverage licenses only the block's own
 * BCSID, while the container BCSID quantifies over all blocks and advances once per reconciliation round,
 * only when every repaired block in the round ended fully covered.
 *
 * <p>These tests mock only the peer side (the BlockInputStream and its single served chunk) and
 * exercise the real reconcileChunksPerBlock against a real closed container. Before the fix the
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
    installMockedPeerStream(peerBlockDataWithChunks());

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

  @Test
  public void trailingUnhealthyPeerChunkFilteredFromDiffMustNotAdvanceBcsId() throws Exception {
    // Local merkle tree matches the real container from setup: only the healthy chunk at offset 0. The peer is at
    // BCSID 99 with chunks at 0 and CHUNK_LEN healthy and a TRAILING chunk at 2*CHUNK_LEN its scanner marked
    // unhealthy.
    ContainerProtos.ChunkInfo chunk0 = chunkProto("chunk0", 0, (byte) 'a');
    ContainerProtos.ChunkInfo peerChunk1 = chunkProto("peer-chunk", CHUNK_LEN, (byte) 'b');
    ContainerProtos.ChunkInfo peerChunk2 = chunkProto("peer-chunk-2", 2L * CHUNK_LEN, (byte) 'c');

    ContainerMerkleTreeWriter localTree = new ContainerMerkleTreeWriter();
    localTree.addChunks(LOCAL_ID, true, chunk0);
    ContainerMerkleTreeWriter peerTree = new ContainerMerkleTreeWriter();
    peerTree.addChunks(LOCAL_ID, true, chunk0, peerChunk1);
    peerTree.addChunks(LOCAL_ID, false, peerChunk2);

    // Run the real diff. reportChunkIfHealthy drops the unhealthy trailing chunk, so the repair list holds only the
    // chunk at CHUNK_LEN -- the hole this leaves at the tail is invisible to the repair loop.
    ContainerDiffReport report = handler.getChecksumManager().diff(checksumInfo(localTree), checksumInfo(peerTree));
    List<ContainerProtos.ChunkMerkleTree> repairList = report.getMissingChunks().get(LOCAL_ID);
    assertEquals(1, repairList.size());
    assertEquals(CHUNK_LEN, repairList.get(0).getOffset());

    // The peer's committed BlockData lists all three chunks -- that is what BCSID 99 attests.
    installMockedPeerStream(peerBlockDataWithChunks(chunk0, peerChunk1, peerChunk2));

    handler.reconcileChunksPerBlock(container, peerPipeline, dnClient, LOCAL_ID, repairList,
        new ContainerMerkleTreeWriter(), ByteBuffer.allocate(CHUNK_LEN));

    // Data repair is best-effort and must still happen: the chunk at CHUNK_LEN was recovered.
    BlockData after = handler.getBlockManager().getBlock(container, new BlockID(CONTAINER_ID, LOCAL_ID));
    assertEquals(2, after.getChunks().size());
    // But the local block does not cover the peer's committed chunk list (2*CHUNK_LEN is absent), so the
    // attestation must not move.
    assertEquals(LOCAL_BCSID, after.getBlockCommitSequenceId(),
        "block BCSID must stay at the local value (" + LOCAL_BCSID + ") because the peer's trailing chunk at offset "
            + (2L * CHUNK_LEN) + " was filtered from the diff and never ingested");
    assertEquals(LOCAL_BCSID, container.getContainerData().getBlockCommitSequenceId(),
        "container BCSID must stay at the local value (" + LOCAL_BCSID + ") on a block missing a trailing chunk");
  }

  @Test
  public void trailingUnhealthyChunkInRepairListMustNotAdvanceBcsId() throws Exception {
    // The missing-block repair path passes the peer's FULL chunk list (addMissingBlock does not health-filter), so
    // an unhealthy chunk reaches the loop and is skipped via continue -- which does not fail the repair. With the
    // unhealthy chunk TRAILING there is no successor whose previousChunkPresent check could fire, so before the fix
    // allChunksSuccessful stayed true and the BCSID advanced past data that was never ingested.
    List<ContainerProtos.ChunkMerkleTree> repairList = Arrays.asList(
        chunkMerkleTree(CHUNK_LEN),
        unhealthyChunkMerkleTree(2L * CHUNK_LEN));
    installMockedPeerStream(peerBlockDataWithChunks(
        chunkProto("chunk0", 0, (byte) 'a'),
        chunkProto("peer-chunk", CHUNK_LEN, (byte) 'b'),
        chunkProto("peer-chunk-2", 2L * CHUNK_LEN, (byte) 'c')));

    handler.reconcileChunksPerBlock(container, peerPipeline, dnClient, LOCAL_ID, repairList,
        new ContainerMerkleTreeWriter(), ByteBuffer.allocate(CHUNK_LEN));

    // The healthy chunk at CHUNK_LEN was recovered; the skipped trailing chunk leaves the block incomplete.
    BlockData after = handler.getBlockManager().getBlock(container, new BlockID(CONTAINER_ID, LOCAL_ID));
    assertEquals(2, after.getChunks().size());
    assertEquals(LOCAL_BCSID, after.getBlockCommitSequenceId(),
        "block BCSID must stay at the local value (" + LOCAL_BCSID + ") because the unhealthy trailing chunk at "
            + "offset " + (2L * CHUNK_LEN) + " was skipped in-loop and never ingested");
    assertEquals(LOCAL_BCSID, container.getContainerData().getBlockCommitSequenceId(),
        "container BCSID must stay at the local value (" + LOCAL_BCSID + ") on a block missing a trailing chunk");
  }

  @Test
  public void fullCoverageMustAdvanceBcsIdToPeerValue() throws Exception {
    // Positive control for the completeness gate: when the repaired local block covers the peer's committed chunk
    // list exactly, the peer's BCSID must still be adopted. Guards against over-tightening the gate, which would
    // silently stop all BCSID convergence without failing any of the negative tests above.
    ContainerProtos.ChunkInfo chunk0 = chunkProto("chunk0", 0, (byte) 'a');
    ContainerProtos.ChunkInfo peerChunk1 = chunkProto("peer-chunk", CHUNK_LEN, (byte) 'b');
    List<ContainerProtos.ChunkMerkleTree> repairList = Collections.singletonList(chunkMerkleTree(CHUNK_LEN));
    installMockedPeerStream(peerBlockDataWithChunks(chunk0, peerChunk1));

    handler.reconcileChunksPerBlock(container, peerPipeline, dnClient, LOCAL_ID, repairList,
        new ContainerMerkleTreeWriter(), ByteBuffer.allocate(CHUNK_LEN));

    BlockData after = handler.getBlockManager().getBlock(container, new BlockID(CONTAINER_ID, LOCAL_ID));
    assertEquals(2, after.getChunks().size());
    assertEquals(PEER_BCSID, after.getBlockCommitSequenceId(),
        "block BCSID must advance to the peer value (" + PEER_BCSID + ") when the local block covers the peer's "
            + "committed chunk list");
    // The per-block repair leaves the container BCSID untouched; the round-end step advances it when every
    // repaired block in the round ended fully covered, mirroring reconcileContainerInternal.
    assertEquals(LOCAL_BCSID, container.getContainerData().getBlockCommitSequenceId(),
        "per-block repair must not touch the container BCSID");
    handler.advanceContainerBcsIdForFullyCoveredRound(container, new ContainerDiffReport(CONTAINER_ID), true,
        PEER_BCSID);
    assertEquals(PEER_BCSID, container.getContainerData().getBlockCommitSequenceId(),
        "container BCSID must advance to the peer value (" + PEER_BCSID + ") after a fully covered round");
  }

  @Test
  public void completeBlockMustNotAdvanceContainerPastIncompleteEarlierBlock() throws Exception {
    // Cross-block variant: coversPeerBlock proves completeness for ONE block, but the container BCSID
    // quantifies over ALL blocks. A fully covered block at a higher BCSID must not advance the container
    // past another block that remains incomplete at a lower BCSID.
    long incompleteBlockId = 1L;
    long completeBlockId = 2L;
    long incompleteBlockBcsId = PEER_BCSID - 1;

    // Block 1 from the peer: committed at BCSID 98 with two chunks; the trailing chunk is unhealthy, so
    // only chunk 0 is recovered and the block stays incomplete.
    ContainerProtos.ChunkInfo incompleteChunk0 = chunkProto("incomplete-0", 0, (byte) 'b');
    ContainerProtos.ChunkInfo incompleteChunk1 = chunkProto("incomplete-1", CHUNK_LEN, (byte) 'c');
    installMockedPeerStream(peerBlockDataWithChunks(incompleteBlockId, incompleteBlockBcsId,
        incompleteChunk0, incompleteChunk1), 0);
    handler.reconcileChunksPerBlock(container, peerPipeline, dnClient, incompleteBlockId,
        Arrays.asList(chunkMerkleTree(0), unhealthyChunkMerkleTree(CHUNK_LEN)),
        new ContainerMerkleTreeWriter(), ByteBuffer.allocate(CHUNK_LEN));

    BlockData incompleteAfter = handler.getBlockManager().getBlock(
        container, new BlockID(CONTAINER_ID, incompleteBlockId));
    assertEquals(1, incompleteAfter.getChunks().size());
    assertEquals(LOCAL_BCSID, container.getContainerData().getBlockCommitSequenceId());

    // Block 2 from the peer: committed at BCSID 99 with a single chunk that is fully recovered.
    ContainerProtos.ChunkInfo completeChunk0 = chunkProto("complete-0", 0, (byte) 'b');
    installMockedPeerStream(peerBlockDataWithChunks(completeBlockId, PEER_BCSID, completeChunk0), 0);
    handler.reconcileChunksPerBlock(container, peerPipeline, dnClient, completeBlockId,
        Collections.singletonList(chunkMerkleTree(0)),
        new ContainerMerkleTreeWriter(), ByteBuffer.allocate(CHUNK_LEN));

    // The complete block adopts the peer's block BCSID...
    BlockData completeAfter = handler.getBlockManager().getBlock(
        container, new BlockID(CONTAINER_ID, completeBlockId));
    assertEquals(PEER_BCSID, completeAfter.getBlockCommitSequenceId());
    // ...but the container BCSID must not advance past the incomplete earlier block: container BCSID 99
    // would attest block 1's commit at 98, whose trailing chunk this replica does not hold.
    assertEquals(LOCAL_BCSID, container.getContainerData().getBlockCommitSequenceId(),
        "container BCSID must not advance past the incomplete block");
    // The round-end advancement is suppressed as well, because the round left block 1 partially repaired --
    // this mirrors reconcileContainerInternal accumulating coverage across the per-block repairs of one round.
    handler.advanceContainerBcsIdForFullyCoveredRound(container, new ContainerDiffReport(CONTAINER_ID), false,
        PEER_BCSID);
    assertEquals(LOCAL_BCSID, container.getContainerData().getBlockCommitSequenceId(),
        "round-end container BCSID advancement must be suppressed when any block in the round stayed partial");
  }

  @Test
  public void filteredOnlyBlockMustSuppressRoundContainerAdvancement() throws Exception {
    // Round two of the trailing-unhealthy scenario: round one recovered the chunk at CHUNK_LEN, so the
    // local block now holds chunks 0-1 and only the peer's unhealthy trailing chunk still differs.
    // reportChunkIfHealthy drops that chunk from the diff, the block vanishes from the repair report
    // entirely, and no partial repair can dirty the round anymore. The round-end advancement must still
    // stay suppressed, or a clean sibling block would advance the container past the invisible block's
    // missing data.
    ContainerProtos.ChunkInfo chunk0 = chunkProto("chunk0", 0, (byte) 'a');
    ContainerProtos.ChunkInfo chunk1 = chunkProto("peer-chunk", CHUNK_LEN, (byte) 'b');
    ContainerProtos.ChunkInfo chunk2 = chunkProto("peer-chunk-2", 2L * CHUNK_LEN, (byte) 'c');

    ContainerMerkleTreeWriter localTree = new ContainerMerkleTreeWriter();
    localTree.addChunks(LOCAL_ID, true, chunk0, chunk1);
    ContainerMerkleTreeWriter peerTree = new ContainerMerkleTreeWriter();
    peerTree.addChunks(LOCAL_ID, true, chunk0, chunk1);
    peerTree.addChunks(LOCAL_ID, false, chunk2);

    ContainerDiffReport report = handler.getChecksumManager().diff(checksumInfo(localTree), checksumInfo(peerTree));
    // The block's only difference was filtered as unhealthy, so no repair will run for it this round...
    assertNull(report.getMissingChunks().get(LOCAL_ID));
    // ...and the drop is recorded on the report as the round's only evidence that the diff was incomplete.
    assertEquals(1, report.getNumUnhealthyChunksFiltered());

    // A sibling block repairs cleanly in the same round and adopts the peer's block BCSID 99.
    long completeBlockId = 2L;
    ContainerProtos.ChunkInfo completeChunk0 = chunkProto("complete-0", 0, (byte) 'b');
    installMockedPeerStream(peerBlockDataWithChunks(completeBlockId, PEER_BCSID, completeChunk0), 0);
    handler.reconcileChunksPerBlock(container, peerPipeline, dnClient, completeBlockId,
        Collections.singletonList(chunkMerkleTree(0)),
        new ContainerMerkleTreeWriter(), ByteBuffer.allocate(CHUNK_LEN));

    // Round end: every repaired block was covered, but the diff dropped an unhealthy chunk, so the diff
    // was not a complete account of what the peer's tree lists.
    handler.advanceContainerBcsIdForFullyCoveredRound(container, report, true, PEER_BCSID);
    assertEquals(LOCAL_BCSID, container.getContainerData().getBlockCommitSequenceId(),
        "container BCSID must not advance when the diff filtered an unrepairable unhealthy chunk");
  }

  /**
   * Stubs the block input stream factory to return a mocked peer stream that advertises the given
   * committed BlockData (BCSID 99) and serves one contiguous chunk at offset CHUNK_LEN.
   */
  private void installMockedPeerStream(ContainerProtos.BlockData peerBlockData) throws Exception {
    installMockedPeerStream(peerBlockData, CHUNK_LEN);
  }

  /**
   * Same as above, serving the single mocked chunk at the given offset instead of CHUNK_LEN.
   */
  private void installMockedPeerStream(ContainerProtos.BlockData peerBlockData, long servedChunkOffset)
      throws Exception {
    byte[] peerChunkData = new byte[CHUNK_LEN];
    Arrays.fill(peerChunkData, (byte) 'b');
    ChunkInfo peerChunkAtChunkLen = new ChunkInfo("peer-chunk", servedChunkOffset, CHUNK_LEN);
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

  private static ContainerProtos.ChunkMerkleTree unhealthyChunkMerkleTree(long offset) {
    return ContainerProtos.ChunkMerkleTree.newBuilder()
        .setOffset(offset)
        .setLength(CHUNK_LEN)
        .setChecksumMatches(false)
        .build();
  }

  /**
   * The peer's committed BlockData at BCSID 99 listing the given chunks. This is the metadata
   * reconcileChunksPerBlock reads via getStreamBlockData and compares against at commit time.
   */
  private static ContainerProtos.BlockData peerBlockDataWithChunks(ContainerProtos.ChunkInfo... chunks) {
    return peerBlockDataWithChunks(LOCAL_ID, PEER_BCSID, chunks);
  }

  private static ContainerProtos.BlockData peerBlockDataWithChunks(long localId, long bcsId,
      ContainerProtos.ChunkInfo... chunks) {
    return ContainerProtos.BlockData.newBuilder()
        .setBlockID(ContainerProtos.DatanodeBlockID.newBuilder()
            .setContainerID(CONTAINER_ID)
            .setLocalID(localId)
            .setBlockCommitSequenceId(bcsId)
            .build())
        .addAllChunks(Arrays.asList(chunks))
        .build();
  }

  private static ContainerProtos.ContainerChecksumInfo checksumInfo(ContainerMerkleTreeWriter tree) {
    return ContainerProtos.ContainerChecksumInfo.newBuilder()
        .setContainerID(CONTAINER_ID)
        .setContainerMerkleTree(tree.toProto())
        .build();
  }

  private static ContainerProtos.ChunkInfo chunkProto(String name, long offset, byte fill) throws Exception {
    byte[] data = new byte[CHUNK_LEN];
    Arrays.fill(data, fill);
    ChunkInfo info = new ChunkInfo(name, offset, CHUNK_LEN);
    info.setChecksumData(checksumOf(data));
    return info.getProtoBufMessage();
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
