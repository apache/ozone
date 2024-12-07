package org.apache.hadoop.ozone.container.ec.reconstruction;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.storage.ECBlockOutputStream;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class ECValidator {

  private static final Logger LOG =
    LoggerFactory.getLogger(ECValidator.class);
  private final boolean isValidationEnabled;
  private Collection<Integer> reconstructionIndexes;
  private final int parityCount;
  private long blockLength;
  private final ECReplicationConfig ecReplicationConfig;
  private int ecChunkSize;

  ECValidator(OzoneClientConfig config, ECReplicationConfig ecReplConfig) {
    // We fetch the configuration value beforehand to avoid re-fetching on every validation call
    isValidationEnabled = config.getEcReconstructionValidation();
    ecReplicationConfig = ecReplConfig;
    parityCount = ecReplConfig.getParity();
    ecChunkSize = ecReplConfig.getEcChunkSize();
  }

  public void setReconstructionIndexes(Collection<Integer> reconstructionIndexes) {
    this.reconstructionIndexes = reconstructionIndexes;
  }

  public void setBlockLength(long blockLength) {
    this.blockLength = blockLength;
  }

  /**
   * Validate the expected checksum data for a chunk with the corresponding checksum in original stripe checksum
   * Note: The stripe checksum is a combination of all the checksums of all the chunks in the stripe
   * @param recreatedChunkChecksum Stores the {@link ContainerProtos.ChecksumData} of the recreated chunk to verify
   * @param stripeChecksum         Stores the {@link ByteBuffer} of stripe checksum
   * @param chunkIndex             Stores the index of the recreated chunk we are comparing
   * @param checksumSize           Stores the length of the stripe checksum
   * @throws OzoneChecksumException If there is a mismatch in the recreated chunk vs stripe checksum, or if there is any
   *                                internal error while performing {@link ByteBuffer} operations
   */
  private void validateChecksumInStripe(ContainerProtos.ChecksumData recreatedChunkChecksum,
                                        ByteBuffer stripeChecksum, int chunkIndex, int checksumSize)
    throws OzoneChecksumException {

    int bytesPerChecksum = recreatedChunkChecksum.getBytesPerChecksum();
    int parityLength = (int) (Math.ceil((double)ecChunkSize / bytesPerChecksum) * 4L * parityCount);
    // Ignore the parity bits
    stripeChecksum.limit(checksumSize - parityLength);

    // If we have a 100 bytes per checksum, and a chunk of size 1000 bytes, it means there are total 10 checksums
    // for each chunk that is present. So the 1st chunk will have 10 checksums together to form a single chunk checksum.
    // For each chunk we will have:
    //    Checksum of length = (chunkIdx * numOfChecksumPerChunk)
    //    Number of Checksums per Chunk = (chunkSize / bytesPerChecksum)
    // So the checksum should start from (numOfBytesPerChecksum * (chunkIdx * numOfChecksumPerChunk)

    int checksumIdxStart = (ecChunkSize * chunkIndex);

    stripeChecksum.position(checksumIdxStart);
    ByteBuffer chunkChecksum = recreatedChunkChecksum.getChecksums(0).asReadOnlyByteBuffer();
    while (chunkChecksum.hasRemaining()) {
      try {
        int recreatedChunkChecksumByte = chunkChecksum.getInt();
        int expectedStripeChecksumByte = stripeChecksum.getInt();
        if (recreatedChunkChecksumByte != expectedStripeChecksumByte) {
          throw new OzoneChecksumException(
              String.format("Mismatch in checksum for recreated data: %s and existing stripe checksum: %s",
                  recreatedChunkChecksumByte, expectedStripeChecksumByte));
        }
      } catch (BufferUnderflowException bue) {
        throw new OzoneChecksumException(
            String.format("No more data to fetch from the stripe checksum at position: %s",
                stripeChecksum.position()));
      }
    }
  }

  /**
   * Get the block from the BlockData which contains the checksum information
   * @param blockDataGroup An array of {@link BlockData} which contains all the blocks in a Datanode
   * @return The block which contains the checksum information
   */
  private BlockData getChecksumBlockData(BlockData[] blockDataGroup) {
    BlockData checksumBlockData = null;
    // Reverse traversal as all parity bits will have checksumBytes
    for (int i = blockDataGroup.length - 1; i >= 0; i--) {
      BlockData blockData = blockDataGroup[i];
      if (null == blockData) {
        continue;
      }

      List<ContainerProtos.ChunkInfo> chunks = blockData.getChunks();
      if (null != chunks && !(chunks.isEmpty())) {
        if (chunks.get(0).hasStripeChecksum()) {
          checksumBlockData = blockData;
          break;
        }
      }
    }

    return checksumBlockData;
  }

  /**
   * Helper function to validate the checksum between recreated data and
   * @param ecBlockOutputStream A {@link ECBlockOutputStream} instance that stores
   *                            the reconstructed index ECBlockOutputStream
   * @throws OzoneChecksumException if the recreated checksum and the block checksum doesn't match
   */
  public void validateChecksum(ECBlockOutputStream ecBlockOutputStream, BlockData[] blockDataGroup)
      throws OzoneChecksumException{
    if (isValidationEnabled) {

      //Checksum will be stored in the 1st chunk and parity chunks
      List<ContainerProtos.ChunkInfo> recreatedChunks = ecBlockOutputStream.getContainerBlockData().getChunksList();
      BlockData checksumBlockData = getChecksumBlockData(blockDataGroup);
      if (null == checksumBlockData) {
        throw new OzoneChecksumException("Could not find checksum data in any index for blockDataGroup while validating");
      }
      List<ContainerProtos.ChunkInfo> checksumBlockChunks = checksumBlockData.getChunks();

      for (int chunkIdx = 0; chunkIdx < recreatedChunks.size(); chunkIdx++) {
        ByteString stripeChecksum = checksumBlockChunks.get(chunkIdx).getStripeChecksum();
        validateChecksumInStripe(
            recreatedChunks.get(chunkIdx).getChecksumData(),
            stripeChecksum.asReadOnlyByteBuffer(), stripeChecksum.size(), chunkIdx
        );
      }
    } else {
      LOG.debug("Checksum validation was disabled, skipping check");
    }
  }
}
