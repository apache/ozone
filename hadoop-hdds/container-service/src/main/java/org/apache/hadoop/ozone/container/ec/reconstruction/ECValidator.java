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

  ECValidator(OzoneClientConfig config, ECReplicationConfig ecReplConfig) {
    // We fetch the configuration value beforehand to avoid re-fetching on every validation call
    isValidationEnabled = config.getEcReconstructionValidation();
    ecReplicationConfig = ecReplConfig;
    parityCount = ecReplConfig.getParity();
  }

  public void setReconstructionIndexes(Collection<Integer> reconstructionIndexes) {
    this.reconstructionIndexes = reconstructionIndexes;
  }

  public void setBlockLength(long blockLength) {
    this.blockLength = blockLength;
  }

  private boolean validateChecksumInStripe(ContainerProtos.ChecksumData checksumData, ByteString stripeChecksum)
    throws OzoneChecksumException{

    int bytesPerChecksum = checksumData.getBytesPerChecksum();
    ByteString checksum = stripeChecksum.substring();
  }

  private BlockData getChecksumBlockData(BlockData[] blockDataGroup) {
    BlockData checksumBlockData = null;
    // Reverse traversal as all parity bits will have checmsumBytes
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

      for (int i = 0; i < recreatedChunks.size(); i++) {
        validateChecksumInStripe(recreatedChunks.get(i).getChecksumData(), checksumBlockChunks.get(i).getStripeChecksum());
      }


    } else {
      LOG.debug("Checksum validation was disabled, skipping check");
    }
  }
}
