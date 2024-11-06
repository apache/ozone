package org.apache.hadoop.ozone.container.ec.reconstruction;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.storage.ECBlockOutputStream;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ECValidator {

  private static final Logger LOG =
    LoggerFactory.getLogger(ECValidator.class);
  private Checksum checksum = null;
  private final boolean isValidationEnabled;

  ECValidator(OzoneClientConfig config) {
    this.checksum = new Checksum(config.getChecksumType(), config.getBytesPerChecksum());
    // We fetch the configuration value beforehand to avoid re-fetching on every validation call
    isValidationEnabled = config.getEcReconstructionValidation();
  }

  /**
   * Helper function to validate the checksum between recreated data and
   * @param buf  A {@link ByteBuffer} instance that stores the chunk
   * @param ecBlockOutputStream A {@link ECBlockOutputStream} instance that stores
   *                            the reconstructed index ECBlockOutputStream
   * @param idx Used to store the index at which data was recreated
   * @throws OzoneChecksumException if the recreated checksum and the block checksum doesn't match
   */
  public void validateBuffer(ByteBuffer buf, ECBlockOutputStream ecBlockOutputStream, int idx)
      throws OzoneChecksumException{
    if (isValidationEnabled) {
      try (ChunkBuffer chunk = ChunkBuffer.wrap(buf)) {
        //Checksum will be stored in the 1st chunk and parity chunks
        ContainerProtos.ChecksumData stripeChecksum = ecBlockOutputStream.getContainerBlockData()
          .getChunks(0).getChecksumData();
        ContainerProtos.ChecksumData chunkChecksum = checksum.computeChecksum(chunk).getProtoBufMessage();
        if (stripeChecksum.getChecksums(idx) != chunkChecksum.getChecksums(0)) {
          LOG.info("Checksum mismatched between recreated chunk and re-created chunk");
          throw new OzoneChecksumException("Inconsistent checksum for re-created chunk and original chunk");
        }
      }
    } else {
      LOG.debug("Checksum validation was disabled, skipping check");
    }
  }
}
