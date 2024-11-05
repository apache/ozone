package org.apache.hadoop.ozone.container.ec.reconstruction;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.storage.ECBlockOutputStream;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ECValidator {

  private static final Logger LOG =
    LoggerFactory.getLogger(ECValidator.class);
  private Checksum checksum = null;

  ECValidator(OzoneClientConfig config) {
    this.checksum = new Checksum(config.getChecksumType(), config.getBytesPerChecksum());
  }

  public boolean validateBuffer(ByteBuffer buf, ECBlockOutputStream ecBlockOutputStream)
      throws OzoneChecksumException{
    try (ChunkBuffer chunk = ChunkBuffer.wrap(buf)) {
      //Checksum will be stored in the 1st chunk and parity chunks
      ContainerProtos.ChecksumData stripeChecksum = ecBlockOutputStream.getContainerBlockData()
        .getChunks(0).getChecksumData();
      ContainerProtos.ChecksumData chunkChecksum = checksum.computeChecksum(chunk).getProtoBufMessage();
      LOG.info("Chunk Checksum: {}, Stripe Checksum: {}", chunkChecksum, stripeChecksum);
    }
  }
}
