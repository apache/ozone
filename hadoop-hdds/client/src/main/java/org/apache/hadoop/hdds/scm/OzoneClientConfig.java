package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.ozone.OzoneConfigKeys;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ConfigGroup(prefix = "ozone.client")
public class OzoneClientConfig {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneClientConfig.class);

  @Config(key = "stream.buffer.flush.size",
      defaultValue = "16MB",
      type = ConfigType.SIZE,
      description = "Size which determines at what buffer position a partial "
          + "flush will be initiated during write. It should be a multiple of"
          + " ozone.client.stream.buffer.size",
      tags = ConfigTag.CLIENT)
  private long streamBufferFlushSize = 16 * 1024 * 1024;

  @Config(key = "stream.buffer.size",
      defaultValue = "4MB",
      type = ConfigType.SIZE,
      description = "The size of chunks the client will send to the server",
      tags = ConfigTag.CLIENT)
  private int streamBufferSize = 4 * 1024 * 1024;

  @Config(key = "stream.buffer.flush.delay",
      defaultValue = "true",
      description = "Default true, when call flush() and determine whether "
          + "the data in the current buffer is greater than ozone.client"
          + ".stream.buffer.size, if greater than then send buffer to the "
          + "datanode. You can  turn this off by setting this configuration "
          + "to false.", tags = ConfigTag.CLIENT)
  private boolean streamBufferFlushDelay = true;

  @Config(key = "stream.buffer.max.size",
      defaultValue = "32MB",
      type = ConfigType.SIZE,
      description = "Size which determines at what buffer position write call"
          + " be blocked till acknowledgement of the first partial flush "
          + "happens by all servers.",
      tags = ConfigTag.CLIENT)
  private long streamBufferMaxSize = 32 * 1024 * 1024;

  @Config(key = "ozone.client.max.retries",
      defaultValue = "5",
      description = "",
      tags = ConfigTag.CLIENT)
  private int maxRetryCount = 5;

  @Config(key = "retry.interval",
      defaultValue = "0",
      description = "",
      tags = ConfigTag.CLIENT)
  private int retryInterval = 0;

  @Config(key = "checksum.type",
      defaultValue = "CRC32",
      description = "",
      tags = ConfigTag.CLIENT)
  private String checksumType = ChecksumType.CRC32.name();

  @Config(key = "bytes.per.checksum",
      defaultValue = "1MB",
      type = ConfigType.SIZE,
      description = "",
      tags = ConfigTag.CLIENT)
  private int bytesPerChecksum = 1024 * 1024;

  @Config(key = "verify.checksum",
      defaultValue = "true",
      description = "",
      tags = ConfigTag.CLIENT)
  private boolean checksumVerify = true;

  public OzoneClientConfig() {
  }

  private void validate() {
    Preconditions.checkState(streamBufferSize > 0);
    Preconditions.checkState(streamBufferFlushSize > 0);
    Preconditions.checkState(streamBufferMaxSize > 0);

    Preconditions.checkState(streamBufferMaxSize % streamBufferFlushSize == 0,
        "expected max. buffer size (%s) to be a multiple of flush size (%s)",
        streamBufferMaxSize, streamBufferFlushSize);
    Preconditions.checkState(streamBufferFlushSize % streamBufferSize == 0,
        "expected flush size (%s) to be a multiple of buffer size (%s)",
        streamBufferFlushSize, streamBufferSize);

    if (bytesPerChecksum <
        OzoneConfigKeys.OZONE_CLIENT_BYTES_PER_CHECKSUM_MIN_SIZE) {
      LOG.warn("The checksum size ({}) is not allowed to be less than the " +
              "minimum size ({}), resetting to the minimum size.",
          bytesPerChecksum,
          OzoneConfigKeys.OZONE_CLIENT_BYTES_PER_CHECKSUM_MIN_SIZE);
      bytesPerChecksum =
          OzoneConfigKeys.OZONE_CLIENT_BYTES_PER_CHECKSUM_MIN_SIZE;
    }

  }

  public long getStreamBufferFlushSize() {
    return streamBufferFlushSize;
  }

  public void setStreamBufferFlushSize(long streamBufferFlushSize) {
    this.streamBufferFlushSize = streamBufferFlushSize;
  }

  public int getStreamBufferSize() {
    return streamBufferSize;
  }

  public void setStreamBufferSize(int streamBufferSize) {
    this.streamBufferSize = streamBufferSize;
  }

  public boolean isStreamBufferFlushDelay() {
    return streamBufferFlushDelay;
  }

  public void setStreamBufferFlushDelay(boolean streamBufferFlushDelay) {
    this.streamBufferFlushDelay = streamBufferFlushDelay;
  }

  public long getStreamBufferMaxSize() {
    return streamBufferMaxSize;
  }

  public void setStreamBufferMaxSize(long streamBufferMaxSize) {
    this.streamBufferMaxSize = streamBufferMaxSize;
  }

  public int getMaxRetryCount() {
    return maxRetryCount;
  }

  public void setMaxRetryCount(int maxRetryCount) {
    this.maxRetryCount = maxRetryCount;
  }

  public int getRetryInterval() {
    return retryInterval;
  }

  public void setRetryInterval(int retryInterval) {
    this.retryInterval = retryInterval;
  }

  public ChecksumType getChecksumType() {
    return ChecksumType.valueOf(checksumType);
  }

  public void setChecksumType(ChecksumType checksumType) {
    this.checksumType = checksumType.name();
  }

  public int getBytesPerChecksum() {
    return bytesPerChecksum;
  }

  public void setBytesPerChecksum(int bytesPerChecksum) {
    this.bytesPerChecksum = bytesPerChecksum;
  }

  public boolean isChecksumVerify() {
    return checksumVerify;
  }

  public void setChecksumVerify(boolean checksumVerify) {
    this.checksumVerify = checksumVerify;
  }

}
