/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.PostConstruct;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.ozone.OzoneConfigKeys;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration values for Ozone Client.
 */
@ConfigGroup(prefix = "ozone.client")
public class OzoneClientConfig {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneClientConfig.class);

  public static final boolean OZONE_READ_SHORT_CIRCUIT_DEFAULT = false;
  public static final String OZONE_DOMAIN_SOCKET_PATH = "ozone.domain.socket.path";
  public static final String OZONE_DOMAIN_SOCKET_PATH_DEFAULT = "/var/lib/ozone/dn_socket";
  public static final String SHORT_CIRCUIT_PREFIX = "read.short-circuit.";
  public static final short DATA_TRANSFER_VERSION = 28;
  public static final byte DATA_TRANSFER_MAGIC_CODE = 99;

  @Config(key = "read.short-circuit",
      defaultValue = "false",
      type = ConfigType.BOOLEAN,
      description = "Whether read short-circuit is enabled or not",
      tags = { ConfigTag.CLIENT, ConfigTag.DATANODE })
  private boolean shortCircuitEnabled = OZONE_READ_SHORT_CIRCUIT_DEFAULT;

  @Config(key = SHORT_CIRCUIT_PREFIX + "buffer.size",
      defaultValue = "128KB",
      type = ConfigType.SIZE,
      description = "Buffer size of reader/writer.",
      tags = { ConfigTag.CLIENT, ConfigTag.DATANODE })
  private int shortCircuitBufferSize = 128 * 1024;

  @Config(key = SHORT_CIRCUIT_PREFIX + "disable.interval",
      defaultValue = "600",
      type = ConfigType.LONG,
      description = "If some unknown IO error happens on Domain socket read, short circuit read will be disabled " +
          "temporarily for this period of time(seconds).",
      tags = { ConfigTag.CLIENT })
  private long shortCircuitReadDisableInterval = 60 * 10;

  public boolean isShortCircuitEnabled() {
    return shortCircuitEnabled;
  }

  public void setShortCircuit(boolean enabled) {
    shortCircuitEnabled = enabled;
  }


  public int getShortCircuitBufferSize() {
    return shortCircuitBufferSize;
  }

  public void setShortCircuitBufferSize(int size) {
    this.shortCircuitBufferSize = size;
  }

  public long getShortCircuitReadDisableInterval() {
    return shortCircuitReadDisableInterval;
  }

  public void setShortCircuitReadDisableInterval(long value) {
    shortCircuitReadDisableInterval = value;
  }

  /**
   * Enum for indicating what mode to use when combining chunk and block
   * checksums to define an aggregate FileChecksum. This should be considered
   * a client-side runtime option rather than a persistent property of any
   * stored metadata, which is why this is not part of ChecksumOpt, which
   * deals with properties of files at rest.
   */
  public enum ChecksumCombineMode {
    MD5MD5CRC,  // MD5 of block checksums, which are MD5 over chunk CRCs
    COMPOSITE_CRC  // Block/chunk-independent composite CRC
  }

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

  @Config(key = "datastream.buffer.flush.size",
      defaultValue = "16MB",
      type = ConfigType.SIZE,
      description = "The boundary at which putBlock is executed",
      tags = ConfigTag.CLIENT)
  private long dataStreamBufferFlushSize = 16 * 1024 * 1024;

  @Config(key = "datastream.min.packet.size",
      defaultValue = "1MB",
      type = ConfigType.SIZE,
      description = "The maximum size of the ByteBuffer "
          + "(used via ratis streaming)",
      tags = ConfigTag.CLIENT)
  private int dataStreamMinPacketSize = 1024 * 1024;

  @Config(key = "datastream.window.size",
      defaultValue = "64MB",
      type = ConfigType.SIZE,
      description = "Maximum size of BufferList(used for retry) size per " +
          "BlockDataStreamOutput instance",
      tags = ConfigTag.CLIENT)
  private long streamWindowSize = 64 * 1024 * 1024;

  @Config(key = "datastream.pipeline.mode",
      defaultValue = "true",
      description = "Streaming write support both pipeline mode(datanode1->" +
          "datanode2->datanode3) and star mode(datanode1->datanode2, " +
          "datanode1->datanode3). By default we use pipeline mode.",
      tags = ConfigTag.CLIENT)
  private boolean datastreamPipelineMode = true;

  @Config(key = "stream.buffer.increment",
      defaultValue = "0B",
      type = ConfigType.SIZE,
      description = "Buffer (defined by ozone.client.stream.buffer.size) "
          + "will be incremented with this steps. If zero, the full buffer "
          + "will "
          + "be created at once. Setting it to a variable between 0 and "
          + "ozone.client.stream.buffer.size can reduce the memory usage for "
          + "very small keys, but has a performance overhead.",
      tags = ConfigTag.CLIENT)
  private int bufferIncrement = 0;

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

  @Config(key = "max.retries",
      defaultValue = "5",
      description = "Maximum number of retries by Ozone Client on "
          + "encountering exception while writing a key",
      tags = ConfigTag.CLIENT)
  private int maxRetryCount = 5;

  @Config(key = "retry.interval",
      defaultValue = "0",
      description =
          "Indicates the time duration a client will wait before retrying a "
              + "write key request on encountering an exception. By default "
              + "there is no wait",
      tags = ConfigTag.CLIENT)
  private int retryInterval = 0;

  @Config(key = "read.max.retries",
      defaultValue = "3",
      description = "Maximum number of retries by Ozone Client on "
          + "encountering connectivity exception when reading a key.",
      tags = ConfigTag.CLIENT)
  private int maxReadRetryCount = 3;

  @Config(key = "read.retry.interval",
      defaultValue = "1",
      description =
          "Indicates the time duration in seconds a client will wait "
              + "before retrying a read key request on encountering "
              + "a connectivity excepetion from Datanodes . "
              + "By default the interval is 1 second",
      tags = ConfigTag.CLIENT)
  private int readRetryInterval = 1;

  @Config(key = "checksum.type",
      defaultValue = "CRC32",
      description = "The checksum type [NONE/ CRC32/ CRC32C/ SHA256/ MD5] "
          + "determines which algorithm would be used to compute checksum for "
          + "chunk data. Default checksum type is CRC32.",
      tags = { ConfigTag.CLIENT, ConfigTag.CRYPTO_COMPLIANCE })
  private String checksumType = ChecksumType.CRC32.name();

  @Config(key = "bytes.per.checksum",
      defaultValue = "16KB",
      type = ConfigType.SIZE,
      description = "Checksum will be computed for every bytes per checksum "
          + "number of bytes and stored sequentially. The minimum value for "
          + "this config is 8KB.",
      tags = { ConfigTag.CLIENT, ConfigTag.CRYPTO_COMPLIANCE })
  private int bytesPerChecksum = 16 * 1024;

  @Config(key = "verify.checksum",
      defaultValue = "true",
      description = "Ozone client to verify checksum of the checksum "
          + "blocksize data.",
      tags = ConfigTag.CLIENT)
  private boolean checksumVerify = true;

  @Config(key = "max.ec.stripe.write.retries",
      defaultValue = "10",
      description = "Ozone EC client to retry stripe to new block group on" +
          " failures.",
      tags = ConfigTag.CLIENT)
  private int maxECStripeWriteRetries = 10;

  @Config(key = "ec.stripe.queue.size",
      defaultValue = "2",
      description = "The max number of EC stripes can be buffered in client " +
          " before flushing into datanodes.",
      tags = ConfigTag.CLIENT)
  private int ecStripeQueueSize = 2;

  @Config(key = "exclude.nodes.expiry.time",
      defaultValue = "600000",
      description = "Time after which an excluded node is reconsidered for" +
          " writes. If the value is zero, the node is excluded for the" +
          " life of the client",
      tags = ConfigTag.CLIENT)
  private long excludeNodesExpiryTime = 10 * 60 * 1000;

  @Config(key = "ec.reconstruct.stripe.read.pool.limit",
      defaultValue = "30",
      description = "Thread pool max size for parallelly read" +
          " available ec chunks to reconstruct the whole stripe.",
      tags = ConfigTag.CLIENT)
  // For the largest recommended EC policy rs-10-4-1024k,
  // 10 chunks are required at least for stripe reconstruction,
  // so 1 core thread for each chunk and
  // 3 concurrent stripe read should be enough.
  private int ecReconstructStripeReadPoolLimit = 10 * 3;

  @Config(key = "ec.reconstruct.stripe.write.pool.limit",
      defaultValue = "30",
      description = "Thread pool max size for parallelly write" +
          " available ec chunks to reconstruct the whole stripe.",
      tags = ConfigTag.CLIENT)
  private int ecReconstructStripeWritePoolLimit = 10 * 3;

  @Config(key = "checksum.combine.mode",
      defaultValue = "COMPOSITE_CRC",
      description = "The combined checksum type [MD5MD5CRC / COMPOSITE_CRC] "
          + "determines which algorithm would be used to compute file checksum."
          + "COMPOSITE_CRC calculates the combined CRC of the whole file, "
          + "where the lower-level chunk/block checksums are combined into "
          + "file-level checksum."
          + "MD5MD5CRC calculates the MD5 of MD5 of checksums of individual "
          + "chunks."
          + "Default checksum type is COMPOSITE_CRC.",
      tags = ConfigTag.CLIENT)
  private String checksumCombineMode =
      ChecksumCombineMode.COMPOSITE_CRC.name();

  @Config(key = "fs.default.bucket.layout",
      defaultValue = "FILE_SYSTEM_OPTIMIZED",
      type = ConfigType.STRING,
      description = "The bucket layout used by buckets created using OFS. " +
          "Valid values include FILE_SYSTEM_OPTIMIZED and LEGACY",
      tags = ConfigTag.CLIENT)
  private String fsDefaultBucketLayout = "FILE_SYSTEM_OPTIMIZED";

  // ozone.client.hbase.enhancements.allowed
  @Config(key = "hbase.enhancements.allowed",
      defaultValue = "false",
      description = "When set to false, client-side HBase enhancement-related Ozone (experimental) features " +
          "are disabled (not allowed to be enabled) regardless of whether those configs are set.\n" +
          "\n" +
          "Here is the list of configs and values overridden when this config is set to false:\n" +
          "1. ozone.fs.hsync.enabled = false\n" +
          "2. ozone.client.incremental.chunk.list = false\n" +
          "3. ozone.client.stream.putblock.piggybacking = false\n" +
          "4. ozone.client.key.write.concurrency = 1\n" +
          "\n" +
          "A warning message will be printed if any of the above configs are overridden by this.",
      tags = ConfigTag.CLIENT)
  private boolean hbaseEnhancementsAllowed = false;

  // ozone.client.incremental.chunk.list
  @Config(key = "incremental.chunk.list",
      defaultValue = "false",
      type = ConfigType.BOOLEAN,
      description = "Client PutBlock request can choose incremental chunk " +
          "list rather than full chunk list to optimize performance. " +
          "Critical to HBase. EC does not support this feature. " +
          "Can be enabled only when ozone.client.hbase.enhancements.allowed = true",
      tags = ConfigTag.CLIENT)
  private boolean incrementalChunkList = false;

  // ozone.client.stream.putblock.piggybacking
  @Config(key = "stream.putblock.piggybacking",
          defaultValue = "false",
          type = ConfigType.BOOLEAN,
          description = "Allow PutBlock to be piggybacked in WriteChunk requests if the chunk is small. " +
              "Can be enabled only when ozone.client.hbase.enhancements.allowed = true",
          tags = ConfigTag.CLIENT)
  private boolean enablePutblockPiggybacking = false;

  // ozone.client.key.write.concurrency
  @Config(key = "key.write.concurrency",
      defaultValue = "1",
      description = "Maximum concurrent writes allowed on each key. " +
          "Defaults to 1 which matches the behavior before HDDS-9844. " +
          "For unlimited write concurrency, set this to -1 or any negative integer value. " +
          "Any value other than 1 is effective only when ozone.client.hbase.enhancements.allowed = true",
      tags = ConfigTag.CLIENT)
  private int maxConcurrentWritePerKey = 1;

  @PostConstruct
  public void validate() {
    Preconditions.checkState(streamBufferSize > 0);
    Preconditions.checkState(streamBufferFlushSize > 0);
    Preconditions.checkState(streamBufferMaxSize > 0);

    Preconditions.checkArgument(bufferIncrement < streamBufferSize,
        "Buffer increment should be smaller than the size of the stream "
            + "buffer");
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

    // Verify client configs related to HBase enhancements
    // Enforce check on ozone.client.hbase.enhancements.allowed
    if (!hbaseEnhancementsAllowed) {
      // ozone.client.hbase.enhancements.allowed = false
      if (incrementalChunkList) {
        LOG.warn("Ignoring ozone.client.incremental.chunk.list = true " +
            "because HBase enhancements are disallowed. " +
            "To enable it, set ozone.client.hbase.enhancements.allowed = true.");
        incrementalChunkList = false;
        LOG.debug("Final ozone.client.incremental.chunk.list = {}", incrementalChunkList);
      }
      if (enablePutblockPiggybacking) {
        LOG.warn("Ignoring ozone.client.stream.putblock.piggybacking = true " +
            "because HBase enhancements are disallowed. " +
            "To enable it, set ozone.client.hbase.enhancements.allowed = true.");
        enablePutblockPiggybacking = false;
        LOG.debug("Final ozone.client.stream.putblock.piggybacking = {}", enablePutblockPiggybacking);
      }
      if (maxConcurrentWritePerKey != 1) {
        LOG.warn("Ignoring ozone.client.key.write.concurrency = {} " +
            "because HBase enhancements are disallowed. " +
            "To enable it, set ozone.client.hbase.enhancements.allowed = true.",
            maxConcurrentWritePerKey);
        maxConcurrentWritePerKey = 1;
        LOG.debug("Final ozone.client.key.write.concurrency = {}", maxConcurrentWritePerKey);
      }
      // Note: ozone.fs.hsync.enabled is enforced by OzoneFSUtils#canEnableHsync, not here
    }
  }

  public long getStreamBufferFlushSize() {
    return streamBufferFlushSize;
  }

  @VisibleForTesting
  public void setStreamBufferFlushSize(long streamBufferFlushSize) {
    this.streamBufferFlushSize = streamBufferFlushSize;
  }

  public int getStreamBufferSize() {
    return streamBufferSize;
  }

  @VisibleForTesting
  public void setStreamBufferSize(int streamBufferSize) {
    this.streamBufferSize = streamBufferSize;
  }

  public boolean isStreamBufferFlushDelay() {
    return streamBufferFlushDelay;
  }

  @VisibleForTesting
  public void setStreamBufferFlushDelay(boolean streamBufferFlushDelay) {
    this.streamBufferFlushDelay = streamBufferFlushDelay;
  }

  public long getStreamBufferMaxSize() {
    return streamBufferMaxSize;
  }

  @VisibleForTesting
  public void setStreamBufferMaxSize(long streamBufferMaxSize) {
    this.streamBufferMaxSize = streamBufferMaxSize;
  }

  public int getDataStreamMinPacketSize() {
    return dataStreamMinPacketSize;
  }

  public void setDataStreamMinPacketSize(int dataStreamMinPacketSize) {
    this.dataStreamMinPacketSize = dataStreamMinPacketSize;
  }

  public long getStreamWindowSize() {
    return streamWindowSize;
  }

  public void setStreamWindowSize(long streamWindowSize) {
    this.streamWindowSize = streamWindowSize;
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

  public int getMaxReadRetryCount() {
    return maxReadRetryCount;
  }

  public void setMaxReadRetryCount(int maxReadRetryCount) {
    this.maxReadRetryCount = maxReadRetryCount;
  }

  public int getReadRetryInterval() {
    return readRetryInterval;
  }

  public void setReadRetryInterval(int readRetryInterval) {
    this.readRetryInterval = readRetryInterval;
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

  public int getMaxECStripeWriteRetries() {
    return this.maxECStripeWriteRetries;
  }

  public int getEcStripeQueueSize() {
    return this.ecStripeQueueSize;
  }

  public long getExcludeNodesExpiryTime() {
    return excludeNodesExpiryTime;
  }

  public int getBufferIncrement() {
    return bufferIncrement;
  }

  public long getDataStreamBufferFlushSize() {
    return dataStreamBufferFlushSize;
  }

  public void setDataStreamBufferFlushSize(long dataStreamBufferFlushSize) {
    this.dataStreamBufferFlushSize = dataStreamBufferFlushSize;
  }

  public ChecksumCombineMode getChecksumCombineMode() {
    try {
      return ChecksumCombineMode.valueOf(checksumCombineMode);
    } catch (IllegalArgumentException iae) {
      LOG.warn("Bad checksum combine mode: {}. Using default {}",
          checksumCombineMode,
          ChecksumCombineMode.COMPOSITE_CRC.name());
      return ChecksumCombineMode.valueOf(
          ChecksumCombineMode.COMPOSITE_CRC.name());
    }
  }

  public void setEcReconstructStripeReadPoolLimit(int poolLimit) {
    this.ecReconstructStripeReadPoolLimit = poolLimit;
  }

  public int getEcReconstructStripeReadPoolLimit() {
    return ecReconstructStripeReadPoolLimit;
  }

  public void setEcReconstructStripeWritePoolLimit(int poolLimit) {
    this.ecReconstructStripeWritePoolLimit = poolLimit;
  }

  public int getEcReconstructStripeWritePoolLimit() {
    return ecReconstructStripeWritePoolLimit;
  }

  public void setFsDefaultBucketLayout(String bucketLayout) {
    if (!bucketLayout.isEmpty()) {
      this.fsDefaultBucketLayout = bucketLayout;
    }
  }

  public String getFsDefaultBucketLayout() {
    return fsDefaultBucketLayout;
  }

  public void setEnablePutblockPiggybacking(boolean enablePutblockPiggybacking) {
    this.enablePutblockPiggybacking = enablePutblockPiggybacking;
  }

  public boolean getEnablePutblockPiggybacking() {
    return enablePutblockPiggybacking;
  }

  public boolean isDatastreamPipelineMode() {
    return datastreamPipelineMode;
  }

  public void setDatastreamPipelineMode(boolean datastreamPipelineMode) {
    this.datastreamPipelineMode = datastreamPipelineMode;
  }

  public void setHBaseEnhancementsAllowed(boolean isHBaseEnhancementsEnabled) {
    this.hbaseEnhancementsAllowed = isHBaseEnhancementsEnabled;
  }

  public boolean getHBaseEnhancementsAllowed() {
    return this.hbaseEnhancementsAllowed;
  }

  public void setIncrementalChunkList(boolean enable) {
    this.incrementalChunkList = enable;
  }

  public boolean getIncrementalChunkList() {
    return this.incrementalChunkList;
  }

  public void setMaxConcurrentWritePerKey(int maxConcurrentWritePerKey) {
    this.maxConcurrentWritePerKey = maxConcurrentWritePerKey;
  }

  public int getMaxConcurrentWritePerKey() {
    return this.maxConcurrentWritePerKey;
  }
}
