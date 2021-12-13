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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options;
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

  @Config(key = "checksum.type",
      defaultValue = "CRC32",
      description = "The checksum type [NONE/ CRC32/ CRC32C/ SHA256/ MD5] "
          + "determines which algorithm would be used to compute checksum for "
          + "chunk data. Default checksum type is CRC32.",
      tags = ConfigTag.CLIENT)
  private String checksumType = ChecksumType.CRC32.name();

  @Config(key = "bytes.per.checksum",
      defaultValue = "1MB",
      type = ConfigType.SIZE,
      description = "Checksum will be computed for every bytes per checksum "
          + "number of bytes and stored sequentially. The minimum value for "
          + "this config is 16KB.",
      tags = ConfigTag.CLIENT)
  private int bytesPerChecksum = 1024 * 1024;

  @Config(key = "verify.checksum",
      defaultValue = "true",
      description = "Ozone client to verify checksum of the checksum "
          + "blocksize data.",
      tags = ConfigTag.CLIENT)
  private boolean checksumVerify = true;

  @Config(key = "checksum.combine.mode",
      defaultValue = "MD5MD5CRC",
      description = "The combined checksum type [MD5MD5CRC / COMPOSITE_CRC] "
          + "determines which algorithm would be used to compute checksum for "
          + "file checksum. Default checksum type is MD5MD5CRC.",
      tags = ConfigTag.CLIENT)
  private String checksumCombineMode = Options.ChecksumCombineMode.MD5MD5CRC.name();

  @PostConstruct
  private void validate() {
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

  public int getBufferIncrement() {
    return bufferIncrement;
  }

  public Options.ChecksumCombineMode getChecksumCombineMode() {
    try {
      return Options.ChecksumCombineMode.valueOf(checksumCombineMode);
    } catch(IllegalArgumentException iae) {
      LOG.warn("Bad checksum combine mode: {}. Using default {}", checksumCombineMode,
          Options.ChecksumCombineMode.MD5MD5CRC.name());
      return Options.ChecksumCombineMode.valueOf(
          Options.ChecksumCombineMode.MD5MD5CRC.name());
    }
  }
}
