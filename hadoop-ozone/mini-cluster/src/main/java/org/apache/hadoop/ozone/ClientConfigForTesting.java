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

package org.apache.hadoop.ozone;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;

import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;

/**
 * Helper for tests that want to set client stream properties.
 */
public final class ClientConfigForTesting {

  private int chunkSize = 1024 * 1024;
  private Long blockSize;
  private Long containerSize;
  private Integer streamBufferSize;
  private Long streamBufferFlushSize;
  private Long dataStreamBufferFlushSize;
  private Long dataStreamWindowSize;
  private Long streamBufferMaxSize;
  private Integer dataStreamMinPacketSize;
  private final StorageUnit unit;

  /**
   * @param unit Defines the unit in which size properties will be passed to the builder.
   * All sizes are stored internally converted to {@link StorageUnit#BYTES}.
   */
  public static ClientConfigForTesting newBuilder(StorageUnit unit) {
    return new ClientConfigForTesting(unit);
  }

  private ClientConfigForTesting(StorageUnit unit) {
    this.unit = unit;
  }

  public ClientConfigForTesting setChunkSize(int size) {
    chunkSize = (int) toBytes(size);
    return this;
  }

  public ClientConfigForTesting setBlockSize(long size) {
    blockSize = toBytes(size);
    return this;
  }

  public ClientConfigForTesting setContainerSize(long size) {
    containerSize = toBytes(size);
    return this;
  }

  @SuppressWarnings("unused") // kept for completeness
  public ClientConfigForTesting setStreamBufferSize(int size) {
    streamBufferSize = (int) toBytes(size);
    return this;
  }

  public ClientConfigForTesting setStreamBufferFlushSize(long size) {
    streamBufferFlushSize = toBytes(size);
    return this;
  }

  public ClientConfigForTesting setStreamBufferMaxSize(long size) {
    streamBufferMaxSize = toBytes(size);
    return this;
  }

  public ClientConfigForTesting setDataStreamMinPacketSize(int size) {
    dataStreamMinPacketSize = (int) toBytes(size);
    return this;
  }

  public ClientConfigForTesting setDataStreamBufferFlushSize(long size) {
    dataStreamBufferFlushSize = toBytes(size);
    return this;
  }

  public ClientConfigForTesting setDataStreamWindowSize(long size) {
    dataStreamWindowSize = toBytes(size);
    return this;
  }

  public void applyTo(MutableConfigurationSource conf) {
    applyTo(conf, false);
  }

  public void applyTo(MutableConfigurationSource conf, boolean onlyIfUnset) {
    calculateUndefinedValues();

    final MutableConfigurationSource target =
        onlyIfUnset ? MutableConfigurationSource.ifUnsetWrapper(conf) : conf;
    target.setFromObject(getClientConfig(conf));

    if (onlyIfUnset) {
      setIfUnset(conf);
    } else {
      set(conf);
    }
  }

  private void calculateUndefinedValues() {
    if (streamBufferSize == null) {
      streamBufferSize = chunkSize;
    }
    if (streamBufferFlushSize == null) {
      streamBufferFlushSize = (long) chunkSize;
    }
    if (streamBufferMaxSize == null) {
      streamBufferMaxSize = 2 * streamBufferFlushSize;
    }
    if (dataStreamBufferFlushSize == null) {
      dataStreamBufferFlushSize = 4L * chunkSize;
    }
    if (dataStreamMinPacketSize == null) {
      dataStreamMinPacketSize = chunkSize / 4;
    }
    if (dataStreamWindowSize == null) {
      dataStreamWindowSize = 8L * chunkSize;
    }
    if (blockSize == null) {
      blockSize = 2 * streamBufferMaxSize;
    }
    if (containerSize == null) {
      containerSize = 4 * blockSize;
    }
  }

  private OzoneClientConfig getClientConfig(MutableConfigurationSource conf) {
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamBufferSize(streamBufferSize);
    clientConfig.setStreamBufferMaxSize(streamBufferMaxSize);
    clientConfig.setStreamBufferFlushSize(streamBufferFlushSize);
    clientConfig.setDataStreamBufferFlushSize(dataStreamBufferFlushSize);
    clientConfig.setDataStreamMinPacketSize(dataStreamMinPacketSize);
    clientConfig.setStreamWindowSize(dataStreamWindowSize);
    return clientConfig;
  }

  private void set(MutableConfigurationSource conf) {
    conf.setStorageSize(OZONE_SCM_CHUNK_SIZE_KEY, chunkSize, StorageUnit.BYTES);
    conf.setStorageSize(OZONE_SCM_BLOCK_SIZE, blockSize, StorageUnit.BYTES);
    conf.setStorageSize(OZONE_SCM_CONTAINER_SIZE, containerSize, StorageUnit.BYTES);
  }

  private void setIfUnset(MutableConfigurationSource conf) {
    final String suffix = StorageUnit.BYTES.getShortName();
    conf.setIfUnset(OZONE_SCM_CHUNK_SIZE_KEY, chunkSize + suffix);
    conf.setIfUnset(OZONE_SCM_BLOCK_SIZE, blockSize + suffix);
    conf.setIfUnset(OZONE_SCM_CONTAINER_SIZE, containerSize + suffix);
  }

  private long toBytes(long value) {
    return Math.round(unit.toBytes(value));
  }

}
