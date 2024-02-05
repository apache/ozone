/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.ConfigurationTarget;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;

import java.util.OptionalInt;
import java.util.OptionalLong;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;

/**
 * Helper for tests that want to set client stream properties.
 */
public final class ClientConfigBuilder {

  private final ConfigurationSource conf;

  private int chunkSize = 1024 * 1024;
  private OptionalLong blockSize = OptionalLong.empty();
  private OptionalInt streamBufferSize = OptionalInt.empty();
  private OptionalLong streamBufferFlushSize = OptionalLong.empty();
  private OptionalLong dataStreamBufferFlushSize = OptionalLong.empty();
  private OptionalLong dataStreamWindowSize = OptionalLong.empty();
  private OptionalLong streamBufferMaxSize = OptionalLong.empty();
  private OptionalInt dataStreamMinPacketSize = OptionalInt.empty();
  private final StorageUnit unit;

  public static ClientConfigBuilder newBuilder(ConfigurationSource conf, StorageUnit unit) {
    return new ClientConfigBuilder(conf, unit);
  }

  private ClientConfigBuilder(ConfigurationSource conf, StorageUnit unit) {
    this.conf = conf;
    this.unit = unit;
  }

  public ClientConfigBuilder setChunkSize(int size) {
    chunkSize = (int) toBytes(size);
    return this;
  }

  public ClientConfigBuilder setBlockSize(long size) {
    blockSize = OptionalLong.of(toBytes(size));
    return this;
  }

  public ClientConfigBuilder setStreamBufferSize(int size) {
    streamBufferSize = OptionalInt.of((int) toBytes(size));
    return this;
  }

  public ClientConfigBuilder setStreamBufferFlushSize(long size) {
    streamBufferFlushSize = OptionalLong.of(toBytes(size));
    return this;
  }

  public ClientConfigBuilder setStreamBufferMaxSize(long size) {
    streamBufferMaxSize = OptionalLong.of(toBytes(size));
    return this;
  }

  public ClientConfigBuilder setDataStreamMinPacketSize(int size) {
    dataStreamMinPacketSize = OptionalInt.of((int) toBytes(size));
    return this;
  }

  public ClientConfigBuilder setDataStreamBufferFlushSize(long size) {
    dataStreamBufferFlushSize = OptionalLong.of(toBytes(size));
    return this;
  }

  public ClientConfigBuilder setDataStreamWindowSize(long size) {
    dataStreamWindowSize = OptionalLong.of(toBytes(size));
    return this;
  }

  public void setOn(ConfigurationTarget target) {
    if (!streamBufferSize.isPresent()) {
      streamBufferSize = OptionalInt.of(chunkSize);
    }
    if (!streamBufferFlushSize.isPresent()) {
      streamBufferFlushSize = OptionalLong.of(chunkSize);
    }
    if (!streamBufferMaxSize.isPresent()) {
      streamBufferMaxSize = OptionalLong.of(2 * streamBufferFlushSize.getAsLong());
    }
    if (!dataStreamBufferFlushSize.isPresent()) {
      dataStreamBufferFlushSize = OptionalLong.of(4L * chunkSize);
    }
    if (!dataStreamMinPacketSize.isPresent()) {
      dataStreamMinPacketSize = OptionalInt.of(chunkSize / 4);
    }
    if (!dataStreamWindowSize.isPresent()) {
      dataStreamWindowSize = OptionalLong.of(8L * chunkSize);
    }
    if (!blockSize.isPresent()) {
      blockSize = OptionalLong.of(2 * streamBufferMaxSize.getAsLong());
    }

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamBufferSize(streamBufferSize.getAsInt());
    clientConfig.setStreamBufferMaxSize(streamBufferMaxSize.getAsLong());
    clientConfig.setStreamBufferFlushSize(streamBufferFlushSize.getAsLong());
    clientConfig.setDataStreamBufferFlushSize(dataStreamBufferFlushSize.getAsLong());
    clientConfig.setDataStreamMinPacketSize(dataStreamMinPacketSize.getAsInt());
    clientConfig.setStreamWindowSize(dataStreamWindowSize.getAsLong());

    target.setFromObject(clientConfig);
    target.setStorageSize(OZONE_SCM_CHUNK_SIZE_KEY, chunkSize, StorageUnit.BYTES);
    target.setStorageSize(OZONE_SCM_BLOCK_SIZE, blockSize.getAsLong(), StorageUnit.BYTES);
  }

  private long toBytes(long value) {
    return Math.round(unit.toBytes(value));
  }

}
