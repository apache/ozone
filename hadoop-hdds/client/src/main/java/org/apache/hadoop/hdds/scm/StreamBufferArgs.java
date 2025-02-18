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

package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * This class encapsulates the arguments that are
 * required for Ozone client StreamBuffer.
 */
public class StreamBufferArgs {

  private int streamBufferSize;
  private long streamBufferFlushSize;
  private long streamBufferMaxSize;
  private boolean streamBufferFlushDelay;

  protected StreamBufferArgs(Builder builder) {
    this.streamBufferSize = builder.bufferSize;
    this.streamBufferFlushSize = builder.bufferFlushSize;
    this.streamBufferMaxSize = builder.bufferMaxSize;
    this.streamBufferFlushDelay = builder.streamBufferFlushDelay;
  }

  public int getStreamBufferSize() {
    return streamBufferSize;
  }

  public long getStreamBufferFlushSize() {
    return streamBufferFlushSize;
  }

  public long getStreamBufferMaxSize() {
    return streamBufferMaxSize;
  }

  public boolean isStreamBufferFlushDelay() {
    return streamBufferFlushDelay;
  }

  public void setStreamBufferFlushDelay(boolean streamBufferFlushDelay) {
    this.streamBufferFlushDelay = streamBufferFlushDelay;
  }

  protected void setStreamBufferSize(int streamBufferSize) {
    this.streamBufferSize = streamBufferSize;
  }

  protected void setStreamBufferFlushSize(long streamBufferFlushSize) {
    this.streamBufferFlushSize = streamBufferFlushSize;
  }

  protected void setStreamBufferMaxSize(long streamBufferMaxSize) {
    this.streamBufferMaxSize = streamBufferMaxSize;
  }

  /**
   * Builder class for StreamBufferArgs.
   */
  public static class Builder {
    private int bufferSize;
    private long bufferFlushSize;
    private long bufferMaxSize;
    private boolean streamBufferFlushDelay;

    public Builder setBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    public Builder setBufferFlushSize(long bufferFlushSize) {
      this.bufferFlushSize = bufferFlushSize;
      return this;
    }

    public Builder setBufferMaxSize(long bufferMaxSize) {
      this.bufferMaxSize = bufferMaxSize;
      return this;
    }

    public Builder setStreamBufferFlushDelay(boolean streamBufferFlushDelay) {
      this.streamBufferFlushDelay = streamBufferFlushDelay;
      return this;
    }

    public StreamBufferArgs build() {
      return new StreamBufferArgs(this);
    }

    public static Builder getNewBuilder() {
      return new Builder();
    }
  }

  public static StreamBufferArgs getDefaultStreamBufferArgs(
      ReplicationConfig replicationConfig, OzoneClientConfig clientConfig) {
    int bufferSize;
    long flushSize;
    long bufferMaxSize;
    boolean streamBufferFlushDelay = clientConfig.isStreamBufferFlushDelay();
    if (replicationConfig.getReplicationType() == HddsProtos.ReplicationType.EC) {
      bufferSize = ((ECReplicationConfig) replicationConfig).getEcChunkSize();
      flushSize = ((ECReplicationConfig) replicationConfig).getEcChunkSize();
      bufferMaxSize = ((ECReplicationConfig) replicationConfig).getEcChunkSize();
    } else {
      bufferSize = clientConfig.getStreamBufferSize();
      flushSize = clientConfig.getStreamBufferFlushSize();
      bufferMaxSize = clientConfig.getStreamBufferMaxSize();
    }

    return Builder.getNewBuilder()
        .setBufferSize(bufferSize)
        .setBufferFlushSize(flushSize)
        .setBufferMaxSize(bufferMaxSize)
        .setStreamBufferFlushDelay(streamBufferFlushDelay)
        .build();
  }
}
