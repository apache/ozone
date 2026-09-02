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

package org.apache.hadoop.hdds.scm.container.common.helpers;

import jakarta.annotation.Nullable;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

/**
 * Allocated block wraps the result returned from SCM#allocateBlock which
 * contains a Pipeline and the key.
 */
public final class AllocatedBlock {
  private final Pipeline pipeline;
  private final ContainerBlockID containerBlockID;

  private final @Nullable StorageTier storageTier;
  private final boolean isFallBack;

  /**
   * Builder for AllocatedBlock.
   */
  public static class Builder {
    private Pipeline pipeline;
    private ContainerBlockID containerBlockID;
    private @Nullable StorageTier storageTier;
    private boolean isFallBack;

    public Builder setPipeline(Pipeline p) {
      this.pipeline = p;
      return this;
    }

    public Builder setContainerBlockID(ContainerBlockID blockId) {
      this.containerBlockID = blockId;
      return this;
    }

    public Builder setStorageTier(StorageTier storageTier) {
      this.storageTier = storageTier;
      return this;
    }

    public Builder setIsFallBack(boolean fallBack) {
      isFallBack = fallBack;
      return this;
    }

    public AllocatedBlock build() {
      return new AllocatedBlock(pipeline, containerBlockID, storageTier, isFallBack);
    }
  }

  private AllocatedBlock(Pipeline pipeline, ContainerBlockID containerBlockID,
      StorageTier storageTier, boolean isFallBack) {
    this.pipeline = pipeline;
    this.containerBlockID = containerBlockID;
    this.storageTier = storageTier;
    this.isFallBack = isFallBack;
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public ContainerBlockID getBlockID() {
    return containerBlockID;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return new Builder()
        .setContainerBlockID(containerBlockID)
        .setPipeline(pipeline)
        .setStorageTier(storageTier)
        .setIsFallBack(isFallBack);
  }

  @Nullable
  public StorageTier getStorageTier() {
    return storageTier;
  }

  public boolean isFallBack() {
    return isFallBack;
  }
}
