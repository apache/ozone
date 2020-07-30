/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.hdds.scm.container.states;

import org.apache.hadoop.hdds.StorageClass;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Class that acts as the container state.
 */
public class ContainerState {
  private final StorageClass storageClass;
  private final PipelineID pipelineID;
  private final String owner;

  /**
   * Constructs a Container Key.
   *
   * @param storageClass - storageClass
   * @param pipelineID - ID of the pipeline
   */
  public ContainerState(
      StorageClass storageClass, String owner, PipelineID pipelineID) {
    this.owner = owner;
    this.pipelineID = pipelineID;
    this.storageClass = storageClass;
  }

  public String getOwner() {
    return this.owner;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ContainerState that = (ContainerState) o;

    return new EqualsBuilder()
        .append(storageClass, that.storageClass)
        .append(pipelineID, that.pipelineID)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(137, 757)
        .append(storageClass)
        .append(pipelineID)
        .toHashCode();
  }

  @Override
  public String toString() {
    return "ContainerKey{" +
        ", storageClass=" + storageClass +
        ", pipelineID=" + pipelineID +
        '}';
  }
}