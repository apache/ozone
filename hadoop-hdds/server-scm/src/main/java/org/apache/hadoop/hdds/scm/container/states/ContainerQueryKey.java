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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Key for the Caching layer for Container Query.
 */
public class ContainerQueryKey {
  private final HddsProtos.LifeCycleState state;
  private final String owner;
  private final StorageClass storageClass;

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ContainerQueryKey that = (ContainerQueryKey) o;

    return new EqualsBuilder()
        .append(getState(), that.getState())
        .append(getOwner(), that.getOwner())
        .append(getStorageClass().getName(), that.getStorageClass().getName())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(61, 71)
        .append(getState())
        .append(getOwner())
        .append(getStorageClass())

        .toHashCode();
  }

  /**
   * Constructor for ContainerQueryKey.
   * @param state LifeCycleState
   * @param owner - Name of the Owner.
   * @param storageClass - StorageClass
   */
  public ContainerQueryKey(HddsProtos.LifeCycleState state, String owner,
      StorageClass storageClass) {
    this.state = state;
    this.owner = owner;
    this.storageClass = storageClass;
  }

  /**
   * Returns the state of containers which this key represents.
   * @return LifeCycleState
   */
  public HddsProtos.LifeCycleState getState() {
    return state;
  }

  /**
   * Returns the owner of containers which this key represents.
   * @return Owner
   */
  public String getOwner() {
    return owner;
  }

  /**
   * Returns the storageClass of this specific container.
   */
  public StorageClass getStorageClass() {
    return storageClass;
  }
}
