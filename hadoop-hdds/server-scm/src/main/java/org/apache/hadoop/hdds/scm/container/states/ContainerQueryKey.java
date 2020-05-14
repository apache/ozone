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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * Key for the Caching layer for Container Query.
 */
public class ContainerQueryKey {
  private final HddsProtos.LifeCycleState state;
  private final String owner;
  private final int replication;
  private final HddsProtos.ReplicationType type;

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
        .append(getReplication(), that.getReplication())
        .append(getType(), that.getType())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(61, 71)
        .append(getState())
        .append(getOwner())
        .append(getReplication())
        .append(getType())
        .toHashCode();
  }

  /**
   * Constructor for ContainerQueryKey.
   * @param state LifeCycleState
   * @param owner - Name of the Owner.
   * @param replication Replication number.
   * @param type - Replication Type.
   */
  public ContainerQueryKey(HddsProtos.LifeCycleState state, String owner,
      int replication, HddsProtos.ReplicationType type) {
    this.state = state;
    this.owner = owner;
    this.replication = replication;
    this.type = type;
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
   * Returns the replication number of containers which this key represents.
   * @return Replication
   */
  public int getReplication() {
    return replication;
  }

  /**
   * Returns the replication type of containers which this key represents.
   * @return ReplicationType
   */
  public HddsProtos.ReplicationType getType() {
    return type;
  }
}
