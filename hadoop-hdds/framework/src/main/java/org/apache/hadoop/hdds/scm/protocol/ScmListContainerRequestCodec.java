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

package org.apache.hadoop.hdds.scm.protocol;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;

import jakarta.annotation.Nullable;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicatedReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMListContainerRequestProto;

/**
 * Codec between {@link SCMListContainerRequestProto} and {@link ListContainerQuery} for list containers.
 */
public final class ScmListContainerRequestCodec {

  private ScmListContainerRequestCodec() {
  }

  /**
   * Immutable parameters for listing containers (encode and decode paths).
   * Construct with {@link ListContainerQuery#newBuilder(long, int)} and optional fluent
   * setters on {@link ListContainerQuery.Builder}; further filters extend the builder
   * without widening the {@link #toProto(ListContainerQuery)} API.
   */
  public static final class ListContainerQuery {
    private final long startContainerID;
    private final int count;
    @Nullable
    private final String traceId;
    @Nullable
    private final HddsProtos.LifeCycleState state;
    @Nullable
    private final HddsProtos.ReplicationFactor factor;
    @Nullable
    private final HddsProtos.ReplicationType replicationType;
    @Nullable
    private final ReplicationConfig replicationConfig;
    @Nullable
    private final Boolean suppressed;

    private ListContainerQuery(Builder builder) {
      this.startContainerID = builder.startContainerID;
      this.count = builder.count;
      this.traceId = builder.traceId;
      this.state = builder.state;
      this.factor = builder.factor;
      this.replicationType = builder.replicationType;
      this.replicationConfig = builder.replicationConfig;
      this.suppressed = builder.suppressed;
    }

    public static Builder newBuilder(long startContainerID, int count) {
      return new Builder(startContainerID, count);
    }

    public long getStartContainerID() {
      return startContainerID;
    }

    public int getCount() {
      return count;
    }

    @Nullable
    public String getTraceId() {
      return traceId;
    }

    @Nullable
    public HddsProtos.LifeCycleState getState() {
      return state;
    }

    @Nullable
    public HddsProtos.ReplicationFactor getFactor() {
      return factor;
    }

    @Nullable
    public HddsProtos.ReplicationType getReplicationType() {
      return replicationType;
    }

    @Nullable
    public ReplicationConfig getReplicationConfig() {
      return replicationConfig;
    }

    @Nullable
    public Boolean getSuppressed() {
      return suppressed;
    }

    /** Fluent builder for {@link ListContainerQuery}. */
    public static final class Builder {
      private final long startContainerID;
      private final int count;
      @Nullable
      private String traceId;
      @Nullable
      private HddsProtos.LifeCycleState state;
      @Nullable
      private HddsProtos.ReplicationFactor factor;
      @Nullable
      private HddsProtos.ReplicationType replicationType;
      @Nullable
      private ReplicationConfig replicationConfig;
      @Nullable
      private Boolean suppressed;

      private Builder(long startContainerID, int count) {
        this.startContainerID = startContainerID;
        this.count = count;
      }

      public Builder setTraceId(@Nullable String traceIdVal) {
        this.traceId = traceIdVal;
        return this;
      }

      public Builder setState(@Nullable HddsProtos.LifeCycleState stateVal) {
        this.state = stateVal;
        return this;
      }

      public Builder setFactor(@Nullable HddsProtos.ReplicationFactor factorVal) {
        this.factor = factorVal;
        return this;
      }

      public Builder setReplicationType(
          @Nullable HddsProtos.ReplicationType replicationTypeVal) {
        this.replicationType = replicationTypeVal;
        return this;
      }

      public Builder setReplicationConfig(@Nullable ReplicationConfig replicationConfigVal) {
        this.replicationConfig = replicationConfigVal;
        return this;
      }

      public Builder setSuppressed(@Nullable Boolean suppressedVal) {
        this.suppressed = suppressedVal;
        return this;
      }

      public ListContainerQuery build() {
        return new ListContainerQuery(this);
      }
    }
  }

  public static SCMListContainerRequestProto toProto(ListContainerQuery query) {
    SCMListContainerRequestProto.Builder proto = SCMListContainerRequestProto.newBuilder()
        .setCount(query.getCount())
        .setStartContainerID(query.getStartContainerID());
    String traceId = query.getTraceId();
    if (traceId != null) {
      proto.setTraceID(traceId);
    }
    HddsProtos.LifeCycleState state = query.getState();
    if (state != null) {
      proto.setState(state);
    }
    ReplicationConfig repConfig = query.getReplicationConfig();
    HddsProtos.ReplicationType replicationType = query.getReplicationType();
    HddsProtos.ReplicationFactor factor = query.getFactor();
    if (repConfig != null) {
      HddsProtos.ReplicationType repType = repConfig.getReplicationType();
      if (repType == EC) {
        proto.setType(EC);
        proto.setEcReplicationConfig(((ECReplicationConfig) repConfig).toProto());
      } else {
        proto.setType(repType);
        proto.setFactor(((ReplicatedReplicationConfig) repConfig).getReplicationFactor());
      }
    } else if (replicationType != null) {
      proto.setType(replicationType);
    } else if (factor != null) {
      proto.setFactor(factor);
    }
    Boolean suppressed = query.getSuppressed();
    if (suppressed != null) {
      proto.setSuppressed(suppressed);
    }
    return proto.build();
  }

  /**
   * Decodes {@link SCMListContainerRequestProto} into {@link ListContainerQuery}.
   */
  public static ListContainerQuery fromProto(SCMListContainerRequestProto request) {
    long startContainerID = 0L;
    if (request.hasStartContainerID()) {
      startContainerID = request.getStartContainerID();
    }
    int count = request.getCount();

    ListContainerQuery.Builder builder = ListContainerQuery.newBuilder(startContainerID, count);

    if (request.hasTraceID()) {
      builder.setTraceId(request.getTraceID());
    }
    if (request.hasState()) {
      builder.setState(request.getState());
    }
    HddsProtos.ReplicationType replicationType = null;
    if (request.hasType()) {
      replicationType = request.getType();
      builder.setReplicationType(replicationType);
    }
    if (replicationType != null) {
      if (replicationType == EC) {
        if (request.hasEcReplicationConfig()) {
          builder.setReplicationConfig(new ECReplicationConfig(request.getEcReplicationConfig()));
        }
      } else {
        if (request.hasFactor()) {
          builder.setReplicationConfig(ReplicationConfig.fromProtoTypeAndFactor(replicationType, request.getFactor()));
        }
      }
    } else if (request.hasFactor()) {
      builder.setFactor(request.getFactor());
    }

    if (request.hasSuppressed()) {
      builder.setSuppressed(request.getSuppressed());
    }
    return builder.build();
  }
}
