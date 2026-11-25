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

package org.apache.hadoop.hdds.scm.container.balancer;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos;

/**
 * Info about balancer status.
 */
public class ContainerBalancerStatusInfo {
  private final OffsetDateTime startedAt;
  private final HddsProtos.ContainerBalancerConfigurationProto  configuration;
  private final List<ContainerBalancerTaskIterationStatusInfo> iterationsStatusInfo;
  private final OffsetDateTime stoppedAt;
  private final String stopReason;
  private final String stopMessage;

  public ContainerBalancerStatusInfo(
          OffsetDateTime startedAt,
          HddsProtos.ContainerBalancerConfigurationProto configuration,
          List<ContainerBalancerTaskIterationStatusInfo> iterationsStatusInfo) {
    this(startedAt, configuration, iterationsStatusInfo, null, null, null);
  }

  public ContainerBalancerStatusInfo(
          OffsetDateTime startedAt,
          HddsProtos.ContainerBalancerConfigurationProto configuration,
          List<ContainerBalancerTaskIterationStatusInfo> iterationsStatusInfo,
          OffsetDateTime stoppedAt,
          String stopReason,
          String stopMessage) {
    this.startedAt = startedAt;
    this.configuration = configuration;
    this.iterationsStatusInfo = iterationsStatusInfo;
    this.stoppedAt = stoppedAt;
    this.stopReason = stopReason;
    this.stopMessage = stopMessage;
  }

  public OffsetDateTime getStartedAt() {
    return startedAt;
  }

  public HddsProtos.ContainerBalancerConfigurationProto  getConfiguration() {
    return configuration;
  }

  public List<ContainerBalancerTaskIterationStatusInfo> getIterationsStatusInfo() {
    return iterationsStatusInfo;
  }

  public OffsetDateTime getStoppedAt() {
    return stoppedAt;
  }

  public String getStopReason() {
    return stopReason;
  }

  public String getStopMessage() {
    return stopMessage;
  }

  /**
   * Converts an instance into a protobuf-compatible object.
   * @return proto representation
   */
  public StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfoProto toProto() {
    StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfoProto.Builder builder =
        StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfoProto
        .newBuilder()
        .setStartedAt(getStartedAt().toEpochSecond())
        .setConfiguration(getConfiguration())
        .addAllIterationsStatusInfo(
            getIterationsStatusInfo()
                .stream()
                .map(ContainerBalancerTaskIterationStatusInfo::toProto)
                .collect(Collectors.toList())
        );
    if (stoppedAt != null) {
      builder.setStoppedAt(stoppedAt.toEpochSecond());
    }
    if (stopReason != null) {
      builder.setStopReason(stopReason);
    }
    if (stopMessage != null) {
      builder.setStopMessage(stopMessage);
    }
    return builder.build();
  }
}
