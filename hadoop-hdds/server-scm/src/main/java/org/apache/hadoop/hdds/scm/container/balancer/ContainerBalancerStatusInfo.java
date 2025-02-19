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

  public ContainerBalancerStatusInfo(
          OffsetDateTime startedAt,
          HddsProtos.ContainerBalancerConfigurationProto configuration,
          List<ContainerBalancerTaskIterationStatusInfo> iterationsStatusInfo) {
    this.startedAt = startedAt;
    this.configuration = configuration;
    this.iterationsStatusInfo = iterationsStatusInfo;
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

  /**
   * Converts an instance into a protobuf-compatible object.
   * @return proto representation
   */
  public StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfoProto toProto() {
    return StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfoProto
        .newBuilder()
        .setStartedAt(getStartedAt().toEpochSecond())
        .setConfiguration(getConfiguration())
        .addAllIterationsStatusInfo(
            getIterationsStatusInfo()
                .stream()
                .map(ContainerBalancerTaskIterationStatusInfo::toProto)
                .collect(Collectors.toList())
        ).build();
  }
}
