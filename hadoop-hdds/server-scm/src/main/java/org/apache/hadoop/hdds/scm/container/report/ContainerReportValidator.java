/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.report;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;

import java.util.Map;
import java.util.Optional;

/**
 * Class for Validating Container Report.
 */
public final class ContainerReportValidator {

  private Map<ReplicationType, ReplicaValidator> replicaValidators;
  private static final ContainerReportValidator CONTAINER_REPORT_VALIDATOR;
  private ContainerReportValidator() {
    this.replicaValidators = ImmutableMap.of(ReplicationType.EC,
            new ECReplicaValidator());
  }

  private interface ReplicaValidator {
    boolean validate(ReplicationConfig replicationConfig,
                            ContainerReplicaProto replicaProto);
  }

  private static class ECReplicaValidator implements ReplicaValidator {

    @Override
    public boolean validate(ReplicationConfig replicationConfig,
                            ContainerReplicaProto replicaProto) {

      return replicationConfig.getReplicationType() == ReplicationType.EC
        && replicaProto.hasReplicaIndex() && replicaProto.getReplicaIndex() > 0
        && replicaProto.getReplicaIndex() <=
              replicationConfig.getRequiredNodes();
    }
  }

  static {
    CONTAINER_REPORT_VALIDATOR = new ContainerReportValidator();
  }
  private static boolean validateReplica(ReplicationConfig replicationConfig,
                                         ContainerReplicaProto replicaProto) {
    return Optional.ofNullable(replicationConfig.getReplicationType())
            .map(CONTAINER_REPORT_VALIDATOR.replicaValidators::get)
            .map(replicaValidator ->
                    replicaValidator.validate(replicationConfig, replicaProto))
            .orElse(true);
  }
  public static boolean validate(ContainerInfo containerInfo,
                                 ContainerReplicaProto replicaProto) {
    return validateReplica(containerInfo.getReplicationConfig(), replicaProto);
  }


}
