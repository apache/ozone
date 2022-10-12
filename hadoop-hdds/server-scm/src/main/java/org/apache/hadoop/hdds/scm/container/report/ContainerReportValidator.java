package org.apache.hadoop.hdds.scm.container.report;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;

import java.util.Map;
import java.util.Optional;

public class ContainerReportValidator {
  private interface ReplicaValidator {
    public boolean validate(ReplicationConfig replicationConfig,
                            ContainerReplicaProto replicaProto);
  }

  private static class ECReplicaValidator implements ReplicaValidator {

    @Override
    public boolean validate(ReplicationConfig replicationConfig,
                            ContainerReplicaProto replicaProto) {

      return replicationConfig.getReplicationType() == ReplicationType.EC
      && replicaProto.hasReplicaIndex() && replicaProto.getReplicaIndex()>0
      && replicaProto.getReplicaIndex() <= replicationConfig.getRequiredNodes();
    }
  }

  private static final Map<ReplicationType, ReplicaValidator> replicaValidators;
  static {
    replicaValidators = ImmutableMap.of(ReplicationType.EC, new ECReplicaValidator());
  }
  private static boolean validateReplica(ReplicationConfig replicationConfig,
                                         ContainerReplicaProto replicaProto) {
    return Optional.ofNullable(replicationConfig.getReplicationType())
            .map(replicaValidators::get)
            .map(replicaValidator ->
                    replicaValidator.validate(replicationConfig, replicaProto))
            .orElse(true);
  }
  public static boolean validate(ContainerInfo containerInfo,
                                 ContainerReplicaProto replicaProto) {
    return validateReplica(containerInfo.getReplicationConfig(), replicaProto);
  }


}
