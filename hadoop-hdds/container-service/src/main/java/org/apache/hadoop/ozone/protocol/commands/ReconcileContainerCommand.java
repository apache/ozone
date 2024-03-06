package org.apache.hadoop.ozone.protocol.commands;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReconcileContainerCommandProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

/**
 * Asks datanodes to reconcile the specified container with other container replicas.
 */
public class ReconcileContainerCommand extends SCMCommand<ReconcileContainerCommandProto> {

  private final List<DatanodeDetails> sourceDatanodes;

  public ReconcileContainerCommand(long containerID, List<DatanodeDetails> sourceDatanodes) {
    // Container ID serves as command ID, since only one reconciliation should be in progress at a time.
    super(containerID);
    this.sourceDatanodes = sourceDatanodes;
  }


  @Override
  public SCMCommandProto.Type getType() {
    return SCMCommandProto.Type.reconcileContainerCommand;
  }

  @Override
  public ReconcileContainerCommandProto getProto() {
    ReconcileContainerCommandProto.Builder builder = ReconcileContainerCommandProto.newBuilder()
        .setContainerID(getId());
    for (DatanodeDetails dd : sourceDatanodes) {
      builder.addSources(dd.getProtoBufMessage());
    }
    return builder.build();
  }

  public List<DatanodeDetails> getSourceDatanodes() {
    return sourceDatanodes;
  }

  public long getContainerID() {
    return getId();
  }

  public static ReconcileContainerCommand getFromProtobuf(ReconcileContainerCommandProto protoMessage) {
    Preconditions.checkNotNull(protoMessage);

    List<HddsProtos.DatanodeDetailsProto> sources = protoMessage.getSourcesList();
    List<DatanodeDetails> sourceNodes = !sources.isEmpty()
        ? sources.stream()
        .map(DatanodeDetails::getFromProtoBuf)
        .collect(Collectors.toList())
        : emptyList();

    return new ReconcileContainerCommand(protoMessage.getContainerID(), sourceNodes);
  }

  @Override
  public String toString() {
    return getType() +
        ": containerId=" + getContainerID() +
        ", sourceNodes=" + sourceDatanodes;
  }
}
