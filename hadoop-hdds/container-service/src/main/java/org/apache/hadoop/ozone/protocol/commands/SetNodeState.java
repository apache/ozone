package org.apache.hadoop.ozone.protocol.commands;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SetNodeStateCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;

/**
 * This allows SCM to send a command to Datanode asking to set its Node State.
 * <p>
 * A Node State can be the following : IN_SERVICE = 1; DECOMMISSIONING = 2;
 * DECOMMISSIONED = 3; MAINTENANCE = 4; Once the node state is change is handled
 * by the data node; the data node will report its state back to the SCM that
 * allows the changes to be propogated back into SCM via the standard SCM
 * architecture.
 * <p>
 * SCM today listens to the data node reports to learn the state of nodes and
 * containers.
 */
public class SetNodeState extends SCMCommand<SetNodeStateCommandProto> {
  private final HddsProtos.NodeOperationalState state;

  /**
   * Ctor that creates a set node state command.
   *
   * @param id - Command ID. Something a time stamp would suffice.
   * @param state - NodeState that want the node to be set into.
   */
  public SetNodeState(long id, HddsProtos.NodeOperationalState state) {
    super(id);
    this.state = state;
  }

  /**
   * Returns the type of this command.
   *
   * @return Type  - This is setNodeStateCommand.
   */
  @Override
  public SCMCommandProto.Type getType() {
    return SCMCommandProto.Type.setNodeStateCommand;
  }

  /**
   * Gets the protobuf message of this object.
   *
   * @return A protobuf message.
   */
  @Override
  public SetNodeStateCommandProto getProto() {
    return SetNodeStateCommandProto.newBuilder()
        .setNodeState(state).build();
  }
}
