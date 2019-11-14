package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.commands.SetNodeState;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * Handles the set Node state command on the data node side.
 */
public class SetNodeStateCommandHandler implements CommandHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(SetNodeStateCommandHandler.class);
  private final OzoneContainer ozoneContainer;

  /**
   * Set Node State command handler.
   *
   * @param ozoneContainer - Ozone Container.
   */
  public SetNodeStateCommandHandler(OzoneContainer ozoneContainer) {
    this.ozoneContainer = ozoneContainer;
  }

  /**
   * Handles a given SCM command.
   *
   * @param command - SCM Command
   * @param container - Ozone Container.
   * @param context - Current Context.
   * @param connectionManager - The SCMs that we are talking to.
   */
  @Override
  public void handle(SCMCommand command, OzoneContainer container,
      StateContext context, SCMConnectionManager connectionManager) {
    long startTime = Time.monotonicNow();
    StorageContainerDatanodeProtocolProtos.SetNodeStateCommandProto
        setNodeCmdProto = null;

    if (command.getType() != Type.setNodeStateCommand) {
      LOG.warn("Skipping handling command, expected command "
              + "type {} but found {}",
          Type.setNodeStateCommand, command.getType());
      return;
    }
    SetNodeState setNodeCmd = (SetNodeState) command;
    setNodeCmdProto = setNodeCmd.getProto();
    container.setGlobalNodeState(setNodeCmdProto.getNodeState());
  }

  /**
   * Returns the command type that this command handler handles.
   *
   * @return Type
   */
  @Override
  public StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type
  getCommandType() {
    return null;
  }

  /**
   * Returns number of times this handler has been invoked.
   *
   * @return int
   */
  @Override
  public int getInvocationCount() {
    return 0;
  }

  /**
   * Returns the average time this function takes to run.
   *
   * @return long
   */
  @Override
  public long getAverageRunTime() {
    return 0;
  }

  /**
   * Default implementation for updating command status.
   *
   * @param context
   * @param command
   * @param cmdStatusUpdater
   * @param log
   */
  @Override
  public void updateCommandStatus(
      StateContext context, SCMCommand command,
      Consumer<CommandStatus> cmdStatusUpdater, Logger log) {

  }
}
