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

package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.hdds.scm.events.SCMEvents.CMD_STATUS_REPORT;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.CONTAINER_ACTIONS;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.CONTAINER_REPORT;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.INCREMENTAL_CONTAINER_REPORT;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.NODE_REPORT;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.PIPELINE_ACTIONS;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.PIPELINE_REPORT;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.ozone.container.upgrade.UpgradeUtils.toLayoutVersionProto;

import com.google.protobuf.Message;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandQueueReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerActionsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineActionsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.IEventInfo;
import org.apache.hadoop.ozone.protocol.commands.ReregisterCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for dispatching heartbeat from datanode to
 * appropriate EventHandler at SCM.
 */
public final class SCMDatanodeHeartbeatDispatcher {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMDatanodeHeartbeatDispatcher.class);

  private final NodeManager nodeManager;
  private final EventPublisher eventPublisher;

  public SCMDatanodeHeartbeatDispatcher(NodeManager nodeManager,
                                        EventPublisher eventPublisher) {
    Objects.requireNonNull(nodeManager, "nodeManager == null");
    Objects.requireNonNull(eventPublisher, "eventPublisher == null");
    this.nodeManager = nodeManager;
    this.eventPublisher = eventPublisher;
  }

  /**
   * Dispatches heartbeat to registered event handlers.
   *
   * @param heartbeat heartbeat to be dispatched.
   *
   * @return list of SCMCommand
   */
  public List<SCMCommand<?>> dispatch(SCMHeartbeatRequestProto heartbeat) {
    DatanodeDetails datanodeDetails =
        DatanodeDetails.getFromProtoBuf(heartbeat.getDatanodeDetails());
    List<SCMCommand<?>> commands;

    // If node is not registered, ask the node to re-register. Do not process
    // Heartbeat for unregistered nodes.
    if (!nodeManager.isNodeRegistered(datanodeDetails)) {
      LOG.info("SCM received heartbeat from an unregistered datanode {}. " +
          "Asking datanode to re-register.", datanodeDetails);
      DatanodeID dnID = datanodeDetails.getID();
      nodeManager.addDatanodeCommand(datanodeDetails.getID(), new ReregisterCommand());

      commands = nodeManager.getCommandQueue(dnID);

    } else {

      LayoutVersionProto layoutVersion = null;
      if (!heartbeat.hasDataNodeLayoutVersion()) {
        // Backward compatibility to make sure old Datanodes can still talk to
        // SCM.
        layoutVersion = toLayoutVersionProto(INITIAL_VERSION.layoutVersion(),
            INITIAL_VERSION.layoutVersion());
      } else {
        layoutVersion = heartbeat.getDataNodeLayoutVersion();
      }

      LOG.debug("Processing DataNode Layout Report.");
      nodeManager.processLayoutVersionReport(datanodeDetails, layoutVersion);

      CommandQueueReportProto commandQueueReport = null;
      if (heartbeat.hasCommandQueueReport()) {
        commandQueueReport = heartbeat.getCommandQueueReport();
      }
      // should we dispatch heartbeat through eventPublisher?
      commands = nodeManager.processHeartbeat(datanodeDetails, commandQueueReport);
      if (heartbeat.hasNodeReport()) {
        LOG.debug("Dispatching Node Report.");
        eventPublisher.fireEvent(
            NODE_REPORT,
            new NodeReportFromDatanode(
                datanodeDetails,
                heartbeat.getNodeReport()));
      }

      if (heartbeat.hasContainerReport()) {
        LOG.debug("Dispatching Container Report.");
        eventPublisher.fireEvent(
            CONTAINER_REPORT,
            new ContainerReportFromDatanode(
                datanodeDetails,
                heartbeat.getContainerReport()));

      }

      final List<IncrementalContainerReportProto> icrs =
          heartbeat.getIncrementalContainerReportList();

      if (!icrs.isEmpty()) {
        LOG.debug("Dispatching ICRs.");
        for (IncrementalContainerReportProto icr : icrs) {
          eventPublisher.fireEvent(INCREMENTAL_CONTAINER_REPORT,
              new IncrementalContainerReportFromDatanode(
                  datanodeDetails, icr));
        }
      }

      if (heartbeat.hasContainerActions()) {
        LOG.debug("Dispatching Container Actions.");
        eventPublisher.fireEvent(
            CONTAINER_ACTIONS,
            new ContainerActionsFromDatanode(
                datanodeDetails,
                heartbeat.getContainerActions()));
      }

      if (heartbeat.hasPipelineReports()) {
        LOG.debug("Dispatching Pipeline Report.");
        eventPublisher.fireEvent(
            PIPELINE_REPORT,
            new PipelineReportFromDatanode(
                datanodeDetails,
                heartbeat.getPipelineReports()));

      }

      if (heartbeat.hasPipelineActions()) {
        LOG.debug("Dispatching Pipeline Actions.");
        eventPublisher.fireEvent(
            PIPELINE_ACTIONS,
            new PipelineActionsFromDatanode(
                datanodeDetails,
                heartbeat.getPipelineActions()));
      }

      if (heartbeat.getCommandStatusReportsCount() != 0) {
        LOG.debug("Dispatching Command Status Report.");
        for (CommandStatusReportsProto commandStatusReport : heartbeat
            .getCommandStatusReportsList()) {
          eventPublisher.fireEvent(
              CMD_STATUS_REPORT,
              new CommandStatusReportFromDatanode(
                  datanodeDetails,
                  commandStatusReport));
        }
      }
    }
    LOG.debug("Heartbeat dispatched for datanode {} with commands {}", datanodeDetails, commands);

    return commands;
  }

  /**
   * Wrapper class for events with the datanode origin.
   */
  public static class ReportFromDatanode<T extends Message> {

    private final DatanodeDetails datanodeDetails;

    private T report;

    public ReportFromDatanode(DatanodeDetails datanodeDetails, T report) {
      this.datanodeDetails = datanodeDetails;
      this.report = report;
    }

    public DatanodeDetails getDatanodeDetails() {
      return datanodeDetails;
    }

    public T getReport() {
      return report;
    }

    public void setReport(T report) {
      this.report = report;
    }
  }

  /**
   * Node report event payload with origin.
   */
  public static class NodeReportFromDatanode
      extends ReportFromDatanode<NodeReportProto> {

    public NodeReportFromDatanode(DatanodeDetails datanodeDetails,
        NodeReportProto report) {
      super(datanodeDetails, report);
    }
  }

  /**
   * Command Queue Report with origin.
   */
  public static class CommandQueueReportFromDatanode
      extends ReportFromDatanode<CommandQueueReportProto> {

    private final Map<SCMCommandProto.Type, Integer> commandsToBeSent;

    public CommandQueueReportFromDatanode(DatanodeDetails datanodeDetails,
        CommandQueueReportProto report,
        Map<SCMCommandProto.Type, Integer> commandsToBeSent) {
      super(datanodeDetails, report);
      this.commandsToBeSent = commandsToBeSent;
    }

    public Map<SCMCommandProto.Type, Integer> getCommandsToBeSent() {
      return commandsToBeSent;
    }
  }

  /**
   * Layout report event payload with origin.
   */
  public static class LayoutReportFromDatanode
      extends ReportFromDatanode<LayoutVersionProto> {

    public LayoutReportFromDatanode(DatanodeDetails datanodeDetails,
                                  LayoutVersionProto report) {
      super(datanodeDetails, report);
    }
  }

  /**
   * Container report payload base reference.
   */
  public interface ContainerReport {
    DatanodeDetails getDatanodeDetails();

    ContainerReportType getType();

    void mergeReport(ContainerReport val);
  }

  /**
   * Container Report Type.
   */
  public enum ContainerReportType {
    /**
     * Incremental container report type
     * {@link IncrementalContainerReportFromDatanode}.
     */
    ICR,
    /**
     * Full container report type
     * {@link ContainerReportFromDatanode}.
     */
    FCR
  }

  /**
   * Container report event payload with origin.
   */
  public static class ContainerReportFromDatanode
      extends ReportFromDatanode<ContainerReportsProto>
      implements ContainerReport, IEventInfo {
    private long createTime = Time.monotonicNow();
    // Used to identify whether container reporting is from a registration.
    private boolean isRegister = false;

    public ContainerReportFromDatanode(DatanodeDetails datanodeDetails,
        ContainerReportsProto report) {
      super(datanodeDetails, report);
    }

    public ContainerReportFromDatanode(DatanodeDetails datanodeDetails,
        ContainerReportsProto report, boolean isRegister) {
      super(datanodeDetails, report);
      this.isRegister = isRegister;
    }

    @Override
    public boolean equals(Object o) {
      return this == o;
    }

    @Override
    public int hashCode() {
      return this.getDatanodeDetails().getID().hashCode();
    }
    
    @Override
    public ContainerReportType getType() {
      return ContainerReportType.FCR;
    }
    
    @Override
    public long getCreateTime() {
      return createTime;
    }

    public boolean isRegister() {
      return isRegister;
    }

    @Override
    public String getEventId() {
      return getDatanodeDetails().toString() + ", {type: " + getType()
          + ", size: " + getReport().getReportsList().size() + "}";
    }

    @Override
    public void mergeReport(ContainerReport nextReport) { }
  }

  /**
   * Incremental Container report event payload with origin.
   */
  public static class IncrementalContainerReportFromDatanode
      extends ReportFromDatanode<IncrementalContainerReportProto>
      implements ContainerReport, IEventInfo {
    private long createTime = Time.monotonicNow();
    
    public IncrementalContainerReportFromDatanode(
        DatanodeDetails datanodeDetails,
        IncrementalContainerReportProto report) {
      super(datanodeDetails, report);
    }

    @Override
    public boolean equals(Object o) {
      return this == o;
    }

    @Override
    public int hashCode() {
      return this.getDatanodeDetails().getID().hashCode();
    }

    @Override
    public ContainerReportType getType() {
      return ContainerReportType.ICR;
    }

    @Override
    public long getCreateTime() {
      return createTime;
    }
    
    @Override
    public String getEventId() {
      return getDatanodeDetails().toString() + ", {type: " + getType()
          + ", size: " + getReport().getReportList().size() + "}";
    }

    @Override
    public void mergeReport(ContainerReport nextReport) {
      if (nextReport.getType() == ContainerReportType.ICR) {
        // To update existing report list , need to create a builder and then
        // merge new reports to existing report list.
        IncrementalContainerReportProto reportProto = getReport().toBuilder().addAllReport(
            ((ReportFromDatanode<IncrementalContainerReportProto>) nextReport).getReport().getReportList()).build();
        setReport(reportProto);
      }
    }
  }

  /**
   * Container action event payload with origin.
   */
  public static class ContainerActionsFromDatanode
      extends ReportFromDatanode<ContainerActionsProto> {

    public ContainerActionsFromDatanode(DatanodeDetails datanodeDetails,
                                       ContainerActionsProto actions) {
      super(datanodeDetails, actions);
    }
  }

  /**
   * Pipeline report event payload with origin.
   */
  public static class PipelineReportFromDatanode
          extends ReportFromDatanode<PipelineReportsProto> {

    public PipelineReportFromDatanode(DatanodeDetails datanodeDetails,
                                      PipelineReportsProto report) {
      super(datanodeDetails, report);
    }
  }

  /**
   * Pipeline action event payload with origin.
   */
  public static class PipelineActionsFromDatanode
      extends ReportFromDatanode<PipelineActionsProto> {

    public PipelineActionsFromDatanode(DatanodeDetails datanodeDetails,
        PipelineActionsProto actions) {
      super(datanodeDetails, actions);
    }
  }

  /**
   * Container report event payload with origin.
   */
  public static class CommandStatusReportFromDatanode
      extends ReportFromDatanode<CommandStatusReportsProto> {

    public CommandStatusReportFromDatanode(DatanodeDetails datanodeDetails,
        CommandStatusReportsProto report) {
      super(datanodeDetails, report);
    }
  }
}
