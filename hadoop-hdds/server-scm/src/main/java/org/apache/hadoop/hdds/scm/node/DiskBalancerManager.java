/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.node;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DiskBalancerReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.storage.DiskBalancerConfiguration;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.DiskBalancerCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;

/**
 * Maintains information about the DiskBalancer on SCM side.
 */
public class DiskBalancerManager {
  public static final Logger LOG =
      LoggerFactory.getLogger(DiskBalancerManager.class);


  private OzoneConfiguration conf;
  private final EventPublisher scmNodeEventPublisher;
  private final SCMContext scmContext;
  private final NodeManager nodeManager;
  private Map<DatanodeDetails, DiskBalancerStatus> statusMap;
  private Map<DatanodeDetails, Long> balancedBytesMap;
  private boolean useHostnames;

  /**
   * Constructs DiskBalancer Manager.
   */
  public DiskBalancerManager(OzoneConfiguration conf,
                        EventPublisher eventPublisher,
                        SCMContext scmContext,
                        NodeManager nodeManager) {
    this.conf = conf;
    this.scmNodeEventPublisher = eventPublisher;
    this.scmContext = scmContext;
    this.nodeManager = nodeManager;
    this.useHostnames = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME,
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
    this.statusMap = new ConcurrentHashMap<>();
  }

  public List<HddsProtos.DatanodeDiskBalancerInfoProto> getDiskBalancerReport(
      int count, int clientVersion) throws IOException {

    List<HddsProtos.DatanodeDiskBalancerInfoProto> reportList =
        new ArrayList<>();

    for (DatanodeDetails datanodeDetails: nodeManager.getNodes(IN_SERVICE,
        HddsProtos.NodeState.HEALTHY)) {
      double volumeDensitySum =
          getVolumeDataDensitySumForDatanodeDetails(datanodeDetails);
      reportList.add(HddsProtos.DatanodeDiskBalancerInfoProto.newBuilder()
          .setCurrentVolumeDensitySum(volumeDensitySum)
          .setNode(datanodeDetails.toProto(clientVersion))
          .build());
    }

    reportList.sort((t1, t2) -> Double.compare(t2.getCurrentVolumeDensitySum(),
        t1.getCurrentVolumeDensitySum()));
    return reportList.stream().limit(count).collect(Collectors.toList());
  }

  /**
   * If hosts is not null, return status of hosts;
   * If hosts is null, return status of all datanodes in balancing.
   */
  public List<HddsProtos.DatanodeDiskBalancerInfoProto> getDiskBalancerStatus(
      Optional<List<String>> hosts,
      Optional<HddsProtos.DiskBalancerRunningStatus> status,
      int clientVersion) throws IOException {
    List<DatanodeDetails> filterDns = null;
    if (hosts.isPresent() && !hosts.get().isEmpty()) {
      filterDns = NodeUtils.mapHostnamesToDatanodes(nodeManager, hosts.get(),
          useHostnames);
    }

    // Filter Running Status by default
    HddsProtos.DiskBalancerRunningStatus filterStatus = status.orElse(null);

    if (filterDns != null) {
      return filterDns.stream()
          .filter(dn -> shouldReturnDatanode(filterStatus, dn))
          .map(nodeManager::getDatanodeInfo)
          .map(dn -> getInfoProto(dn, clientVersion))
          .collect(Collectors.toList());
    } else {
      return nodeManager.getAllNodes().stream()
          .filter(dn -> shouldReturnDatanode(filterStatus, dn))
          .map(dn -> getInfoProto((DatanodeInfo)dn, clientVersion))
          .collect(Collectors.toList());
    }
  }

  /**
   * Send startDiskBalancer command to datanodes.
   * If hosts is not specified, send commands to all healthy datanodes.
   * @param threshold new configuration of threshold
   * @param bandwidthInMB new configuration of bandwidthInMB
   * @param parallelThread new configuration of parallelThread
   * @param hosts Datanodes that command will apply on
   * @return Possible errors
   * @throws IOException
   */
  public List<DatanodeAdminError> startDiskBalancer(
      Optional<Double> threshold, Optional<Long> bandwidthInMB,
      Optional<Integer> parallelThread, Optional<List<String>> hosts)
      throws IOException {
    List<DatanodeDetails> dns;
    if (hosts.isPresent()) {
      dns = NodeUtils.mapHostnamesToDatanodes(nodeManager, hosts.get(),
          useHostnames);
    } else {
      dns = nodeManager.getNodes(NodeStatus.inServiceHealthy());
    }

    List<DatanodeAdminError> errors = new ArrayList<>();
    for (DatanodeDetails dn : dns) {
      try {
        if (!nodeManager.getNodeStatus(dn).isHealthy()) {
          errors.add(new DatanodeAdminError(dn.getHostName(),
              "Datanode not in healthy state"));
          continue;
        }
        // If command doesn't have configuration change, then we reuse the
        // latest configuration reported from Datnaodes
        DiskBalancerConfiguration updateConf = attachDiskBalancerConf(dn,
            threshold, bandwidthInMB, parallelThread);
        DiskBalancerCommand command = new DiskBalancerCommand(
            HddsProtos.DiskBalancerOpType.START, updateConf);
        sendCommand(dn, command);
      } catch (Exception e) {
        LOG.info("Caught an error for {}", dn);
        errors.add(new DatanodeAdminError(dn.getHostName(), e.getMessage()));
      }
    }
    return errors;
  }

  /**
   * Send stopDiskBalancer command to datanodes
   * If hosts is not specified, send commands to all datanodes.
   * @param hosts Datanodes that command will apply on
   * */
  public List<DatanodeAdminError> stopDiskBalancer(Optional<List<String>> hosts)
      throws IOException {
    List<DatanodeDetails> dns;
    if (hosts.isPresent()) {
      dns = NodeUtils.mapHostnamesToDatanodes(nodeManager, hosts.get(),
          useHostnames);
    } else {
      dns = nodeManager.getNodes(NodeStatus.inServiceHealthy());
    }

    List<DatanodeAdminError> errors = new ArrayList<>();
    for (DatanodeDetails dn : dns) {
      try {
        DiskBalancerCommand command = new DiskBalancerCommand(
            HddsProtos.DiskBalancerOpType.STOP, null);
        sendCommand(dn, command);
      } catch (Exception e) {
        errors.add(new DatanodeAdminError(dn.getHostName(), e.getMessage()));
      }
    }
    return errors;
  }


  /**
   * Send update DiskBalancerConf command to datanodes.
   * If hosts is not specified, send commands to all healthy datanodes.
   * @param threshold new configuration of threshold
   * @param bandwidthInMB new configuration of bandwidthInMB
   * @param parallelThread new configuration of parallelThread
   * @param hosts Datanodes that command will apply on
   * @return Possible errors
   * @throws IOException
   */
  public List<DatanodeAdminError> updateDiskBalancerConfiguration(
      Optional<Double> threshold, Optional<Long> bandwidthInMB,
      Optional<Integer> parallelThread, Optional<List<String>> hosts)
      throws IOException {
    List<DatanodeDetails> dns;
    if (hosts.isPresent()) {
      dns = NodeUtils.mapHostnamesToDatanodes(nodeManager, hosts.get(),
          useHostnames);
    } else {
      dns = nodeManager.getNodes(NodeStatus.inServiceHealthy());
    }

    List<DatanodeAdminError> errors = new ArrayList<>();
    for (DatanodeDetails dn : dns) {
      try {
        // If command doesn't have configuration change, then we reuse the
        // latest configuration reported from Datnaodes
        DiskBalancerConfiguration updateConf = attachDiskBalancerConf(dn,
            threshold, bandwidthInMB, parallelThread);
        DiskBalancerCommand command = new DiskBalancerCommand(
            HddsProtos.DiskBalancerOpType.UPDATE, updateConf);
        sendCommand(dn, command);
      } catch (Exception e) {
        errors.add(new DatanodeAdminError(dn.getHostName(), e.getMessage()));
      }
    }
    return errors;
  }

  private boolean shouldReturnDatanode(
      HddsProtos.DiskBalancerRunningStatus status,
      DatanodeDetails datanodeDetails) {
    boolean shouldReturn = true;
    // If status specified, do not return if status not match.
    if (status != null && getRunningStatus(datanodeDetails) != status) {
      shouldReturn = false;
    }
    return shouldReturn;
  }

  private HddsProtos.DatanodeDiskBalancerInfoProto getInfoProto(
      DatanodeInfo dn, int clientVersion) {
    double volumeDensitySum =
        getVolumeDataDensitySumForDatanodeDetails(dn);
    HddsProtos.DiskBalancerRunningStatus runningStatus =
        getRunningStatus(dn);
    HddsProtos.DatanodeDiskBalancerInfoProto.Builder builder =
        HddsProtos.DatanodeDiskBalancerInfoProto.newBuilder()
            .setNode(dn.toProto(clientVersion))
            .setCurrentVolumeDensitySum(volumeDensitySum)
            .setRunningStatus(getRunningStatus(dn));
    if (runningStatus != HddsProtos.DiskBalancerRunningStatus.UNKNOWN) {
      builder.setDiskBalancerConf(statusMap.get(dn)
          .getDiskBalancerConfiguration().toProtobufBuilder());
    }
    return builder.build();
  }
  /**
   * Get volume density for a specific DatanodeDetails node.
   *
   * @param datanodeDetails DatanodeDetails
   * @return DiskBalancer report.
   */
  private double getVolumeDataDensitySumForDatanodeDetails(
      DatanodeDetails datanodeDetails) {
    Preconditions.checkArgument(datanodeDetails instanceof DatanodeInfo);

    DatanodeInfo datanodeInfo = (DatanodeInfo) datanodeDetails;

    double totalCapacity = 0d, totalUsed = 0d;
    for (StorageReportProto reportProto : datanodeInfo.getStorageReports()) {
      totalCapacity += reportProto.getCapacity();
      totalUsed += reportProto.getScmUsed();
    }

    Preconditions.checkArgument(totalCapacity != 0);
    double idealUsage = totalUsed / totalCapacity;

    double volumeDensitySum = datanodeInfo.getStorageReports().stream()
        .map(report ->
            Math.abs((double)report.getScmUsed() / report.getCapacity()
                - idealUsage))
        .mapToDouble(Double::valueOf).sum();

    return volumeDensitySum;
  }

  private HddsProtos.DiskBalancerRunningStatus getRunningStatus(
      DatanodeDetails datanodeDetails) {
    if (!statusMap.containsKey(datanodeDetails)) {
      return HddsProtos.DiskBalancerRunningStatus.UNKNOWN;
    } else {
      if (statusMap.get(datanodeDetails).isRunning()) {
        return HddsProtos.DiskBalancerRunningStatus.RUNNING;
      } else {
        return HddsProtos.DiskBalancerRunningStatus.STOPPED;
      }
    }
  }

  @VisibleForTesting
  public void addRunningDatanode(DatanodeDetails datanodeDetails) {
    statusMap.put(datanodeDetails, new DiskBalancerStatus(true,
        new DiskBalancerConfiguration()));
  }

  public void processDiskBalancerReport(DiskBalancerReportProto reportProto,
      DatanodeDetails dn) {
    boolean isRunning = reportProto.getIsRunning();
    DiskBalancerConfiguration diskBalancerConfiguration =
        reportProto.hasDiskBalancerConf() ?
            DiskBalancerConfiguration.fromProtobuf(
                reportProto.getDiskBalancerConf(), conf) :
            new DiskBalancerConfiguration();
    statusMap.put(dn, new DiskBalancerStatus(isRunning,
        diskBalancerConfiguration));
    if (reportProto.hasBalancedBytes()) {
      balancedBytesMap.put(dn, reportProto.getBalancedBytes());
    }
  }

  private DiskBalancerConfiguration attachDiskBalancerConf(
      DatanodeDetails dn, Optional<Double> threshold,
      Optional<Long> bandwidthInMB, Optional<Integer> parallelThread) {
    DiskBalancerConfiguration baseConf = statusMap.containsKey(dn) ?
        statusMap.get(dn).getDiskBalancerConfiguration() :
        new DiskBalancerConfiguration();
    threshold.ifPresent(baseConf::setThreshold);
    bandwidthInMB.ifPresent(baseConf::setDiskBandwidthInMB);
    parallelThread.ifPresent(baseConf::setParallelThread);
    return baseConf;
  }

  private void sendCommand(DatanodeDetails dn, DiskBalancerCommand command) {
    try {
      command.setTerm(scmContext.getTermOfLeader());
    } catch (NotLeaderException nle) {
      LOG.warn("Skip sending DiskBalancerCommand for Datanode {}," +
          " since not leader SCM.", dn.getUuidString());
      return;
    }
    LOG.info("Sending {} to Datanode {}", command, dn);
    scmNodeEventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND,
        new CommandForDatanode<>(dn.getUuid(), command));
  }

  @VisibleForTesting
  public Map<DatanodeDetails, DiskBalancerStatus> getStatusMap() {
    return statusMap;
  }
}
