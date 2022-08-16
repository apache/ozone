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
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.storage.DiskBalancerConfiguration;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdfs.DFSConfigKeys;
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

  private final EventPublisher scmNodeEventPublisher;
  private final SCMContext scmContext;
  private final NodeManager nodeManager;
  private Map<DatanodeDetails, DiskBalancerStatus> statusMap;
  private boolean useHostnames;

  /**
   * Constructs DiskBalancer Manager.
   */
  public DiskBalancerManager(OzoneConfiguration conf,
                        EventPublisher eventPublisher,
                        SCMContext scmContext,
                        NodeManager nodeManager) {
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
      Optional<List<String>> hosts, int clientVersion) throws IOException {
    List<HddsProtos.DatanodeDiskBalancerInfoProto> statusList =
        new ArrayList<>();
    List<DatanodeDetails> filterDns = null;
    if (hosts.isPresent() && !hosts.get().isEmpty()) {
      filterDns = NodeUtils.mapHostnamesToDatanodes(nodeManager, hosts.get(),
          useHostnames);
    }

    for (DatanodeDetails datanodeDetails: nodeManager.getNodes(IN_SERVICE,
        HddsProtos.NodeState.HEALTHY)) {
      if (shouldReturnDatanode(filterDns, datanodeDetails)) {
        double volumeDensitySum =
            getVolumeDataDensitySumForDatanodeDetails(datanodeDetails);
        statusList.add(HddsProtos.DatanodeDiskBalancerInfoProto.newBuilder()
            .setCurrentVolumeDensitySum(volumeDensitySum)
            .setDiskBalancerRunning(isRunning(datanodeDetails))
            .setDiskBalancerConf(statusMap.getOrDefault(datanodeDetails,
                    DiskBalancerStatus.DUMMY_STATUS)
                .getDiskBalancerConfiguration().toProtobufBuilder())
            .setNode(datanodeDetails.toProto(clientVersion))
            .build());
      }
    }
    return statusList;
  }

  private boolean shouldReturnDatanode(List<DatanodeDetails> hosts,
      DatanodeDetails datanodeDetails) {
    if (hosts == null || hosts.isEmpty()) {
      return isRunning(datanodeDetails);
    } else {
      return hosts.contains(datanodeDetails);
    }
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
    for (StorageContainerDatanodeProtocolProtos.StorageReportProto reportProto :
        datanodeInfo.getStorageReports()) {
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

  private boolean isRunning(DatanodeDetails datanodeDetails) {
    return statusMap
        .getOrDefault(datanodeDetails, DiskBalancerStatus.DUMMY_STATUS)
        .isRunning();
  }

  @VisibleForTesting
  public void addRunningDatanode(DatanodeDetails datanodeDetails) {
    statusMap.put(datanodeDetails, new DiskBalancerStatus(true,
        new DiskBalancerConfiguration()));
  }
}
