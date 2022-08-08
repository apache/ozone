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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
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
          .setCurrentVolumeDensitySum(String.valueOf(volumeDensitySum))
          .setNode(datanodeDetails.toProto(clientVersion))
          .build());
    }

    reportList.sort((t1, t2) -> t2.getCurrentVolumeDensitySum().
        compareTo(t1.getCurrentVolumeDensitySum()));
    return reportList.stream().limit(count).collect(Collectors.toList());
  }

  public List<HddsProtos.DatanodeDiskBalancerInfoProto> getDiskBalancerStatus(
      Optional<List<String>> hosts, int clientVersion) throws IOException {
    List<HddsProtos.DatanodeDiskBalancerInfoProto> statusList =
        new ArrayList<>();
    List<DatanodeDetails> filterDns = null;

    if (hosts.isPresent() && !hosts.get().isEmpty()) {
      filterDns = mapHostnamesToDatanodes(hosts.get());
    }

    for (DatanodeDetails datanodeDetails: nodeManager.getNodes(IN_SERVICE,
        HddsProtos.NodeState.HEALTHY)) {
      if ((filterDns != null && !filterDns.isEmpty() &&
          filterDns.contains(datanodeDetails) &&
          statusMap.containsKey(datanodeDetails)) ||
          isRunning(datanodeDetails)) {
        double volumeDensitySum =
            getVolumeDataDensitySumForDatanodeDetails(datanodeDetails);
        statusList.add(HddsProtos.DatanodeDiskBalancerInfoProto.newBuilder()
            .setCurrentVolumeDensitySum(String.valueOf(volumeDensitySum))
            .setDiskBalancerRunning(isRunning(datanodeDetails))
            .setDiskBalancerConf(statusMap.get(datanodeDetails)
                .getDiskBalancerConfiguration().toProtobufBuilder())
            .setNode(datanodeDetails.toProto(clientVersion))
            .build());
      }
    }

    return statusList;
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

  private List<DatanodeDetails> mapHostnamesToDatanodes(List<String> hosts)
      throws InvalidHostStringException {
    List<DatanodeDetails> results = new LinkedList<>();
    for (String hostString : hosts) {
      NodeDecommissionManager.HostDefinition
          host = new NodeDecommissionManager.HostDefinition(hostString);
      InetAddress addr;
      try {
        addr = InetAddress.getByName(host.getHostname());
      } catch (UnknownHostException e) {
        throw new InvalidHostStringException("Unable to resolve host "
            + host.getRawHostname(), e);
      }
      String dnsName;
      if (useHostnames) {
        dnsName = addr.getHostName();
      } else {
        dnsName = addr.getHostAddress();
      }
      List<DatanodeDetails> found = nodeManager.getNodesByAddress(dnsName);
      if (found.size() == 0) {
        throw new InvalidHostStringException("Host " + host.getRawHostname()
            + " (" + dnsName + ") is not running any datanodes registered"
            + " with SCM."
            + " Please check the host name.");
      } else if (found.size() == 1) {
        if (host.getPort() != -1 &&
            !validateDNPortMatch(host.getPort(), found.get(0))) {
          throw new InvalidHostStringException("Host " + host.getRawHostname()
              + " is running a datanode registered with SCM,"
              + " but the port number doesn't match."
              + " Please check the port number.");
        }
        results.add(found.get(0));
      } else if (found.size() > 1) {
        DatanodeDetails match = null;
        for (DatanodeDetails dn : found) {
          if (validateDNPortMatch(host.getPort(), dn)) {
            match = dn;
            break;
          }
        }
        if (match == null) {
          throw new InvalidHostStringException("Host " + host.getRawHostname()
              + " is running multiple datanodes registered with SCM,"
              + " but no port numbers match."
              + " Please check the port number.");
        }
        results.add(match);
      }
    }
    return results;
  }

  /**
   * Check if the passed port is used by the given DatanodeDetails object. If
   * it is, return true, otherwise return false.
   * @param port Port number to check if it is used by the datanode
   * @param dn Datanode to check if it is using the given port
   * @return True if port is used by the datanode. False otherwise.
   */
  private boolean validateDNPortMatch(int port, DatanodeDetails dn) {
    for (DatanodeDetails.Port p : dn.getPorts()) {
      if (p.getValue() == port) {
        return true;
      }
    }
    return false;
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