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

package org.apache.hadoop.hdds.scm.cli.datanode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.DiskBalancerProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDiskBalancerInfoProto;
import picocli.CommandLine.Command;

/**
 * Handler to get disk balancer status.
 */
@Command(
    name = "status",
    description = "Get DiskBalancer status",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DiskBalancerStatusSubcommand extends AbstractDiskBalancerSubCommand {

  // Store statuses for non-JSON mode consolidation
  private final Map<String, DatanodeDiskBalancerInfoProto> statuses =
      new ConcurrentHashMap<>();

  @Override
  protected Object executeCommand(String hostName) throws IOException {
    DiskBalancerProtocol diskBalancerProxy = DiskBalancerSubCommandUtil
        .getSingleNodeDiskBalancerProxy(hostName);
    try {
      final DatanodeDiskBalancerInfoProto status = diskBalancerProxy.getDiskBalancerInfo();
      
      // Only create JSON result object if JSON mode is enabled
      if (getOptions().isJson()) {
        return createStatusResult(status);
      }
      
      // For non-JSON mode, store the proto for later consolidation
      statuses.put(hostName, status);
      return status; // Return non-null to indicate success
    } finally {
      diskBalancerProxy.close();
    }
  }

  @Override
  protected void displayResults(List<String> successNodes, List<String> failedNodes) {
    // In JSON mode, results are already written
    if (getOptions().isJson()) {
      return;
    }

    // Display error messages for failed nodes
    if (!failedNodes.isEmpty()) {
      System.err.printf("Failed to get DiskBalancer status from nodes: [%s]%n", 
          String.join(", ", failedNodes));
    }

    // Display consolidated status for successful nodes
    if (!successNodes.isEmpty() && !statuses.isEmpty()) {
      List<DatanodeDiskBalancerInfoProto> statusList =
          new ArrayList<>(statuses.values());
      System.out.println(generateStatus(statusList));
    }
  }

  private String generateStatus(List<DatanodeDiskBalancerInfoProto> protos) {
    StringBuilder formatBuilder = new StringBuilder("Status result:%n" +
        "%-35s %-15s %-15s %-15s %-12s %-20s %-12s %-12s %-15s %-18s %-20s%n");

    List<String> contentList = new ArrayList<>();
    contentList.add("Datanode");
    contentList.add("Status");
    contentList.add("Threshold(%)");
    contentList.add("BandwidthInMB");
    contentList.add("Threads");
    contentList.add("StopAfterDiskEven");
    contentList.add("SuccessMove");
    contentList.add("FailureMove");
    contentList.add("BytesMoved(MB)");
    contentList.add("EstBytesToMove(MB)");
    contentList.add("EstTimeLeft(min)");

    for (HddsProtos.DatanodeDiskBalancerInfoProto proto : protos) {
      formatBuilder.append("%-35s %-15s %-15s %-15s %-12s %-20s %-12s %-12s %-15s %-18s %-20s%n");
      long estimatedTimeLeft = calculateEstimatedTimeLeft(proto);
      long bytesMovedMB = (long) Math.ceil(proto.getBytesMoved() / (1024.0 * 1024.0));
      long bytesToMoveMB = (long) Math.ceil(proto.getBytesToMove() / (1024.0 * 1024.0));

      contentList.add(proto.getNode().getHostName());
      contentList.add(proto.getRunningStatus().name());
      contentList.add(
          String.format("%.4f", proto.getDiskBalancerConf().getThreshold()));
      contentList.add(
          String.valueOf(proto.getDiskBalancerConf().getDiskBandwidthInMB()));
      contentList.add(
          String.valueOf(proto.getDiskBalancerConf().getParallelThread()));
      contentList.add(
          String.valueOf(proto.getDiskBalancerConf().getStopAfterDiskEven()));
      contentList.add(String.valueOf(proto.getSuccessMoveCount()));
      contentList.add(String.valueOf(proto.getFailureMoveCount()));
      contentList.add(String.valueOf(bytesMovedMB));
      contentList.add(String.valueOf(bytesToMoveMB));
      contentList.add(estimatedTimeLeft >= 0 ? String.valueOf(estimatedTimeLeft) : "N/A");
    }

    formatBuilder.append("%nNote:%n");
    formatBuilder.append("  - EstBytesToMove is calculated based on the target disk even state" +
        " with the configured threshold.%n");
    formatBuilder.append("  - EstTimeLeft is calculated based on EstimatedBytesToMove and configured" +
        " disk bandwidth.%n");
    formatBuilder.append("  - Both EstimatedBytes and EstTimeLeft could be non-zero while no containers" +
        " can be moved, especially when the configured threshold or disk capacity is too small.");

    return String.format(formatBuilder.toString(),
        contentList.toArray(new String[0]));
  }

  @Override
  protected String getActionName() {
    return "status";
  }

  /**
   * Create a JSON result map for a status.
   * 
   * @param status the DiskBalancer status proto
   * @return JSON result map
   */
  private Map<String, Object> createStatusResult(DatanodeDiskBalancerInfoProto status) {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("datanode", status.getNode().getHostName());
    result.put("action", "status");
    result.put("status", "success");
    result.put("serviceStatus", status.getRunningStatus().name());
    result.put("threshold", status.getDiskBalancerConf().getThreshold());
    result.put("bandwidthInMB", status.getDiskBalancerConf().getDiskBandwidthInMB());
    result.put("threads", status.getDiskBalancerConf().getParallelThread());
    result.put("stopAfterDiskEven", status.getDiskBalancerConf().getStopAfterDiskEven());
    result.put("successMove", status.getSuccessMoveCount());
    result.put("failureMove", status.getFailureMoveCount());
    result.put("bytesMovedMB", (long) Math.ceil(status.getBytesMoved() / (1024.0 * 1024.0)));
    result.put("estBytesToMoveMB", (long) Math.ceil(status.getBytesToMove() / (1024.0 * 1024.0)));
    long estimatedTimeLeft = calculateEstimatedTimeLeft(status);
    result.put("estTimeLeftMin", estimatedTimeLeft >= 0 ? estimatedTimeLeft : null);
    return result;
  }

  private long calculateEstimatedTimeLeft(DatanodeDiskBalancerInfoProto proto) {
    long bytesToMove = proto.getBytesToMove();

    if (bytesToMove == 0) {
      return 0;
    }
    long bandwidth = proto.getDiskBalancerConf().getDiskBandwidthInMB();

    // Convert estimated data from bytes to MB
    double estimatedDataPendingMB = bytesToMove / (1024.0 * 1024.0);
    double estimatedTimeLeft = (bandwidth > 0) ? (estimatedDataPendingMB / bandwidth) / 60 : -1;
    return (long) Math.ceil(estimatedTimeLeft);
  }
}
