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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.DiskBalancerProtocol;
import org.apache.hadoop.hdds.protocol.proto.DiskBalancerProtocolProtos.DatanodeDiskBalancerInfoType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDiskBalancerInfoProto;
import org.apache.hadoop.ozone.ClientVersion;
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

  private final List<HddsProtos.DatanodeDiskBalancerInfoProto> statuses = new ArrayList<>();

  @Override
  protected Object executeCommand(String hostName) throws IOException {
    DiskBalancerProtocol diskBalancerProxy = DiskBalancerSubCommandUtil
        .getSingleNodeDiskBalancerProxy(hostName);
    try {
      HddsProtos.DatanodeDiskBalancerInfoProto status = 
          diskBalancerProxy.getDiskBalancerInfo(
              DatanodeDiskBalancerInfoType.STATUS,
              ClientVersion.CURRENT_VERSION);
      statuses.add(status);
      return new StatusResult(status);
    } finally {
      diskBalancerProxy.close();
    }
  }

  @Override
  protected void displayResults(List<String> successNodes, List<String> failedNodes) {
    // In JSON mode, results are already written, only show summary if needed
    if (getOptions().isJson()) {
      return;
    }

    if (!failedNodes.isEmpty()) {
      System.err.printf("Failed to get DiskBalancer status from nodes: [%s]%n", 
          String.join(", ", failedNodes));
    }

    // Display consolidated status for successful nodes
    if (!statuses.isEmpty()) {
      System.out.println(generateStatus(statuses));
    }
  }

  private String generateStatus(List<HddsProtos.DatanodeDiskBalancerInfoProto> protos) {
    StringBuilder formatBuilder = new StringBuilder("Status result:%n" +
        "%-35s %-15s %-15s %-15s %-12s %-12s %-12s %-15s %-15s %-15s%n");

    List<String> contentList = new ArrayList<>();
    contentList.add("Datanode");
    contentList.add("Status");
    contentList.add("Threshold(%)");
    contentList.add("BandwidthInMB");
    contentList.add("Threads");
    contentList.add("SuccessMove");
    contentList.add("FailureMove");
    contentList.add("BytesMoved(MB)");
    contentList.add("EstBytesToMove(MB)");
    contentList.add("EstTimeLeft(min)");

    for (HddsProtos.DatanodeDiskBalancerInfoProto proto : protos) {
      formatBuilder.append("%-35s %-15s %-15s %-15s %-12s %-12s %-12s %-15s %-15s %-15s%n");
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
      contentList.add(String.valueOf(proto.getSuccessMoveCount()));
      contentList.add(String.valueOf(proto.getFailureMoveCount()));
      contentList.add(String.valueOf(bytesMovedMB));
      contentList.add(String.valueOf(bytesToMoveMB));
      contentList.add(estimatedTimeLeft >= 0 ? String.valueOf(estimatedTimeLeft) : "N/A");
    }

    formatBuilder.append("%nNote: Estimated time left is calculated" +
        " based on the estimated bytes to move and the configured disk bandwidth.");

    return String.format(formatBuilder.toString(),
        contentList.toArray(new String[0]));
  }

  private long calculateEstimatedTimeLeft(HddsProtos.DatanodeDiskBalancerInfoProto proto) {
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

  /**
   * Wrapper class for JSON serialization of DiskBalancer status.
   */
  @JsonPropertyOrder({
      "datanode", "status", "threshold", "bandwidthInMB", "threads",
      "successMove", "failureMove", "bytesMovedMB", "estBytesToMoveMB", "estTimeLeftMin"
  })
  private static class StatusResult {
    private final DatanodeDiskBalancerInfoProto status;

    StatusResult(DatanodeDiskBalancerInfoProto status) {
      this.status = status;
    }

    @JsonProperty
    public String getDatanode() {
      return status.getNode().getHostName();
    }

    @JsonProperty
    public String getStatus() {
      return status.getRunningStatus().name();
    }

    @JsonProperty
    public double getThreshold() {
      return status.getDiskBalancerConf().getThreshold();
    }

    @JsonProperty
    public long getBandwidthInMB() {
      return status.getDiskBalancerConf().getDiskBandwidthInMB();
    }

    @JsonProperty
    public int getThreads() {
      return status.getDiskBalancerConf().getParallelThread();
    }

    @JsonProperty
    public long getSuccessMove() {
      return status.getSuccessMoveCount();
    }

    @JsonProperty
    public long getFailureMove() {
      return status.getFailureMoveCount();
    }

    @JsonProperty
    public long getBytesMovedMB() {
      return (long) Math.ceil(status.getBytesMoved() / (1024.0 * 1024.0));
    }

    @JsonProperty
    public long getEstBytesToMoveMB() {
      return (long) Math.ceil(status.getBytesToMove() / (1024.0 * 1024.0));
    }

    @JsonProperty
    public Long getEstTimeLeftMin() {
      long bytesToMove = status.getBytesToMove();
      if (bytesToMove == 0) {
        return 0L;
      }
      long bandwidth = status.getDiskBalancerConf().getDiskBandwidthInMB();
      double estimatedDataPendingMB = bytesToMove / (1024.0 * 1024.0);
      double estimatedTimeLeft = (bandwidth > 0) ? (estimatedDataPendingMB / bandwidth) / 60 : -1;
      return estimatedTimeLeft >= 0 ? (long) Math.ceil(estimatedTimeLeft) : null;
    }
  }
}
