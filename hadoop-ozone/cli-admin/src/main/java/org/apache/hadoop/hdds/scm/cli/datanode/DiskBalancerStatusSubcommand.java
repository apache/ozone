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
import java.util.List;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Handler to get disk balancer status.
 */
@Command(
    name = "status",
    description = "Get Datanode DiskBalancer Status for inServiceHealthy DNs",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DiskBalancerStatusSubcommand extends ScmSubcommand {

  @Option(names = {"-s", "--state"},
      description = "Display only datanodes with the given status: RUNNING, STOPPED, UNKNOWN.")
  private HddsProtos.DiskBalancerRunningStatus state = null;

  @CommandLine.Option(names = {"-d", "--datanodes"},
      description = "Get diskBalancer status on specific datanodes.")
  private List<String> hosts = new ArrayList<>();

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    List<HddsProtos.DatanodeDiskBalancerInfoProto> resultProto =
        scmClient.getDiskBalancerStatus(
            hosts.isEmpty() ? null : hosts,
            state);

    System.out.println(generateStatus(resultProto));
  }

  private String generateStatus(
      List<HddsProtos.DatanodeDiskBalancerInfoProto> protos) {
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

    for (HddsProtos.DatanodeDiskBalancerInfoProto proto: protos) {
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
}
