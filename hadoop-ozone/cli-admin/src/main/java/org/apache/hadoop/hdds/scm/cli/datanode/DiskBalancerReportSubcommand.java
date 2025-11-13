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
import org.apache.hadoop.hdds.protocol.DiskBalancerProtocol;
import org.apache.hadoop.hdds.protocol.proto.DiskBalancerProtocolProtos.DatanodeDiskBalancerInfoType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDiskBalancerInfoProto;
import org.apache.hadoop.ozone.ClientVersion;
import picocli.CommandLine.Command;

/**
 * Handler to get disk balancer report.
 */
@Command(
    name = "report",
    description = "Get DiskBalancer volume density report",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DiskBalancerReportSubcommand extends AbstractDiskBalancerSubCommand {

  private final List<HddsProtos.DatanodeDiskBalancerInfoProto> reports = new ArrayList<>();

  @Override
  protected boolean executeCommand(String hostName) {
    try (DiskBalancerProtocol diskBalancerProxy = DiskBalancerSubCommandUtil
        .getSingleNodeDiskBalancerProxy(hostName)) {
      HddsProtos.DatanodeDiskBalancerInfoProto report = 
          diskBalancerProxy.getDiskBalancerInfo(
              DatanodeDiskBalancerInfoType.REPORT,
              ClientVersion.CURRENT_VERSION);
      reports.add(report);
      return true;
    } catch (IOException e) {
      System.err.printf("Error on node [%s]: %s%n", hostName, e.getMessage());
      return false;
    }
  }

  @Override
  protected void displayResults(List<String> successNodes, List<String> failedNodes) {
    if (!failedNodes.isEmpty()) {
      System.err.printf("Failed to get DiskBalancer report from nodes: [%s]%n", 
          String.join(", ", failedNodes));
    }

    // Display consolidated report for successful nodes
    if (!reports.isEmpty()) {
      System.out.println(generateReport(reports));
    }
  }

  private String generateReport(List<DatanodeDiskBalancerInfoProto> protos) {
    // Sort by volume density in descending order (highest imbalance first)
    List<DatanodeDiskBalancerInfoProto> sortedProtos = new ArrayList<>(protos);
    sortedProtos.sort((a, b) ->
        Double.compare(b.getCurrentVolumeDensitySum(), a.getCurrentVolumeDensitySum()));

    StringBuilder formatBuilder = new StringBuilder("Report result:%n" +
        "%-50s %s%n");

    List<String> contentList = new ArrayList<>();
    contentList.add("Datanode");
    contentList.add("VolumeDensity");

    for (DatanodeDiskBalancerInfoProto proto : sortedProtos) {
      formatBuilder.append("%-50s %s%n");
      contentList.add(proto.getNode().getHostName());
      contentList.add(String.valueOf(proto.getCurrentVolumeDensitySum()));
    }

    return String.format(formatBuilder.toString(),
        contentList.toArray(new String[0]));
  }
}
