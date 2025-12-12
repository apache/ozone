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
 * Handler to get disk balancer report.
 */
@Command(
    name = "report",
    description = "Get DiskBalancer volume density report",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DiskBalancerReportSubcommand extends AbstractDiskBalancerSubCommand {

  // Store reports temporarily for non-JSON mode consolidation
  private final Map<String, HddsProtos.DatanodeDiskBalancerInfoProto> reports = 
      new ConcurrentHashMap<>();

  @Override
  protected Object executeCommand(String hostName) throws IOException {
    DiskBalancerProtocol diskBalancerProxy = DiskBalancerSubCommandUtil
        .getSingleNodeDiskBalancerProxy(hostName);
    try {
      final DatanodeDiskBalancerInfoProto report = diskBalancerProxy.getDiskBalancerInfo();

      // Only create JSON result object if JSON mode is enabled
      if (getOptions().isJson()) {
        return createReportResult(report);
      }
      
      // For non-JSON mode, store the proto for later consolidation
      reports.put(hostName, report);
      return report; // Return non-null to indicate success
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
      System.err.printf("Failed to get DiskBalancer report from nodes: [%s]%n", 
          String.join(", ", failedNodes));
    }

    // Display consolidated report for successful nodes
    if (!successNodes.isEmpty() && !reports.isEmpty()) {
      List<HddsProtos.DatanodeDiskBalancerInfoProto> reportList = 
          new ArrayList<>(reports.values());
      System.out.println(generateReport(reportList));
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

  @Override
  protected String getActionName() {
    return "report";
  }

  /**
   * Create a JSON result map for a report.
   * 
   * @param report the DiskBalancer report proto
   * @return JSON result map
   */
  private Map<String, Object> createReportResult(
      HddsProtos.DatanodeDiskBalancerInfoProto report) {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("datanode", report.getNode().getHostName());
    result.put("action", "report");
    result.put("status", "success");
    result.put("volumeDensity", report.getCurrentVolumeDensitySum());
    return result;
  }
}
