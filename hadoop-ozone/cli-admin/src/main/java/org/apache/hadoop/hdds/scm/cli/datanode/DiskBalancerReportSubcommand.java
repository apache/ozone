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

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.DiskBalancerProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDiskBalancerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.VolumeReportProto;
import org.apache.hadoop.util.StringUtils;
import picocli.CommandLine.Command;

/**
 * Handler to get disk balancer report.
 */
@Command(
    name = "report",
    description = "Get DiskBalancer volume density report and per volume info from dns.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DiskBalancerReportSubcommand extends AbstractDiskBalancerSubCommand {

  // Store reports temporarily for non-JSON mode consolidation
  private final Map<String, DatanodeDiskBalancerInfoProto> reports =
      new ConcurrentHashMap<>();

  @Override
  protected Object executeCommand(String hostName) throws IOException {
    DiskBalancerProtocol diskBalancerProxy = DiskBalancerSubCommandUtil
        .getSingleNodeDiskBalancerProxy(hostName);
    try {
      final DatanodeDiskBalancerInfoProto report = diskBalancerProxy.getDiskBalancerInfo();

      // Only create JSON result object if JSON mode is enabled
      if (getOptions().isJson()) {
        return toJson(report);
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
          String.join(", ", failedNodes.stream()
              .map(this::formatDatanodeDisplayName)
              .collect(toList())));
    }

    // Display consolidated report for successful nodes
    if (!successNodes.isEmpty() && !reports.isEmpty()) {
      List<DatanodeDiskBalancerInfoProto> reportList = new ArrayList<>(reports.values());
      System.out.println(generateReport(reportList));
    }
  }

  private String generateReport(List<DatanodeDiskBalancerInfoProto> protos) {
    protos.sort((a, b) ->
        Double.compare(b.getCurrentVolumeDensitySum(), a.getCurrentVolumeDensitySum()));

    StringBuilder formatBuilder = new StringBuilder("Report result:%n");
    List<String> contentList = new ArrayList<>();

    for (int i = 0; i < protos.size(); i++) {
      DatanodeDiskBalancerInfoProto p = protos.get(i);
      String dn = DiskBalancerSubCommandUtil.getDatanodeHostAndIp(p.getNode());

      StringBuilder header = new StringBuilder();
      header.append("Datanode: ").append(dn).append('\n');
      header.append("Aggregate VolumeDataDensity: ").
          append(p.getCurrentVolumeDensitySum()).append('\n');

      if (p.hasIdealUsage() && p.hasDiskBalancerConf()
          && p.getDiskBalancerConf().hasThreshold()) {
        double idealUsage = p.getIdealUsage();
        double threshold = p.getDiskBalancerConf().getThreshold();
        double lt = idealUsage - threshold / 100.0;
        double ut = idealUsage + threshold / 100.0;
        header.append("IdealUsage: ").append(String.format("%.8f", idealUsage));
        header.append(" | Threshold: ").append(threshold).append('%');
        header.append(" | ThresholdRange: (").append(String.format("%.8f", lt));
        header.append(", ").append(String.format("%.8f", ut)).append(')').append('\n').append('\n');
        header.append("Volume Details -:").append('\n');
      }
      formatBuilder.append("%s%n");
      contentList.add(header.toString());

      if (p.getVolumeInfoCount() > 0 && p.hasIdealUsage()) {
        formatBuilder.append("%-45s %-40s %15s %15s %30s %20s %15s %15s%n");
        contentList.add("StorageID");
        contentList.add("StoragePath");
        contentList.add("TotalCapacity");
        contentList.add("UsedSpace");
        contentList.add("Container Pre-AllocatedSpace");
        contentList.add("EffectiveUsedSpace");
        contentList.add("Utilization");
        contentList.add("VolumeDensity");

        double ideal = p.getIdealUsage();
        for (VolumeReportProto v : p.getVolumeInfoList()) {
          formatBuilder.append("%-45s %-40s %15s %15s %30s %20s %15s %15s%n");
          contentList.add(v.getStorageId() != null ? v.getStorageId() : "-");
          contentList.add(v.hasStoragePath() ? v.getStoragePath() : "-");
          contentList.add(v.hasTotalCapacity() ? StringUtils.byteDesc(v.getTotalCapacity()) : "-");
          contentList.add(v.hasUsedSpace() ? StringUtils.byteDesc(v.getUsedSpace()) : "-");
          contentList.add(StringUtils.byteDesc(v.getCommittedBytes()));
          contentList.add(v.hasEffectiveUsedSpace() ? StringUtils.byteDesc(v.getEffectiveUsedSpace()) : "-");
          contentList.add(String.format("%.8f", v.getUtilization()));
          contentList.add(String.format("%.8f", Math.abs(v.getUtilization() - ideal)));
        }
        formatBuilder.append("%n");
      }

      if (i < protos.size() - 1) {
        formatBuilder.append("-------%n%n");
      }
    }

    formatBuilder.append("%nNote:%n");
    formatBuilder.append("  - Aggregate VolumeDataDensity: Sum of per-volume density" +
        " (deviation from ideal); higher means more imbalance.%n");
    formatBuilder.append("  - IdealUsage: Target utilization ratio (0-1) when volumes" +
        " are evenly balanced.%n");
    formatBuilder.append("  - ThresholdRange: Acceptable deviation (percent); volumes within" +
        " IdealUsage +/- Threshold are considered balanced.%n");
    formatBuilder.append("  - VolumeDensity: Deviation of a particular volume's utilization from IdealUsage.%n");
    formatBuilder.append("  - Utilization: Ratio of actual used space to capacity (0-1) for a particular volume.%n");
    formatBuilder.append("  - TotalCapacity: Total volume capacity.%n");
    formatBuilder.append("  - UsedSpace: Ozone used space.%n");
    formatBuilder.append("  - Container Pre-AllocatedSpace: Space reserved for containers not yet written to disk.%n");
    formatBuilder.append("  - EffectiveUsedSpace: This is the actual used space of volume which is visible" +
        " to the diskBalancer : (ozoneCapacity minus ozoneAvailable) + containerPreAllocatedSpace + " +
        "move delta for source volume.%n");

    return String.format(formatBuilder.toString(), contentList.toArray(new String[0]));
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
  private Map<String, Object> toJson(DatanodeDiskBalancerInfoProto report) {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("datanode", DiskBalancerSubCommandUtil.getDatanodeHostAndIp(report.getNode()));
    result.put("action", "report");
    result.put("status", "success");
    result.put("volumeDensity", report.getCurrentVolumeDensitySum());

    if (report.hasIdealUsage() && report.hasDiskBalancerConf()
        && report.getDiskBalancerConf().hasThreshold()) {
      double idealUsage = report.getIdealUsage();
      double threshold = report.getDiskBalancerConf().getThreshold();
      double lt = idealUsage - threshold / 100.0;
      double ut = idealUsage + threshold / 100.0;
      result.put("idealUsage", String.format("%.8f", idealUsage));
      result.put("threshold %", report.getDiskBalancerConf().getThreshold());
      result.put("thresholdRange", String.format("(%.08f, %.08f)", lt, ut));
    }

    if (report.getVolumeInfoCount() > 0) {
      double ideal = report.hasIdealUsage() ? report.getIdealUsage() : 0.0;
      List<Map<String, Object>> vols = new ArrayList<>();
      for (VolumeReportProto v : report.getVolumeInfoList()) {
        Map<String, Object> vm = new LinkedHashMap<>();
        vm.put("storageId", v.getStorageId());
        vm.put("storagePath", v.hasStoragePath() ? v.getStoragePath() : "-");
        vm.put("totalCapacity", v.hasTotalCapacity() ? StringUtils.byteDesc(v.getTotalCapacity()) : "-");
        vm.put("usedSpace", v.hasUsedSpace() ? StringUtils.byteDesc(v.getUsedSpace()) : "-");
        vm.put("containerPreAllocatedSpace", StringUtils.byteDesc(v.getCommittedBytes()));
        vm.put("effectiveUsedSpace", v.hasEffectiveUsedSpace() ?
            StringUtils.byteDesc(v.getEffectiveUsedSpace()) : "-");
        vm.put("utilization", v.getUtilization());
        vm.put("volumeDensity", Math.abs(v.getUtilization() - ideal));
        vols.add(vm);
      }

      result.put("volumes", vols);
    }
    return result;
  }
}
