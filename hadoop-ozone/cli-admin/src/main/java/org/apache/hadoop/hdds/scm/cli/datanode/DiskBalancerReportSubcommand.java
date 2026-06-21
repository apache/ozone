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
import java.util.Locale;
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
    description = "Get DiskBalancer volume density report and per volume info from datanodes",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DiskBalancerReportSubcommand extends AbstractDiskBalancerSubCommand {

  // Store reports temporarily for non-JSON mode consolidation
  private final Map<String, DatanodeDiskBalancerInfoProto> reports =
      new ConcurrentHashMap<>();

  private static final String PERCENT_FORMAT = "%.2f%%";

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
      List<DatanodeDiskBalancerInfoProto> reportList = successNodes.stream()
          .map(reports::get)
          .collect(toList());
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
      header.append("Datanode: ").append(dn).append(System.lineSeparator())
          .append("Aggregate VolumeDataDensity: ")
          .append(formatPercent(p.getCurrentVolumeDensitySum()))
          .append(System.lineSeparator());

      if (p.hasIdealUsage() && p.hasDiskBalancerConf()
          && p.getDiskBalancerConf().hasThreshold()) {
        double idealUsage = p.getIdealUsage();
        double threshold = p.getDiskBalancerConf().getThreshold();
        double lt = Math.max(0.0, idealUsage - threshold / 100.0);
        double ut = Math.min(1.0, idealUsage + threshold / 100.0);
        header.append("IdealUsage: ").append(formatPercent(idealUsage))
            .append(" | Threshold: ")
            .append(String.format(Locale.ROOT, PERCENT_FORMAT, threshold))
            .append(" | ThresholdRange: (").append(formatPercent(lt))
            .append(", ").append(formatPercent(ut)).append(')')
            .append(System.lineSeparator())
            .append(System.lineSeparator())
            .append("Volume Details:").append(System.lineSeparator());
      }
      formatBuilder.append("%s%n");
      contentList.add(header.toString());

      if (p.getVolumeInfoCount() > 0 && p.hasIdealUsage()) {
        formatBuilder.append("%-45s %-40s %15s %15s %15s %30s %20s %15s %15s%n");
        contentList.add("StorageID");
        contentList.add("StoragePath");
        contentList.add("OzoneCapacity");
        contentList.add("OzoneAvailable");
        contentList.add("OzoneUsed");
        contentList.add("ContainerPreAllocatedSpace");
        contentList.add("EffectiveUsedSpace");
        contentList.add("Utilization");
        contentList.add("VolumeDensity");

        double ideal = p.getIdealUsage();
        for (VolumeReportProto v : p.getVolumeInfoList()) {
          formatBuilder.append("%-45s %-40s %15s %15s %15s %30s %20s %15s %15s%n");
          contentList.add(v.hasStorageId() ? v.getStorageId() : "-");
          contentList.add(v.hasStoragePath() ? v.getStoragePath() : "-");
          contentList.add(v.hasTotalCapacity() ? StringUtils.byteDesc(v.getTotalCapacity()) : "-");
          contentList.add(v.hasOzoneAvailable() ? StringUtils.byteDesc(v.getOzoneAvailable()) : "-");
          contentList.add(v.hasUsedSpace() ? StringUtils.byteDesc(v.getUsedSpace()) : "-");
          contentList.add(StringUtils.byteDesc(v.getCommittedBytes()));
          contentList.add(v.hasEffectiveUsedSpace() ? StringUtils.byteDesc(v.getEffectiveUsedSpace()) : "-");
          contentList.add(formatPercent(v.getUtilization()));
          contentList.add(formatPercent(Math.abs(v.getUtilization() - ideal)));
        }
        formatBuilder.append("%n");
      }

      if (i < protos.size() - 1) {
        formatBuilder.append("-------%n%n");
      }
    }

    formatBuilder.append("%nNote:%n")
        .append("  - Aggregate VolumeDataDensity: Sum of per-volume density (deviation from ideal);")
        .append(" higher means more imbalance.%n")
        .append("  - IdealUsage: Target utilization (0-100%%) when volumes are evenly balanced.%n")
        .append("  - ThresholdRange: Acceptable deviation (percent); volumes within")
        .append(" IdealUsage +/- Threshold are considered balanced.%n")
        .append("  - VolumeDensity: Deviation of a particular volume's utilization from IdealUsage.%n")
        .append("  - Utilization: how much a particular volume is utilized ")
        .append("(effectiveUsedSpace / ozoneCapacity) in %%.%n")
        .append("  - OzoneCapacity: Ozone data volume capacity.%n")
        .append("  - OzoneAvailable: Ozone data volume available space.%n")
        .append("  - OzoneUsed: Ozone data volume used space.%n")
        .append("  - ContainerPreAllocatedSpace: Space reserved for containers not yet written to disk.%n")
        .append("  - EffectiveUsedSpace: This is the actual used space of volume which is visible")
        .append(" to the diskBalancer : (ozoneCapacity minus ozoneAvailable) + containerPreAllocatedSpace + ")
        .append("move delta.%n")
        .append("  - move delta: source volume space to be reclaimed after move completion;" +
            " this value is reflected only when diskBalancer is running else it is 0.%n");

    return String.format(formatBuilder.toString(), contentList.toArray(new String[0]));
  }

  @Override
  protected String getActionName() {
    return "report";
  }

  private static String formatPercent(double ratio) {
    return String.format(Locale.US, PERCENT_FORMAT, ratio * 100.0);
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
    result.put("volumeDensity", formatPercent(report.getCurrentVolumeDensitySum()));

    if (report.hasIdealUsage() && report.hasDiskBalancerConf()
        && report.getDiskBalancerConf().hasThreshold()) {
      double idealUsage = report.getIdealUsage();
      double threshold = report.getDiskBalancerConf().getThreshold();
      double lt = Math.max(0.0, idealUsage - threshold / 100.0);
      double ut = Math.min(1.0, idealUsage + threshold / 100.0);
      result.put("idealUsage", formatPercent(idealUsage));
      result.put("threshold %", String.format(Locale.ROOT, PERCENT_FORMAT, threshold));
      result.put("thresholdRange", String.format("(%s, %s)",
          formatPercent(lt), formatPercent(ut)));
    }

    if (report.getVolumeInfoCount() > 0) {
      double ideal = report.hasIdealUsage() ? report.getIdealUsage() : 0.0;
      List<Map<String, Object>> vols = new ArrayList<>();
      for (VolumeReportProto v : report.getVolumeInfoList()) {
        Map<String, Object> vm = new LinkedHashMap<>();
        vm.put("storageId", v.getStorageId());
        vm.put("storagePath", v.hasStoragePath() ? v.getStoragePath() : "-");
        vm.put("ozoneCapacity", v.hasTotalCapacity() ? StringUtils.byteDesc(v.getTotalCapacity()) : "-");
        vm.put("ozoneAvailable", v.hasOzoneAvailable() ? StringUtils.byteDesc(v.getOzoneAvailable()) : "-");
        vm.put("ozoneUsed", v.hasUsedSpace() ? StringUtils.byteDesc(v.getUsedSpace()) : "-");
        vm.put("containerPreAllocatedSpace", StringUtils.byteDesc(v.getCommittedBytes()));
        vm.put("effectiveUsedSpace", v.hasEffectiveUsedSpace() ?
            StringUtils.byteDesc(v.getEffectiveUsedSpace()) : "-");
        vm.put("utilization", formatPercent(v.getUtilization()));
        vm.put("volumeDensity", formatPercent(Math.abs(v.getUtilization() - ideal)));
        vols.add(vm);
      }

      result.put("volumes", vols);
    }
    return result;
  }
}
