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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.DiskBalancerProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDiskBalancerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeDiskBalancerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto;
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
  protected void resetCommandState() {
    reports.clear();
  }

  @Override
  protected Object executeCommand(String hostName) throws IOException {
    DiskBalancerProtocol diskBalancerProxy = DiskBalancerSubCommandUtil
        .getSingleNodeDiskBalancerProxy(hostName);
    try {
      final DatanodeDiskBalancerInfoProto report = diskBalancerProxy.getDiskBalancerInfo();

      // Only create JSON result object if JSON mode is enabled
      if (getOptions().isJson()) {
        return toJson(hostName, report);
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
      System.out.println(generateReport(successNodes, reportList));
    }
  }

  private String generateReport(
      List<String> successNodes, List<DatanodeDiskBalancerInfoProto> protos) {
    List<Map.Entry<String, DatanodeDiskBalancerInfoProto>> entries = new ArrayList<>();
    for (int i = 0; i < protos.size(); i++) {
      entries.add(new AbstractMap.SimpleEntry<>(successNodes.get(i), protos.get(i)));
    }
    entries.sort((a, b) -> Double.compare(
        b.getValue().getCurrentVolumeDensitySum(),
        a.getValue().getCurrentVolumeDensitySum()));

    StringBuilder formatBuilder = new StringBuilder("Report result:%n");
    List<String> contentList = new ArrayList<>();

    for (int i = 0; i < entries.size(); i++) {
      Map.Entry<String, DatanodeDiskBalancerInfoProto> entry = entries.get(i);
      DatanodeDiskBalancerInfoProto p = entry.getValue();
      String dn = formatDatanodeDisplayName(entry.getKey(), p.getNode());

      StringBuilder header = new StringBuilder();
      header.append("Datanode: ").append(dn).append(System.lineSeparator())
          .append("Aggregate VolumeDataDensity: ")
          .append(formatPercent(p.getCurrentVolumeDensitySum()))
          .append(System.lineSeparator());

      if (p.getStorageTypeInfoCount() > 0 && p.hasDiskBalancerConf()
          && p.getDiskBalancerConf().hasThreshold()) {
        appendStorageTypeDetails(header, p);
      } else if (p.hasIdealUsage() && p.hasDiskBalancerConf()
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

      if (p.getVolumeInfoCount() > 0 && (p.hasIdealUsage() || p.getStorageTypeInfoCount() > 0)) {
        formatBuilder.append("%-12s %-45s %-40s %15s %15s %15s %30s %20s %15s %15s%n");
        contentList.add("StorageType");
        contentList.add("StorageID");
        contentList.add("StoragePath");
        contentList.add("OzoneCapacity");
        contentList.add("OzoneAvailable");
        contentList.add("OzoneUsed");
        contentList.add("ContainerPreAllocatedSpace");
        contentList.add("EffectiveUsedSpace");
        contentList.add("Utilization");
        contentList.add("VolumeDensity");

        Map<StorageTypeProto, StorageTypeDiskBalancerInfoProto> storageTypeInfo =
            getStorageTypeInfo(p);
        for (VolumeReportProto v : p.getVolumeInfoList()) {
          formatBuilder.append("%-12s %-45s %-40s %15s %15s %15s %30s %20s %15s %15s%n");
          contentList.add(v.hasStorageType() ? v.getStorageType().name() : "-");
          contentList.add(v.hasStorageId() ? v.getStorageId() : "-");
          contentList.add(v.hasStoragePath() ? v.getStoragePath() : "-");
          contentList.add(v.hasTotalCapacity() ? StringUtils.byteDesc(v.getTotalCapacity()) : "-");
          contentList.add(v.hasOzoneAvailable() ? StringUtils.byteDesc(v.getOzoneAvailable()) : "-");
          contentList.add(v.hasUsedSpace() ? StringUtils.byteDesc(v.getUsedSpace()) : "-");
          contentList.add(StringUtils.byteDesc(v.getCommittedBytes()));
          contentList.add(v.hasEffectiveUsedSpace() ? StringUtils.byteDesc(v.getEffectiveUsedSpace()) : "-");
          contentList.add(formatPercent(v.getUtilization()));
          contentList.add(formatVolumeDensity(p, v, storageTypeInfo));
        }
        formatBuilder.append("%n");
      }

      if (i < entries.size() - 1) {
        formatBuilder.append("-------%n%n");
      }
    }

    formatBuilder.append("%nNote:%n")
        .append("  - Aggregate VolumeDataDensity: Sum of per-volume density from each storage type's ideal;")
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

    return String.format(formatBuilder.toString(), contentList.toArray(new Object[0]));
  }

  private static void appendStorageTypeDetails(StringBuilder header,
      DatanodeDiskBalancerInfoProto report) {
    double threshold = report.getDiskBalancerConf().getThreshold();
    header.append("Storage Type Details:").append(System.lineSeparator());
    for (StorageTypeDiskBalancerInfoProto info : report.getStorageTypeInfoList()) {
      header.append("  ").append(info.getStorageType()).append(": ");
      if (!info.getBalanceable() || !info.hasIdealUsage()) {
        header.append("not balanceable (").append(info.getUsableVolumeCount())
            .append(" usable volume(s))").append(System.lineSeparator());
        continue;
      }
      double idealUsage = info.getIdealUsage();
      double lowerThreshold = Math.max(0.0, idealUsage - threshold / 100.0);
      double upperThreshold = Math.min(1.0, idealUsage + threshold / 100.0);
      header.append("IdealUsage: ").append(formatPercent(idealUsage))
          .append(" | ThresholdRange: (").append(formatPercent(lowerThreshold))
          .append(", ").append(formatPercent(upperThreshold)).append(')')
          .append(" | VolumeDataDensity: ")
          .append(formatPercent(info.getCurrentVolumeDensitySum()))
          .append(" | EstBytesToMove: ").append(StringUtils.byteDesc(info.getBytesToMove()))
          .append(System.lineSeparator());
    }
    header.append(System.lineSeparator()).append("Volume Details:").append(System.lineSeparator());
  }

  private static Map<StorageTypeProto, StorageTypeDiskBalancerInfoProto> getStorageTypeInfo(
      DatanodeDiskBalancerInfoProto report) {
    Map<StorageTypeProto, StorageTypeDiskBalancerInfoProto> result = new LinkedHashMap<>();
    for (StorageTypeDiskBalancerInfoProto info : report.getStorageTypeInfoList()) {
      result.put(info.getStorageType(), info);
    }
    return result;
  }

  private static String formatVolumeDensity(DatanodeDiskBalancerInfoProto report,
      VolumeReportProto volume,
      Map<StorageTypeProto, StorageTypeDiskBalancerInfoProto> storageTypeInfo) {
    if (volume.hasStorageType()) {
      StorageTypeDiskBalancerInfoProto info = storageTypeInfo.get(volume.getStorageType());
      if (info != null && info.hasIdealUsage()) {
        return formatPercent(Math.abs(volume.getUtilization() - info.getIdealUsage()));
      }
    }
    return report.hasIdealUsage()
        ? formatPercent(Math.abs(volume.getUtilization() - report.getIdealUsage())) : "-";
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
  private Map<String, Object> toJson(String hostName, DatanodeDiskBalancerInfoProto report) {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("datanode", formatDatanodeDisplayName(hostName, report.getNode()));
    result.put("action", "report");
    result.put("status", "success");
    result.put("volumeDensity", formatPercent(report.getCurrentVolumeDensitySum()));

    Map<StorageTypeProto, StorageTypeDiskBalancerInfoProto> storageTypeInfo =
        getStorageTypeInfo(report);
    // Report ideal usage per storage type when the datanode sends it. The node-level
    // idealUsage averages across storage types, which is not a target any move can reach
    // on a datanode with more than one type, so it is only reported as a fallback for
    // datanodes that predate the per-storage-type fields.
    if (!storageTypeInfo.isEmpty()) {
      double threshold = report.getDiskBalancerConf().getThreshold();
      List<Map<String, Object>> storageTypes = new ArrayList<>();
      for (StorageTypeDiskBalancerInfoProto info : report.getStorageTypeInfoList()) {
        Map<String, Object> storageType = new LinkedHashMap<>();
        storageType.put("storageType", info.getStorageType().name());
        storageType.put("balanceable", info.getBalanceable());
        storageType.put("usableVolumeCount", info.getUsableVolumeCount());
        storageType.put("volumeDensity", formatPercent(info.getCurrentVolumeDensitySum()));
        storageType.put("estBytesToMove", StringUtils.byteDesc(info.getBytesToMove()));
        if (info.hasIdealUsage()) {
          double idealUsage = info.getIdealUsage();
          double lowerThreshold = Math.max(0.0, idealUsage - threshold / 100.0);
          double upperThreshold = Math.min(1.0, idealUsage + threshold / 100.0);
          storageType.put("idealUsage", formatPercent(idealUsage));
          storageType.put("thresholdRange", String.format("(%s, %s)",
              formatPercent(lowerThreshold), formatPercent(upperThreshold)));
        }
        storageTypes.add(storageType);
      }
      result.put("storageTypes", storageTypes);
      result.put("threshold %", String.format(Locale.ROOT, PERCENT_FORMAT, threshold));
    } else if (report.hasIdealUsage() && report.hasDiskBalancerConf()
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
      List<Map<String, Object>> vols = new ArrayList<>();
      for (VolumeReportProto v : report.getVolumeInfoList()) {
        Map<String, Object> vm = new LinkedHashMap<>();
        vm.put("storageType", v.hasStorageType() ? v.getStorageType().name() : "-");
        vm.put("storageId", v.getStorageId());
        vm.put("storagePath", v.hasStoragePath() ? v.getStoragePath() : "-");
        vm.put("ozoneCapacity", v.hasTotalCapacity() ? StringUtils.byteDesc(v.getTotalCapacity()) : "-");
        vm.put("ozoneAvailable", v.hasOzoneAvailable() ? StringUtils.byteDesc(v.getOzoneAvailable()) : "-");
        vm.put("ozoneUsed", v.hasUsedSpace() ? StringUtils.byteDesc(v.getUsedSpace()) : "-");
        vm.put("containerPreAllocatedSpace", StringUtils.byteDesc(v.getCommittedBytes()));
        vm.put("effectiveUsedSpace", v.hasEffectiveUsedSpace() ?
            StringUtils.byteDesc(v.getEffectiveUsedSpace()) : "-");
        vm.put("utilization", formatPercent(v.getUtilization()));
        vm.put("volumeDensity", formatVolumeDensity(report, v, storageTypeInfo));
        vols.add(vm);
      }

      result.put("volumes", vols);
    }
    return result;
  }
}
