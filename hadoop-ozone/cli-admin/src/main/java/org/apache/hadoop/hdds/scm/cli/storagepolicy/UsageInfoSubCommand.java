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

package org.apache.hadoop.hdds.scm.cli.storagepolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.StorageTypeUtils;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DatanodeStorageTypeUsageInfoProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ListStorageTypeUsageInfoRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StorageTypeUsageInfoProto;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.util.StringUtils;
import picocli.CommandLine;

/**
 * Handler of list storage usage info by StorageType command.
 */
@CommandLine.Command(
    name = "usageinfo",
    description = "List usage information in the StorageType dimension",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class UsageInfoSubCommand extends ScmSubcommand {

  @CommandLine.Option(
      names = {"-d", "--with-datanode"},
      description = "Print detailed per-datanode storage-type usage",
      defaultValue = "false")
  private boolean printDatanodeInfo;

  @CommandLine.Option(
      names = {"-o", "--operational-state"},
      description = "Show datanodes in a specific operational state "
          + "(IN_SERVICE, DECOMMISSIONING, DECOMMISSIONED, "
          + "ENTERING_MAINTENANCE, IN_MAINTENANCE), "
          + "or ALL to show all operational states",
      defaultValue = "IN_SERVICE")
  private String nodeOperationalStateStr;

  @CommandLine.Option(
      names = {"-n", "--node-state"},
      description = "Show datanodes in a specific health state "
          + "(HEALTHY, STALE, DEAD), or ALL to show all health states",
      defaultValue = "HEALTHY")
  private String nodeStateStr;

  @CommandLine.Option(
      names = {"--json"},
      defaultValue = "false",
      description = "Format output as JSON")
  private boolean json;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    NodeOperationalState nodeOpState = null;
    NodeState nodeState = null;
    if (!nodeOperationalStateStr.equalsIgnoreCase("ALL")) {
      nodeOpState = NodeOperationalState.valueOf(nodeOperationalStateStr.toUpperCase());
    }
    if (!nodeStateStr.equalsIgnoreCase("ALL")) {
      nodeState = NodeState.valueOf(nodeStateStr.toUpperCase());
    }

    ListStorageTypeUsageInfoRequestProto.Builder reqBuilder = ListStorageTypeUsageInfoRequestProto.newBuilder();
    if (nodeOpState != null) {
      reqBuilder.setOpState(nodeOpState);
    }
    if (nodeState != null) {
      reqBuilder.setState(nodeState);
    }

    List<DatanodeStorageTypeUsageInfoProto> usageInfos = scmClient.listStorageTypeUsageInfo(reqBuilder.build());

    List<ClusterStorageTypeUsage> summary = aggregateClusterUsage(usageInfos);
    List<DatanodeStorageTypeUsage> datanodeUsage = printDatanodeInfo ? buildDatanodeUsageList(usageInfos) : null;

    if (json) {
      UsageInfoOutput output = new UsageInfoOutput(summary, datanodeUsage);
      System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(output));
      return;
    }

    printStorageTypeSummaryInfo(summary);
    if (datanodeUsage != null) {
      printStorageTypeDatanodeInfo(datanodeUsage);
    }
  }

  /**
   * Sums capacity/used/remaining/committed/freeSpaceToSpare across all returned datanodes, per StorageType.
   * Uses Math.addExact since this sums values across multiple datanodes and must not silently overflow.
   */
  private List<ClusterStorageTypeUsage> aggregateClusterUsage(List<DatanodeStorageTypeUsageInfoProto> dnInfos) {
    Map<StorageType, ClusterStorageTypeUsage> totals = new EnumMap<>(StorageType.class);
    for (DatanodeStorageTypeUsageInfoProto dn : dnInfos) {
      for (StorageTypeUsageInfoProto st : dn.getStorageTypeUsageInfoList()) {
        StorageType storageType = StorageTypeUtils.getFromProtobuf(st.getStorageType());
        ClusterStorageTypeUsage usage = totals.computeIfAbsent(storageType, ClusterStorageTypeUsage::new);
        usage.datanodeCount++;
        usage.capacity = Math.addExact(usage.capacity, st.getCapacity());
        usage.used = Math.addExact(usage.used, st.getUsed());
        usage.remaining = Math.addExact(usage.remaining, st.getRemaining());
        usage.committed = Math.addExact(usage.committed, st.getCommitted());
        usage.freeSpaceToSpare = Math.addExact(usage.freeSpaceToSpare, st.getFreeSpaceToSpare());
      }
    }
    List<ClusterStorageTypeUsage> summary = new ArrayList<>(totals.values());
    summary.sort((a, b) -> a.getStorageType().compareTo(b.getStorageType()));
    return summary;
  }

  private List<DatanodeStorageTypeUsage> buildDatanodeUsageList(List<DatanodeStorageTypeUsageInfoProto> dnInfos) {
    List<DatanodeStorageTypeUsage> datanodeUsage = new ArrayList<>();
    for (DatanodeStorageTypeUsageInfoProto dn : dnInfos) {
      for (StorageTypeUsageInfoProto st : dn.getStorageTypeUsageInfoList()) {
        datanodeUsage.add(new DatanodeStorageTypeUsage(dn, st));
      }
    }
    return datanodeUsage;
  }

  private void printStorageTypeSummaryInfo(List<ClusterStorageTypeUsage> summary) {
    System.out.println("Cluster StorageType Usage Summary");
    for (ClusterStorageTypeUsage usage : summary) {
      StorageType storageType = usage.getStorageType();
      System.out.printf("  %-23s: %s %n", storageType + " Datanode Count", usage.getDatanodeCount());
      System.out.printf("  %-23s: %s (%s) %n", storageType + " Capacity",
          usage.getCapacity() + " B", StringUtils.byteDesc(usage.getCapacity()));
      System.out.printf("  %-23s: %s (%s) %n", storageType + " Ozone Used",
          usage.getUsed() + " B", StringUtils.byteDesc(usage.getUsed()));
      System.out.printf("  %-23s: %s (%s) %n", storageType + " Remaining",
          usage.getRemaining() + " B", StringUtils.byteDesc(usage.getRemaining()));
      System.out.printf("  %-23s: %s (%s) %n", storageType + " Committed",
          usage.getCommitted() + " B", StringUtils.byteDesc(usage.getCommitted()));
      System.out.printf("  %-23s: %s (%s) %n", storageType + " FreeSpaceToSpare",
          usage.getFreeSpaceToSpare() + " B", StringUtils.byteDesc(usage.getFreeSpaceToSpare()));
      System.out.println();
    }
  }

  private void printStorageTypeDatanodeInfo(List<DatanodeStorageTypeUsage> datanodeUsage) {
    System.out.println("Datanode StorageType Usage List");
    StorageType currentType = null;
    for (DatanodeStorageTypeUsage usage : datanodeUsage) {
      if (usage.getStorageType() != currentType) {
        currentType = usage.getStorageType();
        System.out.printf("%nStorageType %s: %n%n", currentType);
      }
      DatanodeDetails dd = usage.getDatanodeDetails();
      System.out.printf("  %-23s: %s (%s, %s, %s) %n", "Datanode", dd.getUuidString(),
          dd.getHostName(), dd.getIpAddress(), dd.getNetworkLocation());
      System.out.printf("  %-23s: %s (%s) %n", currentType + " Capacity",
          usage.getCapacity() + " B", StringUtils.byteDesc(usage.getCapacity()));
      System.out.printf("  %-23s: %s (%s) %n", currentType + " Ozone Used",
          usage.getUsed() + " B", StringUtils.byteDesc(usage.getUsed()));
      System.out.printf("  %-23s: %s (%s) %n", currentType + " Remaining",
          usage.getRemaining() + " B", StringUtils.byteDesc(usage.getRemaining()));
      System.out.printf("  %-23s: %s (%s) %n", currentType + " Committed",
          usage.getCommitted() + " B", StringUtils.byteDesc(usage.getCommitted()));
      System.out.printf("  %-23s: %s (%s) %n", currentType + " FreeSpaceToSpare",
          usage.getFreeSpaceToSpare() + " B", StringUtils.byteDesc(usage.getFreeSpaceToSpare()));
      System.out.println();
    }
  }

  /**
   * Internal class to de-serialize the Proto format into a class so we can output it as JSON.
   * Cluster-wide, aggregated-across-datanodes usage for a single StorageType.
   */
  private static final class ClusterStorageTypeUsage {
    private final StorageType storageType;
    private int datanodeCount;
    private long capacity;
    private long used;
    private long remaining;
    private long committed;
    private long freeSpaceToSpare;

    ClusterStorageTypeUsage(StorageType storageType) {
      this.storageType = storageType;
    }

    public StorageType getStorageType() {
      return storageType;
    }

    public int getDatanodeCount() {
      return datanodeCount;
    }

    public long getCapacity() {
      return capacity;
    }

    public long getUsed() {
      return used;
    }

    public long getRemaining() {
      return remaining;
    }

    public long getCommitted() {
      return committed;
    }

    public long getFreeSpaceToSpare() {
      return freeSpaceToSpare;
    }
  }

  /**
   * Internal class to de-serialize the Proto format into a class so we can output it as JSON.
   * Per-datanode, per-StorageType usage, only populated when {@code -wd} is set.
   */
  private static final class DatanodeStorageTypeUsage {
    private final DatanodeDetails datanodeDetails;
    private final StorageType storageType;
    private final long capacity;
    private final long used;
    private final long remaining;
    private final long committed;
    private final long freeSpaceToSpare;

    DatanodeStorageTypeUsage(DatanodeStorageTypeUsageInfoProto dnProto, StorageTypeUsageInfoProto proto) {
      this.datanodeDetails = DatanodeDetails.getFromProtoBuf(dnProto.getDatanodeDetails());
      this.storageType = StorageTypeUtils.getFromProtobuf(proto.getStorageType());
      this.capacity = proto.getCapacity();
      this.used = proto.getUsed();
      this.remaining = proto.getRemaining();
      this.committed = proto.getCommitted();
      this.freeSpaceToSpare = proto.getFreeSpaceToSpare();
    }

    public DatanodeDetails getDatanodeDetails() {
      return datanodeDetails;
    }

    public StorageType getStorageType() {
      return storageType;
    }

    public long getCapacity() {
      return capacity;
    }

    public long getUsed() {
      return used;
    }

    public long getRemaining() {
      return remaining;
    }

    public long getCommitted() {
      return committed;
    }

    public long getFreeSpaceToSpare() {
      return freeSpaceToSpare;
    }
  }

  /**
   * JSON output container: the cluster summary, plus the per-datanode breakdown when {@code -wd} is set.
   */
  private static final class UsageInfoOutput {
    private final List<ClusterStorageTypeUsage> summary;
    private final List<DatanodeStorageTypeUsage> datanodeUsage;

    UsageInfoOutput(List<ClusterStorageTypeUsage> summary, List<DatanodeStorageTypeUsage> datanodeUsage) {
      this.summary = summary;
      this.datanodeUsage = datanodeUsage;
    }

    public List<ClusterStorageTypeUsage> getSummary() {
      return summary;
    }

    public List<DatanodeStorageTypeUsage> getDatanodeUsage() {
      return datanodeUsage;
    }
  }
}
