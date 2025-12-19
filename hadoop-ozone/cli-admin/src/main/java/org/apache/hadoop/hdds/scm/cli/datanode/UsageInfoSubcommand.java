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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Strings;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.util.StringUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Command to list the usage info of a datanode.
 */
@Command(
    name = "usageinfo",
    description = "List usage information " +
        "(such as Capacity, SCMUsed, Remaining) of a datanode by IP address " +
        "or Host name or UUID",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class UsageInfoSubcommand extends ScmSubcommand {

  private static final NumberFormat PERCENT_FORMAT
      = NumberFormat.getPercentInstance();

  static {
    PERCENT_FORMAT.setMinimumFractionDigits(2);
    PERCENT_FORMAT.setMaximumFractionDigits(2);
  }

  @CommandLine.ArgGroup(multiplicity = "1")
  private NodeSelectionArguments exclusiveArguments;

  @CommandLine.Option(names = {"-c", "--count"}, description = "Number of " +
      "datanodes to display (Default: ${DEFAULT-VALUE}).",
      paramLabel = "NUMBER OF NODES", defaultValue = "3")
  private int count;

  @CommandLine.Option(names = { "--json" },
      defaultValue = "false",
      description = "Format output as JSON")
  private boolean json;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    String hostnameOrIp =
        !Strings.isNullOrEmpty(exclusiveArguments.getIp()) ? exclusiveArguments.getIp()
            : !Strings.isNullOrEmpty(exclusiveArguments.getHostname()) ? exclusiveArguments.getHostname()
            : exclusiveArguments.address; //Fallback to deprecated --address for backward compatibility with older CLI.
    
    List<HddsProtos.DatanodeUsageInfoProto> infoList;
    if (count < 1) {
      throw new IOException("Count must be an integer greater than 0.");
    }

    // fetch info by ip or hostname or uuid
    if (!Strings.isNullOrEmpty(hostnameOrIp) ||
        !Strings.isNullOrEmpty(exclusiveArguments.getNodeId())) {
      infoList = scmClient.getDatanodeUsageInfo(hostnameOrIp,
          exclusiveArguments.getNodeId());
    } else { // get info of most used or least used nodes
      infoList = scmClient.getDatanodeUsageInfo(exclusiveArguments.mostUsed,
          count);
    }

    List<DatanodeUsage> usageList = infoList.stream()
        .map(d -> new DatanodeUsage(d))
        .collect(Collectors.toList());

    if (json) {
      System.out.println(
          JsonUtils.toJsonStringWithDefaultPrettyPrinter(usageList));
      return;
    }
    System.out.printf("Usage Information (%d Datanodes)%n%n", usageList.size());
    usageList.forEach(this::printInfo);
  }

  /**
   * Print datanode usage information.
   *
   * @param info Information such as Capacity, SCMUsed etc.
   */
  private void printInfo(DatanodeUsage info) {
    System.out.printf("%-13s: %s %n", "UUID",
        info.getDatanodeDetails().getUuid());
    System.out.printf("%-13s: %s %n", "IP Address",
        info.getDatanodeDetails().getIpAddress());
    System.out.printf("%-13s: %s %n", "Hostname",
        info.getDatanodeDetails().getHostName());
    // print capacity in a readable format
    System.out.printf("%-13s: %s (%s) %n", "Capacity", info.getCapacity()
        + " B", StringUtils.byteDesc(info.getCapacity()));

    // print total used space and its percentage in a readable format
    System.out.printf("%-13s: %s (%s) %n", "Total Used", info.getTotalUsed()
        + " B", StringUtils.byteDesc(info.getTotalUsed()));
    System.out.printf("%-13s: %s %n", "Total Used %",
        PERCENT_FORMAT.format(info.getTotalUsedRatio()));

    // print space used by ozone and its percentage in a readable format
    System.out.printf("%-13s: %s (%s) %n", "Ozone Used", info.getOzoneUsed()
        + " B", StringUtils.byteDesc(info.getOzoneUsed()));
    System.out.printf("%-13s: %s %n", "Ozone Used %",
        PERCENT_FORMAT.format(info.getUsedRatio()));

    // print total remaining space and its percentage in a readable format
    System.out.printf("%-13s: %s (%s) %n", "Remaining", info.getRemaining()
        + " B", StringUtils.byteDesc(info.getRemaining()));
    System.out.printf("%-13s: %s %n", "Remaining %",
        PERCENT_FORMAT.format(info.getRemainingRatio()));
    System.out.printf("%-13s: %d %n", "Pipeline(s)",
            info.getPipelineCount());
    System.out.printf("%-13s: %d %n", "Container(s)",
            info.getContainerCount());
    System.out.printf("%-24s: %s (%s) %n", "Container Pre-allocated",
        info.getCommitted() + " B", StringUtils.byteDesc(info.getCommitted()));
    System.out.printf("%-24s: %s (%s) %n", "Remaining Allocatable",
        (info.getRemaining() - info.getCommitted()) + " B",
        StringUtils.byteDesc((info.getRemaining() - info.getCommitted())));
    System.out.printf("%-24s: %s (%s) %n", "Free Space To Spare",
        info.getFreeSpaceToSpare() + " B",
        StringUtils.byteDesc(info.getFreeSpaceToSpare()));
    System.out.printf("%-13s: %s (%s) %n", "Reserved",
        info.getReserved() + " B", 
        StringUtils.byteDesc(info.getReserved()));
    System.out.println();
  }

  /**
   * Used by Jackson to serialize double values to 2 decimal places.
   */
  private static class DecimalJsonSerializer extends JsonSerializer<Double> {
    @Override
    public void serialize(Double value, JsonGenerator jgen,
        SerializerProvider provider)
        throws IOException {
      jgen.writeNumber(String.format("%.2f", value));
    }
  }

  /**
   * Internal class to de-serialized the Proto format into a class so we can
   * output it as JSON.
   */
  private static class DatanodeUsage {

    private DatanodeDetails datanodeDetails = null;
    private long capacity = 0;
    private long used = 0;
    private long remaining = 0;
    private long committed = 0;
    private long freeSpaceToSpare = 0;
    private long containerCount = 0;
    private long pipelineCount = 0;
    private long reserved = 0;

    DatanodeUsage(HddsProtos.DatanodeUsageInfoProto proto) {
      if (proto.hasNode()) {
        datanodeDetails = DatanodeDetails.getFromProtoBuf(proto.getNode());
      }
      if (proto.hasCapacity()) {
        capacity = proto.getCapacity();
      }
      if (proto.hasUsed()) {
        used = proto.getUsed();
      }
      if (proto.hasRemaining()) {
        remaining = proto.getRemaining();
      }
      if (proto.hasCommitted()) {
        committed = proto.getCommitted();
      }
      if (proto.hasContainerCount()) {
        containerCount = proto.getContainerCount();
      }
      if (proto.hasPipelineCount()) {
        pipelineCount = proto.getPipelineCount();
      }
      if (proto.hasFreeSpaceToSpare()) {
        freeSpaceToSpare = proto.getFreeSpaceToSpare();
      }
      if (proto.hasReserved()) { 
        reserved = proto.getReserved();
      }
    }

    public DatanodeDetails getDatanodeDetails() {
      return datanodeDetails;
    }

    public long getCapacity() {
      return capacity;
    }

    public long getTotalUsed() {
      return capacity - remaining;
    }

    public long getOzoneUsed() {
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

    public long getContainerCount() {
      return containerCount;
    }

    @JsonSerialize(using = DecimalJsonSerializer.class)
    public double getTotalUsedPercent() {
      return getTotalUsedRatio() * 100;
    }

    @JsonSerialize(using = DecimalJsonSerializer.class)
    public double getOzoneUsedPercent() {
      return getUsedRatio() * 100;
    }

    @JsonSerialize(using = DecimalJsonSerializer.class)
    public double getRemainingPercent() {
      return getRemainingRatio() * 100;
    }

    @JsonIgnore
    public double getTotalUsedRatio() {
      return 1 - getRemainingRatio();
    }

    @JsonIgnore
    public double getUsedRatio() {
      return used / (double) capacity;
    }

    @JsonIgnore
    public double getRemainingRatio() {
      return remaining / (double) capacity;
    }

    public long getPipelineCount() {
      return pipelineCount;
    }

    public long getReserved() { 
      return reserved;
    }
  }

  private static class NodeSelectionArguments extends NodeSelectionMixin {
    @Deprecated
    @CommandLine.Option(names = {"--address"}, paramLabel = "ADDRESS",
        description = "Show info by datanode ip or hostname address.",
        defaultValue = "",
        hidden = true)
    private String address;

    @CommandLine.Option(names = {"-m", "--most-used"},
        description = "Show the most used datanodes.",
        defaultValue = "false")
    private boolean mostUsed;

    @CommandLine.Option(names = {"-l", "--least-used"},
        description = "Show the least used datanodes.",
        defaultValue = "false")
    private boolean leastUsed;
  }
}
