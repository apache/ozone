/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.cli.datanode;

import com.google.common.base.Strings;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.util.StringUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.List;

/**
 * Command to list the usage info of a datanode.
 */
@Command(
    name = "usageinfo",
    description = "List usage information " +
        "(such as Capacity, SCMUsed, Remaining) of a datanode by IP address " +
        "or UUID",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class UsageInfoSubcommand extends ScmSubcommand {

  @CommandLine.ArgGroup(multiplicity = "1")
  private ExclusiveArguments exclusiveArguments;

  private static class ExclusiveArguments {
    @CommandLine.Option(names = {"--ip"}, paramLabel = "IP", description =
        "Show info by datanode ip address.", defaultValue = "")
    private String ipaddress;

    @CommandLine.Option(names = {"--uuid"}, paramLabel = "UUID", description =
        "Show info by datanode UUID.", defaultValue = "")
    private String uuid;

    @CommandLine.Option(names = {"-m", "--most-used"},
        description = "Show the most used datanodes.",
        defaultValue = "false")
    private boolean mostUsed;

    @CommandLine.Option(names = {"-l", "--least-used"},
        description = "Show the least used datanodes.",
        defaultValue = "false")
    private boolean leastUsed;
  }

  @CommandLine.Option(names = {"-c", "--count"}, description = "Number of " +
      "datanodes to display (Default: ${DEFAULT-VALUE}).",
      paramLabel = "NUMBER OF NODES", defaultValue = "3")
  private int count;


  @Override
  public void execute(ScmClient scmClient) throws IOException {
    List<HddsProtos.DatanodeUsageInfoProto> infoList;
    if (count < 1) {
      throw new IOException("Count must be an integer greater than 0.");
    }

    // fetch info by ip or uuid
    if (!Strings.isNullOrEmpty(exclusiveArguments.ipaddress) ||
        !Strings.isNullOrEmpty(exclusiveArguments.uuid)) {
      infoList = scmClient.getDatanodeUsageInfo(exclusiveArguments.ipaddress,
          exclusiveArguments.uuid);
    } else { // get info of most used or least used nodes
      infoList = scmClient.getDatanodeUsageInfo(exclusiveArguments.mostUsed,
          count);
    }

    infoList.forEach(this::printInfo);
  }

  /**
   * Print datanode usage information.
   *
   * @param info Information such as Capacity, SCMUsed etc.
   */
  public void printInfo(HddsProtos.DatanodeUsageInfoProto info) {
    long capacity = info.getCapacity();
    long used = info.getUsed(), remaining = info.getRemaining();
    long totalUsed = capacity - remaining;
    double usedRatio = used / (double) capacity;
    double remainingRatio = remaining / (double) capacity;
    NumberFormat percentFormat = NumberFormat.getPercentInstance();
    percentFormat.setMinimumFractionDigits(2);
    percentFormat.setMaximumFractionDigits(2);

    System.out.printf("Usage info for datanode with UUID %s:%n",
        info.getNode().getUuid());
    // print capacity in a readable format
    System.out.printf("%-13s: %-21s (%s) %n", "Capacity", capacity + " B",
        StringUtils.byteDesc(capacity));

    // print total used space and its percentage in a readable format
    System.out.printf("%-13s: %-21s (%s) %n", "Total Used", totalUsed + " B",
        StringUtils.byteDesc(totalUsed));
    System.out.printf("%-13s: %s %n", "Total Used %",
        percentFormat.format(1 - remainingRatio));

    // print space used by ozone and its percentage in a readable format
    System.out.printf("%-13s: %-21s (%s) %n", "Ozone Used", used + " B",
        StringUtils.byteDesc(used));
    System.out.printf("%-13s: %s %n", "Ozone Used %",
        percentFormat.format(usedRatio));

    // print total remaining space and its percentage in a readable format
    System.out.printf("%-13s: %-21s (%s) %n", "Remaining", remaining + " B",
        StringUtils.byteDesc(remaining));
    System.out.printf("%-13s: %s %n%n", "Remaining %",
        percentFormat.format(remainingRatio));
  }
}
