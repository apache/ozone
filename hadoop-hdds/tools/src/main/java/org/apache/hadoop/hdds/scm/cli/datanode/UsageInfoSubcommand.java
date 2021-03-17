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
    double capacity = (double) info.getCapacity();
    double usedRatio = info.getUsed() / capacity;
    double remainingRatio = info.getRemaining() / capacity;
    NumberFormat percentFormat = NumberFormat.getPercentInstance();
    percentFormat.setMinimumFractionDigits(5);

    System.out.printf("Usage info for datanode with UUID %s:%n",
        info.getNode().getUuid());
    System.out.printf("%-10s: %20sB %n", "Capacity", info.getCapacity());
    System.out.printf("%-10s: %20sB (%s) %n", "SCMUsed", info.getUsed(),
        percentFormat.format(usedRatio));
    System.out.printf("%-10s: %20sB (%s) %n%n",
        "Remaining", info.getRemaining(),
        percentFormat.format(remainingRatio));
  }
}
