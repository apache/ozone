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

  @CommandLine.Option(names = {"--ip"}, paramLabel = "IP", description =
      "Show info by datanode ip address")
  private String ipaddress;

  @CommandLine.Option(names = {"--uuid"}, paramLabel = "UUID", description =
      "Show info by datanode UUID")
  private String uuid;

  public String getIpaddress() {
    return ipaddress;
  }

  public void setIpaddress(String ipaddress) {
    this.ipaddress = ipaddress;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    if (Strings.isNullOrEmpty(ipaddress)) {
      ipaddress = "";
    }
    if (Strings.isNullOrEmpty(uuid)) {
      uuid = "";
    }
    if (Strings.isNullOrEmpty(ipaddress) && Strings.isNullOrEmpty(uuid)) {
      throw new IOException("ipaddress or uuid of the datanode must be " +
          "specified.");
    }

    List<HddsProtos.DatanodeUsageInfo> infoList =
        scmClient.getDatanodeUsageInfo(ipaddress, uuid);

    for (HddsProtos.DatanodeUsageInfo info : infoList) {
      printInfo(info);
    }
  }

  public void printInfo(HddsProtos.DatanodeUsageInfo info) {
    Double capacity = (double)info.getCapacity();
    Double usedRatio = info.getUsed() / capacity;
    Double remainingRatio = info.getRemaining() / capacity;
    NumberFormat percentFormat = NumberFormat.getPercentInstance();
    percentFormat.setMinimumFractionDigits(5);

    System.out.printf("%-10s: %20sB %n", "Capacity", info.getCapacity());
    System.out.printf("%-10s: %20sB (%s) %n", "SCMUsed", info.getUsed(),
        percentFormat.format(usedRatio));
    System.out.printf("%-10s: %20sB (%s) %n%n",
        "Remaining", info.getRemaining(),
        percentFormat.format(remainingRatio));
  }
}
