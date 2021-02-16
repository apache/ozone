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

/**
 * Command to list the disk metrics of a datanode.
 */
@Command(
    name = "disk-metrics",
    description = "List disk metrics " +
        "(such as Capacity, SCMUsed, Remaining) of a datanode by IP address " +
        "or UUID",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DiskMetricsSubCommand extends ScmSubcommand {

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

    HddsProtos.DatanodeDiskMetrics metrics =
        scmClient.getDatanodeDiskMetrics(ipaddress, uuid);
    Double capacity = Double.parseDouble(metrics.getCapacity());
    Double used = Double.parseDouble(metrics.getUsed());
    Double remaining = Double.parseDouble(metrics.getRemaining());
    Double usedRatio = used / capacity;
    Double remainingRatio = remaining / capacity;
    NumberFormat percentFormat = NumberFormat.getPercentInstance();
    percentFormat.setMinimumFractionDigits(5);

    System.out.printf("%-10s: %20sB %n", "Capacity", metrics.getCapacity());
    System.out.printf("%-10s: %20sB (%s) %n", "SCMUsed", metrics.getUsed(),
        percentFormat.format(usedRatio));
    System.out.printf("%-10s: %20sB (%s) %n",
        "Remaining", metrics.getRemaining(),
        percentFormat.format(remainingRatio));
  }
}
