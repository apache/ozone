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

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Handler to get Datanode Volume Density report.
 */
@Command(
    name = "report",
    description = "Get Datanode Volume Density Report",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DiskBalancerReportSubcommand extends ScmSubcommand {
  @Option(names = {"-c", "--count"},
      description = "Result count to return. Sort by Volume Density " +
          "in descending order. Defaults to 25")
  private int count = 25;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    List<HddsProtos.DatanodeDiskBalancerInfoProto> resultProto =
        scmClient.getDiskBalancerReport(count);
    System.out.println(generateReport(resultProto));
  }

  private String generateReport(
      List<HddsProtos.DatanodeDiskBalancerInfoProto> protos) {
    StringBuilder formatBuilder = new StringBuilder("Report result:%n" +
        "%-50s %s%n");

    List<String> contentList = new ArrayList<>();
    contentList.add("Datanode");
    contentList.add("VolumeDensity");

    for (HddsProtos.DatanodeDiskBalancerInfoProto proto: protos) {
      formatBuilder.append("%-50s %s%n");
      contentList.add(proto.getNode().getHostName());
      contentList.add(String.valueOf(proto.getCurrentVolumeDensitySum()));
    }

    return String.format(formatBuilder.toString(),
        contentList.toArray(new String[0]));
  }
}
