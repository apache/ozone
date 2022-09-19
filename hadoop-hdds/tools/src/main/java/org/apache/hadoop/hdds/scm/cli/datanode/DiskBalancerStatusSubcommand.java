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
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * Handler to get disk balancer status.
 */
@Command(
    name = "status",
    description = "Get Datanode DiskBalancer Status",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DiskBalancerStatusSubcommand extends ScmSubcommand {

  private final Set<String> stateSet =
      new HashSet<>(Arrays.asList("RUNNING", "STOPPED", "UNKNOWN"));

  @Option(names = {"-s", "--state"},
      description = "RUNNING, STOPPED, UNKNOWN")
  private String state = "RUNNING";

  @Parameters(description = "List of fully qualified host names")
  private List<String> hosts = new ArrayList<>();

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    if (state != null && !stateSet.contains(state.toUpperCase(Locale.ROOT))) {
      System.err.println("Unsupported state: " + state);
    }

    assert state != null;
    List<HddsProtos.DatanodeDiskBalancerInfoProto> resultProto =
        scmClient.getDiskBalancerStatus(
            hosts.size() == 0 ? Optional.empty() : Optional.of(hosts),
            Optional.of(HddsProtos.DiskBalancerRunningStatus.valueOf(
                state.toUpperCase(Locale.ROOT)))
            );

    System.out.println(generateStatus(resultProto));
  }

  private String generateStatus(
      List<HddsProtos.DatanodeDiskBalancerInfoProto> protos) {
    StringBuilder formatBuilder = new StringBuilder("Status result:%n" +
        "%-50s %s %s %s %s %s%n");

    List<String> contentList = new ArrayList<>();
    contentList.add("Datanode");
    contentList.add("VolumeDensity");
    contentList.add("Status");
    contentList.add("Threshold");
    contentList.add("BandwidthInMB");
    contentList.add("ParallelThread");

    for (HddsProtos.DatanodeDiskBalancerInfoProto proto: protos) {
      formatBuilder.append("%-50s %s %s %s %s %s%n");
      contentList.add(proto.getNode().getHostName());
      contentList.add(String.valueOf(proto.getCurrentVolumeDensitySum()));
      contentList.add(proto.getRunningStatus().name());
      contentList.add(
          String.valueOf(proto.getDiskBalancerConf().getThreshold()));
      contentList.add(
          String.valueOf(proto.getDiskBalancerConf().getDiskBandwidthInMB()));
      contentList.add(
          String.valueOf(proto.getDiskBalancerConf().getParallelThread()));
    }

    return String.format(formatBuilder.toString(),
        contentList.toArray(new String[0]));
  }
}
