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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import static org.apache.hadoop.hdds.scm.cli.container.upgrade.UpgradeManager.LOG;

/**
 * Handler to get disk balancer status.
 */
@Command(
    name = "status",
    description = "Get Datanode DiskBalancer Status",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DiskBalancerStatusSubcommand extends ScmSubcommand {

  @Option(names = {"-s", "--state"},
      description = "Display only datanodes with the given status: RUNNING, STOPPED, UNKNOWN.")
  private HddsProtos.DiskBalancerRunningStatus state = null;

  @CommandLine.Option(names = {"-d", "--datanodes"},
      description = "Get diskBalancer status on specific datanodes.")
  private List<String> hosts = new ArrayList<>();

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    List<HddsProtos.DatanodeDiskBalancerInfoProto> resultProto =
        scmClient.getDiskBalancerStatus(
            hosts.isEmpty() ? Optional.empty() : Optional.of(hosts),
            state == null ? Optional.empty() : Optional.of(state));

    System.out.println(generateStatus(resultProto));
//    String metricsJson = scmClient.getMetrics("Hadoop:service=HddsDatanode,name=UgiMetrics");
//    System.out.println("metricsJson : {}"+ metricsJson);
//    int successMoveCount = -1;
//    int failureMoveCount = -1;
//    JsonNode jsonNode = null;
//    if (metricsJson != null) {
//      ObjectMapper objectMapper = new ObjectMapper();
//      JsonFactory factory = objectMapper.getFactory();
//      JsonParser parser = factory.createParser(metricsJson);
//      jsonNode = (JsonNode) objectMapper.readTree(parser).get("beans").get(0);
//      JsonNode successCount = jsonNode.get("SuccessCount");
//      JsonNode failureCount = jsonNode.get("FailureCount");
//      successMoveCount = (successCount == null ? -1 : Integer.parseInt(successCount.toString()));
//      failureMoveCount = (failureCount == null ? -1 : Integer.parseInt(failureCount.toString()));
//    }
//    LOG.info("SuccessMoveCount : {} , failuremovecount : {} .",successMoveCount , failureMoveCount);
//    System.out.println("SuccessMoveCount : "+ successMoveCount);
//    System.out.println("FailureMoveCount : "+ failureMoveCount);
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
