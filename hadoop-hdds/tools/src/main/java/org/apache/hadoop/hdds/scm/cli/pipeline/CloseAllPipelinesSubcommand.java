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

package org.apache.hadoop.hdds.scm.cli.pipeline;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Handler of close pipeline command.
 */
@CommandLine.Command(
    name = "closeAll",
    description = "Close all pipelines",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class CloseAllPipelinesSubcommand extends ScmSubcommand {

  @CommandLine.Option(names = {"-t", "--type"},
      description = "Filter listed pipelines by replication type, RATIS or EC",
      defaultValue = "")
  private String replicationType;

  @CommandLine.Option(
      names = {"-r", "--replication"},
      description = "Filter listed pipelines by replication, eg ONE, THREE or "
          + "for EC rs-3-2-1024k",
      defaultValue = "")
  private String replication;

  @CommandLine.Option(
      names = {"-ffc", "--filterByFactor", "--filter-by-factor"},
      description = "[deprecated] Filter pipelines by factor (e.g. ONE, THREE) "
          + " (implies RATIS replication type)")
  private ReplicationFactor factor;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    Optional<Predicate<? super Pipeline>> replicationFilter =
        ListPipelinesSubcommand.getReplicationFilter(replication, factor, replicationType);

    Stream<Pipeline> stream =
        scmClient.listPipelines()
            .stream()
            .filter(p -> p.getPipelineState() != Pipeline.PipelineState.CLOSED);
    if (replicationFilter.isPresent()) {
      stream = stream.filter(replicationFilter.get());
    }

    stream.forEach(pipeline -> {
      try {
        scmClient.closePipeline(HddsProtos.PipelineID.newBuilder().setId(pipeline.getId().getId().toString()).build());
      } catch (IOException e) {
        System.out.println("Can't close pipeline: " + pipeline.getId() + ", cause: " + e.getMessage());
      }
    });
  }
}
