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

package org.apache.hadoop.hdds.scm.cli.pipeline;

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import picocli.CommandLine;

/**
 * Handler of close pipeline command.
 */
@CommandLine.Command(
    name = "close",
    description = "Close pipeline",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ClosePipelineSubcommand extends ScmSubcommand {
  @CommandLine.ArgGroup(multiplicity = "1")
  private CloseOptionGroup closeOption;

  @CommandLine.Mixin
  private final FilterPipelineOptions filterOptions = new FilterPipelineOptions();

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    if (!Strings.isNullOrEmpty(closeOption.pipelineId)) {
      if (filterOptions.getReplicationFilter().isPresent()) {
        throw new IllegalArgumentException("Replication filters can only be used with --all");
      }
      scmClient.closePipeline(HddsProtos.PipelineID.newBuilder().setId(closeOption.pipelineId).build());
    } else if (closeOption.closeAll) {
      Optional<Predicate<? super Pipeline>> replicationFilter = filterOptions.getReplicationFilter();

      List<Pipeline> pipelineList = new ArrayList<>();
      Predicate<? super Pipeline> predicate = replicationFilter.orElse(null);
      List<Pipeline> pipelines = scmClient.listPipelines();
      if (predicate == null) {
        for (Pipeline pipeline : pipelines) {
          if (pipeline.getPipelineState() != Pipeline.PipelineState.CLOSED) {
            pipelineList.add(pipeline);
          }
        }
      } else {
        for (Pipeline pipeline : pipelines) {
          boolean filterPassed = predicate.test(pipeline);
          if (pipeline.getPipelineState() != Pipeline.PipelineState.CLOSED && filterPassed) {
            pipelineList.add(pipeline);
          }
        }
      }

      System.out.println("Sending close command for " + pipelineList.size() + " pipelines...");
      pipelineList.forEach(pipeline -> {
        try {
          scmClient.closePipeline(
              HddsProtos.PipelineID.newBuilder().setId(pipeline.getId().getId().toString()).build());
        } catch (IOException e) {
          System.err.println("Error closing pipeline: " + pipeline.getId() + ", cause: " + e.getMessage());
        }
      });
    }
  }

  private static class CloseOptionGroup {
    @CommandLine.Parameters(description = "ID of the pipeline to close")
    private String pipelineId;

    @CommandLine.Option(
        names = {"--all"},
        description = "Close all pipelines")
    private boolean closeAll;
  }
}
