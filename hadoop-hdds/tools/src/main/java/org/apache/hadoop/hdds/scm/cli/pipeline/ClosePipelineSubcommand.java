/**
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

import com.google.common.base.Strings;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import picocli.CommandLine;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Handler of close pipeline command.
 */
@CommandLine.Command(
    name = "close",
    description = "Close pipeline",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ClosePipelineSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private PipelineCommands parent;

  @CommandLine.Parameters(description = "ID of the pipeline to close,"
      + "'ALL' means all pipeline")
  private String pipelineId;

  @CommandLine.Option(names = {"-ffc", "--filterByFactor"},
      description = "Filter listed pipelines by Factor(ONE/one)",
      defaultValue = "",
      required = false)
  private String factor;

  @Override
  public Void call() throws Exception {
    try (ScmClient scmClient = parent.getParent().createScmClient()) {
      if (pipelineId.equalsIgnoreCase("ALL")) {
        List<Pipeline> successList = new LinkedList<>();
        List<Pipeline> unsuccessList = new LinkedList<>();

        scmClient.listPipelines().stream()
            .filter(p -> (Strings.isNullOrEmpty(factor) ||
                (p.getFactor().toString().compareToIgnoreCase(factor) == 0)))
            .forEach(pipeline -> {
              try {
                scmClient.closePipeline(
                    HddsProtos.PipelineID.newBuilder()
                        .setId(pipeline.getId().getId().toString()).build());
                successList.add(pipeline);
              } catch (IOException e) {
                unsuccessList.add(pipeline);
              }
            });
        System.out.println("Successfully closed: " + successList.size());
        successList.forEach(System.out::println);
        System.out.println("Unsuccessfully closed: " + unsuccessList.size());
        unsuccessList.forEach(System.out::println);
      } else {
        scmClient.closePipeline(
            HddsProtos.PipelineID.newBuilder().setId(pipelineId).build());
      }
      return null;
    }
  }
}
