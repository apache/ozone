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
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.server.JsonUtils;
import picocli.CommandLine;

/**
 * Handler of list pipelines command.
 */
@CommandLine.Command(
    name = "list",
    description = "List all active pipelines",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ListPipelinesSubcommand extends ScmSubcommand {
  @CommandLine.Mixin
  private final FilterPipelineOptions filterOptions = new FilterPipelineOptions();

  @CommandLine.Option(
      names = {"-s", "--state", "-fst", "--filterByState", "--filter-by-state"},
      description = "Filter listed pipelines by State, eg OPEN, CLOSED",
      defaultValue = "")
  private String state;

  @CommandLine.Option(
      names = {"--json"},
      defaultValue = "false",
      description = "Format output as JSON")
  private boolean json;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    Optional<Predicate<? super Pipeline>> replicationFilter = filterOptions.getReplicationFilter();

    Stream<Pipeline> stream = scmClient.listPipelines().stream();
    if (replicationFilter.isPresent()) {
      stream = stream.filter(replicationFilter.get());
    }
    if (!Strings.isNullOrEmpty(state)) {
      stream = stream.filter(p -> p.getPipelineState().toString()
          .compareToIgnoreCase(state) == 0);
    }

    if (json) {
      List<Pipeline> pipelineList = stream.collect(Collectors.toList());
      System.out.print(
              JsonUtils.toJsonStringWithDefaultPrettyPrinter(pipelineList));
    } else {
      stream.forEach(System.out::println);
    }
  }
}
