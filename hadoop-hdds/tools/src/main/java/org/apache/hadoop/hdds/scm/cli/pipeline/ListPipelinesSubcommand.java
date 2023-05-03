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

import com.google.common.base.Strings;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.server.JsonUtils;
import picocli.CommandLine;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Handler of list pipelines command.
 */
@CommandLine.Command(
    name = "list",
    description = "List all active pipelines",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ListPipelinesSubcommand extends ScmSubcommand {

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

  @CommandLine.Option(
      names = {"-s", "--state", "-fst", "--filterByState", "--filter-by-state"},
      description = "Filter listed pipelines by State, eg OPEN, CLOSED",
      defaultValue = "")
  private String state;

  @CommandLine.Option(names = { "--json" },
            defaultValue = "false",
            description = "Format output as JSON")
    private boolean json;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    Optional<Predicate<? super Pipeline>> replicationFilter =
        getReplicationFilter();

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

  private Optional<Predicate<? super Pipeline>> getReplicationFilter() {
    boolean hasReplication = !Strings.isNullOrEmpty(replication);
    boolean hasFactor = factor != null;
    boolean hasReplicationType = !Strings.isNullOrEmpty(replicationType);

    if (hasFactor) {
      if (hasReplication) {
        throw new IllegalArgumentException(
            "Factor and replication are mutually exclusive");
      }

      ReplicationConfig replicationConfig =
          RatisReplicationConfig.getInstance(factor.toProto());
      return Optional.of(
          p -> replicationConfig.equals(p.getReplicationConfig()));
    }

    if (hasReplication) {
      if (!hasReplicationType) {
        throw new IllegalArgumentException(
            "Replication type is required if replication is set");
      }

      ReplicationConfig replicationConfig =
          ReplicationConfig.parse(ReplicationType.valueOf(replicationType),
              replication, new OzoneConfiguration());
      return Optional.of(
          p -> replicationConfig.equals(p.getReplicationConfig()));
    }

    if (hasReplicationType) {
      return Optional.of(p -> p.getReplicationConfig()
          .getReplicationType()
          .toString()
          .compareToIgnoreCase(replicationType) == 0);
    }

    return Optional.empty();
  }
}
