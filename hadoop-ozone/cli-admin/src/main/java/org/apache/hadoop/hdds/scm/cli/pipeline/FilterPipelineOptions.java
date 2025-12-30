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
import java.util.Optional;
import java.util.function.Predicate;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import picocli.CommandLine;

/**
 * Defines command-line option for filtering pipelines.
 */
public class FilterPipelineOptions {
  @CommandLine.Option(
      names = {"-t", "--type"},
      description = "Filter pipelines by replication type, RATIS or EC",
      defaultValue = "")
  private String replicationType;

  @CommandLine.Option(
      names = {"-r", "--replication"},
      description = "Filter pipelines by replication, eg ONE, THREE or for EC rs-3-2-1024k",
      defaultValue = "")
  private String replication;

  @CommandLine.Option(
      names = {"-ffc", "--filterByFactor", "--filter-by-factor"},
      description = "[deprecated] Filter pipelines by factor (e.g. ONE, THREE) (implies RATIS replication type)")
  private ReplicationFactor factor;

  Optional<Predicate<? super Pipeline>> getReplicationFilter() {
    boolean hasReplication = !Strings.isNullOrEmpty(replication);
    boolean hasFactor = factor != null;
    boolean hasReplicationType = !Strings.isNullOrEmpty(replicationType);

    if (hasFactor) {
      if (hasReplication) {
        throw new IllegalArgumentException("Factor and replication are mutually exclusive");
      }
      ReplicationConfig replicationConfig = RatisReplicationConfig.getInstance(factor.toProto());
      return Optional.of(p -> replicationConfig.equals(p.getReplicationConfig()));
    }

    if (hasReplication) {
      if (!hasReplicationType) {
        throw new IllegalArgumentException("Replication type is required if replication is set");
      }

      ReplicationConfig replicationConfig =
          ReplicationConfig.parse(ReplicationType.valueOf(replicationType), replication, new OzoneConfiguration());
      return Optional.of(p -> replicationConfig.equals(p.getReplicationConfig()));
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
