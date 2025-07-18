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

package org.apache.hadoop.ozone.freon;

import static picocli.CommandLine.Spec.Target.MIXEE;

import java.util.Optional;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.shell.ReplicationOptions;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Options for specifying replication config for Freon.
 */
public class FreonReplicationOptions extends ReplicationOptions {

  private static final String FACTOR_OPT = "--factor";

  private ReplicationFactor factor;

  @Spec(MIXEE)
  private CommandSpec spec;

  @Option(names = { "-F", FACTOR_OPT },
      description = "[deprecated] Replication factor (ONE, THREE)",
      defaultValue = "THREE"
  )
  public void setFactor(ReplicationFactor factor) {
    this.factor = factor;
  }

  // -t is already taken for number of threads
  @Option(names = {"--type", "--replication-type"},
      description = TYPE_DESCRIPTION)
  @Override
  public void setType(String type) {
    super.setType(type);
  }

  @Option(names = {"--replication", "-r"},
      description = REPLICATION_DESCRIPTION)
  @Override
  public void setReplication(String replication) {
    super.setReplication(replication);
  }

  /**
   * Support legacy --factor option.
   */
  @Override
  public Optional<ReplicationConfig> fromParams(ConfigurationSource conf) {
    if (spec != null && spec.commandLine().getParseResult() != null &&
            spec.commandLine().getParseResult().hasMatchedOption(FACTOR_OPT)) {
      return Optional.of(ReplicationConfig.fromTypeAndFactor(
          ReplicationType.RATIS, factor));
    }

    return super.fromParams(conf);
  }
}
