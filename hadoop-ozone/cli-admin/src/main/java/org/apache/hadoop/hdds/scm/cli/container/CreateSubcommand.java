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

package org.apache.hadoop.hdds.scm.cli.container;

import java.io.IOException;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.ozone.shell.ShellReplicationOptions;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * This is the handler that process container creation command.
 */
@Command(
    name = "create",
    description = "Create container. If no replication config provided, " +
        "defaults to STAND_ALONE with replication factor ONE.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class CreateSubcommand extends ScmSubcommand {

  @Option(description = "Owner of the new container", defaultValue = "OZONE",
      names = { "-o", "--owner"})
  private String owner;

  @CommandLine.Mixin
  private ShellReplicationOptions containerReplicationOptions;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    ReplicationConfig replicationConfig = containerReplicationOptions.fromParamsOrConfig(new OzoneConfiguration());
    if (replicationConfig == null) {
      // if replication options not provided via command then by default STAND_ALONE container will be created.
      replicationConfig = ReplicationConfig.fromProtoTypeAndFactor(HddsProtos.ReplicationType.STAND_ALONE, 
          HddsProtos.ReplicationFactor.ONE);
    }
    ContainerWithPipeline container = scmClient.createContainer(replicationConfig, owner);
    System.out.printf("Container %s is created with replication config %s.%n",
        container.getContainerInfo().getContainerID(), replicationConfig);
  }
}
