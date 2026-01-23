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

import java.io.IOException;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import picocli.CommandLine;

/**
 * Handler of createPipeline command.
 */
@CommandLine.Command(
    name = "create",
    description = "create pipeline",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class CreatePipelineSubcommand extends ScmSubcommand {

  @CommandLine.Option(
      names = {"-t", "--replication-type", "--replicationType"},
      description = "Replication type is RATIS. Full name" +
          " --replicationType will be removed in later versions.",
      defaultValue = "RATIS",
      hidden = true
  )
  private HddsProtos.ReplicationType type;

  @CommandLine.Option(
      names = {"-f", "--replication-factor", "--replicationFactor"},
      description = "Replication factor for RATIS (ONE, THREE). Full name" +
          " --replicationFactor will be removed in later versions.",
      defaultValue = "ONE"
  )
  private HddsProtos.ReplicationFactor factor;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    // Once we support creating EC containers/pipelines from the client, the
    // client should check if SCM is able to fulfil the request, and
    // understands an EcReplicationConfig. For that we also need to have SCM's
    // version here from ScmInfo response.
    // As I see there is no way to specify ECReplicationConfig properly here
    // so failing the request if type is EC, seems to be safe.
    if (type == HddsProtos.ReplicationType.CHAINED
        || type == HddsProtos.ReplicationType.EC
        || type == HddsProtos.ReplicationType.STAND_ALONE) {
      throw new IllegalArgumentException(type.name()
          + " is not supported yet.");
    }
    Pipeline pipeline = scmClient.createReplicationPipeline(
        type,
        factor,
        HddsProtos.NodePool.getDefaultInstance());

    if (pipeline != null) {
      System.out.println(pipeline.getId().toString() +
          " is created. " + pipeline.toString());
    }
  }
}
