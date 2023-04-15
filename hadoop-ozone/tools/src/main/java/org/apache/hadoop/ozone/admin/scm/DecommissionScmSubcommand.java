/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.admin.scm;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.RemoveSCMRequest;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmResponseProto;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;

import java.io.IOException;

/**
 * Handler of ozone admin scm decommission command.
 */
@CommandLine.Command(
    name = "decommission",
    description = "Decommission SCM <scmid>.  Includes removing from ratis "
    + "ring and removing its certificate from certStore",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)


public class DecommissionScmSubcommand extends ScmSubcommand {
  @CommandLine.ParentCommand
  private ScmAdmin parent;

  @CommandLine.Option(names = {"-clusterid", "--clusterid"},
      description = "ClusterID of the SCM cluster to decommission node from.",
      required = true)
  private String clusterId;

  @CommandLine.Option(names = {"-nodeid", "--nodeid"},
      description = "NodeID of the SCM to be decommissioned.",
      required = true)
  private String nodeId;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    DecommissionScmResponseProto response = scmClient
        .decommissionScm(new RemoveSCMRequest(clusterId, nodeId, ""));
    HddsProtos.RemoveScmResponseProto removeResponse =
        response.getRemoveScmResponse();
    if (!removeResponse.getSuccess()) {
      System.out.println("Error decommissioning Scm "
          + removeResponse.getScmId());
      System.out.println(response.getRemoveScmError());
    } else {
      System.out.println("Decommissioned Scm " + removeResponse.getScmId()
          + " from cluster "  + clusterId);
    }
  }
}

