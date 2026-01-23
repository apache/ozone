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

package org.apache.hadoop.ozone.admin.reconfig;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.cli.AdminSubcommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.ozone.admin.OzoneAdmin;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Subcommand to group reconfigure OM related operations.
 */
@Command(
    name = "reconfig",
    description = "Dynamically reconfigure server without restarting it",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class,
    subcommands = {
        ReconfigureStartSubcommand.class, 
        ReconfigureStatusSubcommand.class,
        ReconfigurePropertiesSubcommand.class
    })
@MetaInfServices(AdminSubcommand.class)
public class ReconfigureCommands implements AdminSubcommand {

  @CommandLine.ParentCommand
  private OzoneAdmin parent;

  @CommandLine.Option(names = {"--service"},
      description = "service: OM, SCM, DATANODE.",
      required = true)
  private String service;

  @CommandLine.Option(names = {"--address"},
      description = "node address: <ip:port> or <hostname:port>.",
      required = false)
  private String address;

  @CommandLine.Option(names = {"--in-service-datanodes"},
      description = "If set, the client will send reconfiguration requests " +
          "to all available DataNodes in the IN_SERVICE operational state.",
      required = false)
  private boolean batchReconfigDatanodes;

  public String getAddress() {
    return address;
  }

  public HddsProtos.NodeType getService() {
    return HddsProtos.NodeType.valueOf(service);
  }

  public boolean isBatchReconfigDatanodes() {
    return batchReconfigDatanodes;
  }

  public List<String> getAllOperableNodesClientRpcAddress() {
    List<String> nodes;
    try (ScmClient scmClient = new ContainerOperationClient(
        parent.getOzoneConf())) {
      nodes = ReconfigureSubCommandUtil
          .getAllOperableNodesClientRpcAddress(scmClient);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return nodes;
  }
}
