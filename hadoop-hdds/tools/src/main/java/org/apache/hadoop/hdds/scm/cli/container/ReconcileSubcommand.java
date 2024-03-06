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
package org.apache.hadoop.hdds.scm.cli.container;

import java.io.IOException;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ReconcileContainerResponseProto;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;

import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * This is the handler that process container list command.
 */
@Command(
    name = "reconcile",
    description = "Reconcile container replicas",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ReconcileSubcommand extends ScmSubcommand {

  @CommandLine.Parameters(description = "ID of the container to reconcile")
  private long containerId;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    ReconcileContainerResponseProto response = scmClient.reconcileContainer(containerId);
    if (response.hasStatus()) {
      switch (response.getStatus()) {
      case OK:
        System.out.println("Reconciliation has been triggered for container " + containerId);
        System.out.println("Use \"ozone admin container info " + containerId + "\" to check the hashes of each " +
            "container replica");
        break;
      case CONTAINER_STILL_OPEN:
        System.err.println("Cannot reconcile an open container");
      case UNSUPPORTED_CONTAINER_TYPE:
        System.err.println("Reconciliation is currently only supported on Ratis containers");
      default:
        System.err.println("Reconciliation encountered an unknown error");
      }
    }
  }
}
