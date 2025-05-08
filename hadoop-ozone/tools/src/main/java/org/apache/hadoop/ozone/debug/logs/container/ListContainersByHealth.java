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

package org.apache.hadoop.ozone.debug.logs.container;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.ozone.debug.logs.container.utils.ContainerDatanodeDatabase;
import org.apache.hadoop.ozone.shell.ListOptions;
import picocli.CommandLine;

/**
 * Lists containers based on their health state.
 */
@CommandLine.Command(
    name = "list-health",
    description = "Lists containers based on health state like UNDER_REPLICATED, " +
        "OVER_REPLICATED , UNHEALTHY, QUASI_CLOSED stuck."
)
public class ListContainersByHealth extends AbstractSubcommand implements Callable<Void> {

  @CommandLine.Option(names = {"--issue"},
      required = true,
      description = "Health state of the container.")
  private ReplicationManagerReport.HealthState healthState;

  @CommandLine.Mixin
  private ListOptions listOptions;

  @CommandLine.ParentCommand
  private ContainerLogController parent;

  @Override
  public Void call() throws Exception {

    Path dbPath = parent.resolveDbPath();
    if (dbPath == null) {
      return null;
    }

    ContainerDatanodeDatabase cdd = new ContainerDatanodeDatabase(dbPath.toString());
    
    switch (healthState) {
    case UNDER_REPLICATED:
      cdd.listReplicatedContainers("UNDER_REPLICATED", listOptions.getLimit());
      break;
    case OVER_REPLICATED:
      cdd.listReplicatedContainers("OVER_REPLICATED", listOptions.getLimit());
      break;
    case UNHEALTHY:
      cdd.listUnhealthyContainers(listOptions.getLimit());
      break;
    case QUASI_CLOSED_STUCK:
      cdd.listQuasiClosedStuckContainers(listOptions.getLimit());
      break;
    default:
      err().println("Unsupported health state: " + healthState);
    }

    return null;
  }
}

