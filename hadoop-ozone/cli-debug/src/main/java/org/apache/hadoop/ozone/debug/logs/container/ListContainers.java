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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.debug.logs.container.utils.ContainerDatanodeDatabase;
import org.apache.hadoop.ozone.shell.ListLimitOptions;
import picocli.CommandLine;


/**
 * List containers based on the parameter given.
 */

@CommandLine.Command(
    name = "list",
    description = "Finds containers from the database based on the option provided."
)
public class ListContainers extends AbstractSubcommand implements Callable<Void> {
  
  @CommandLine.ArgGroup(multiplicity = "1")
  private ExclusiveOptions exclusiveOptions;

  @CommandLine.Mixin
  private ListLimitOptions listOptions;
  
  @CommandLine.ParentCommand
  private ContainerLogController parent;

  private static final class ExclusiveOptions {
    @CommandLine.Option(names = {"--lifecycle"},
        description = "Replicas whose latest state equals the given value are shown. " +
            "Prints one row per matching replica.")
    private HddsProtos.LifeCycleState lifecycleState;

    @CommandLine.Option(names = {"--health"},
        description = "Log-derived health filter.%n"
            + " UNDER_REPLICATED: containers where healthy replica count (latest state of replica not UNHEALTHY or"
            + "  DELETED) is below the configured replication factor. Count = total active (non-DELETED) replicas.%n"
            + " OVER_REPLICATED: containers where active replica count exceeds configured replication factor and"
            + "  healthy replica count is at least equal to configured replication factor. Count = total active"
            + "  (non-DELETED) replicas.%n"
            + " UNHEALTHY: containers where every active replica is UNHEALTHY (no healthy replicas remain).%n"
            + "  Count = number of UNHEALTHY replicas.%n"
            + " QUASI_CLOSED_STUCK: approximate log heuristic only (not SCM quasi-closed stuck): containers"
            + " with at least three datanodes whose QUASI_CLOSED log entry is not superseded by CLOSED or"
            + " DELETED on that datanode.")
    private LogHealthFilter healthState;
  }

  @Override
  public Void call() throws Exception {
    
    Path dbPath = parent.resolveDbPath();

    ContainerDatanodeDatabase cdd = new ContainerDatanodeDatabase(dbPath.toString());

    if (exclusiveOptions.lifecycleState != null) {
      cdd.listContainersByState(exclusiveOptions.lifecycleState.name(), listOptions.getLimit());
    } else if (exclusiveOptions.healthState != null) {
      switch (exclusiveOptions.healthState) {
      case UNDER_REPLICATED:
      case OVER_REPLICATED:
        cdd.listReplicatedContainers(exclusiveOptions.healthState.name(), listOptions.getLimit());
        break;
      case UNHEALTHY:
        cdd.listUnhealthyContainers(listOptions.getLimit());
        break;
      case QUASI_CLOSED_STUCK:
        cdd.listQuasiClosedStuckContainers(listOptions.getLimit());
        break;
      default:
        err().println("Unsupported health state: " + exclusiveOptions.healthState);
      }
    }
    
    return null;
  }
  
  /**
   * Log-derived health filters supported by {@code list --health}.
   */
  enum LogHealthFilter {
    UNDER_REPLICATED,
    OVER_REPLICATED,
    UNHEALTHY,
    QUASI_CLOSED_STUCK
  }
}
