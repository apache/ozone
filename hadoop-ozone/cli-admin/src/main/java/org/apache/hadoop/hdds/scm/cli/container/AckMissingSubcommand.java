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
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerListResult;
import picocli.CommandLine;

/**
 * Acknowledge missing container(s) to suppress them from Replication Manager Report.
 */
@CommandLine.Command(
    name = "ack",
    description = "Acknowledge missing container(s) to suppress them from reports",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class AckMissingSubcommand extends ScmSubcommand {

  @CommandLine.Parameters(description = "Container IDs to acknowledge (comma-separated)",
      arity = "0..1")
  private String containers;

  @CommandLine.Option(names = {"--list"},
      description = "List all acknowledged missing containers")
  private boolean list;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    if (list) {
      // List acknowledged containers
      ContainerListResult result = scmClient.listContainer(1, Integer.MAX_VALUE);
      for (ContainerInfo info : result.getContainerInfoList()) {
        if (info.getAckMissing()) {
          out().println(info.getContainerID());
        }
      }
    } else if (containers != null && !containers.isEmpty()) {
      // Acknowledge containers
      Set<Long> ids = parseContainerIds(containers);
      for (Long id : ids) {
        try {
          int replicaCount = scmClient.getContainerReplicas(id).size();
          if (replicaCount > 0) {
            err().println("Cannot acknowledge container " + id + ": has " + replicaCount + " replica(s). " +
                "Only containers with 0 replicas can be acknowledged as missing.");
            continue;
          }
          
          ContainerInfo containerInfo = scmClient.getContainer(id);
          if (containerInfo.getNumberOfKeys() == 0) {
            err().println("Cannot acknowledge container " + id + ": container is empty (0 keys). " +
                "Empty containers are auto-deleted and don't need acknowledgement.");
            continue;
          }
          
          scmClient.acknowledgeMissingContainer(id);
          out().println("Acknowledged container: " + id);
        } catch (IOException e) {
          err().println("Failed to acknowledge container " + id + ": " + e.getMessage());
        }
      }
    } else {
      throw new IllegalArgumentException(
          "Either provide container IDs or use --list option");
    }
  }

  private Set<Long> parseContainerIds(String input) {
    return Arrays.stream(input.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .map(Long::parseLong)
        .collect(Collectors.toSet());
  }
}
