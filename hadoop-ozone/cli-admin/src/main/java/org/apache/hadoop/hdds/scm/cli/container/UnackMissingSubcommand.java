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
import picocli.CommandLine;

/**
 * Unacknowledge missing container(s) to report them again.
 */
@CommandLine.Command(
    name = "unack",
    description = "Unacknowledge missing container(s) to report them again",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class UnackMissingSubcommand extends ScmSubcommand {

  @CommandLine.Parameters(description = "Container IDs to unacknowledge (comma-separated)")
  private String containers;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    if (containers == null || containers.isEmpty()) {
      throw new IllegalArgumentException(
          "Container IDs must be provided");
    }

    Set<Long> ids = parseContainerIds(containers);
    for (Long id : ids) {
      try {
        ContainerInfo containerInfo = scmClient.getContainer(id);
        if (!containerInfo.getAckMissing()) {
          err().println("Cannot unacknowledge container " + id + ": " +
              "Only acknowledged missing containers can be unacknowledged.");
          continue;
        }
        scmClient.unacknowledgeMissingContainer(id);
        out().println("Unacknowledged container: " + id);
      } catch (IOException e) {
        err().println("Failed to unacknowledge container " + id + ": " + e.getMessage());
      }
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
