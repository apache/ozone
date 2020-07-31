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
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;

import static org.apache.hadoop.hdds.scm.cli.container.ContainerCommands.checkContainerExists;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * This is the handler that process delete container command.
 */
@Command(
    name = "delete",
    description = "Delete container",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DeleteSubcommand extends ScmSubcommand {

  @Parameters(description = "Id of the container to close")
  private long containerId;

  @Option(names = {"-f",
      "--force"}, description = "forcibly delete the container")
  private boolean force;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    checkContainerExists(scmClient, containerId);
    scmClient.deleteContainer(containerId, force);
  }
}
