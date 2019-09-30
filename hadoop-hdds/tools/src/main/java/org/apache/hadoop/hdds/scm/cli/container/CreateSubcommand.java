/**
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

import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .ContainerWithPipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * This is the handler that process container creation command.
 */
@Command(
    name = "create",
    description = "Create container",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class CreateSubcommand implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(CreateSubcommand.class);

  @ParentCommand
  private ContainerCommands parent;

  @Option(description = "Owner of the new container", defaultValue = "OZONE",
      required = false, names = {
      "-o", "--owner"})

  private String owner;

  @Override
  public Void call() throws Exception {
    try (ScmClient scmClient = parent.getParent().createScmClient()) {
      ContainerWithPipeline container = scmClient.createContainer(owner);
      LOG.info("Container {} is created.",
          container.getContainerInfo().getContainerID());
      return null;
    }
  }
}
