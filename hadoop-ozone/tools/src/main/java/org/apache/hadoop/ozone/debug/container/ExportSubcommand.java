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

package org.apache.hadoop.ozone.debug.container;

import org.apache.hadoop.ozone.container.replication.ContainerReplicationSource;
import org.apache.hadoop.ozone.container.replication.OnDemandContainerReplicationSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.io.File;
import java.io.FileOutputStream;
import java.util.concurrent.Callable;

/**
 * Handles {@code ozone debug container export} command.
 */
@Command(
    name = "export",
    description = "Export one container to a tarball")
public class ExportSubcommand implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ExportSubcommand.class);

  @ParentCommand
  private ContainerCommands parent;

  @CommandLine.Option(names = {"--container"},
      required = true,
      description = "Container Id")
  private long containerId;

  @CommandLine.Option(names = {"--dest"},
      defaultValue = "/tmp",
      description = "Destination directory")
  private String destination;

  @Override
  public Void call() throws Exception {
    parent.loadContainersFromVolumes();

    final ContainerReplicationSource replicationSource =
        new OnDemandContainerReplicationSource(parent.getController());

    LOG.info("Starting to replication");

    replicationSource.prepare(containerId);
    LOG.info("Preparation is done");

    final File destinationFile =
        new File(destination, "container-" + containerId + ".tar.gz");
    try (FileOutputStream fos = new FileOutputStream(destinationFile)) {
      replicationSource.copyData(containerId, fos);
    }
    LOG.info("Container is exported to {}", destinationFile);

    return null;
  }
}
