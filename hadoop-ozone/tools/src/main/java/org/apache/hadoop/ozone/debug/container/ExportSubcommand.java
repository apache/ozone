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

import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
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

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_NOT_FOUND;
import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;

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
      description = "Destination directory to hold exported container files")
  private String destination;

  @CommandLine.Option(names = {"--count"},
      description = "Count of containers to export")
  private long containerCount = 1;

  @CommandLine.Option(names = {"--path", "-p"},
      description = "Specifies the absolute path to the DataNode storage directory " +
          "that contains container data and metadata. When this option is provided, " +
          "the specified path will be used instead of the default directory defined " +
          "in the configuration. Example: --path=/data/hdds")
  private String dataPath;

  @Override
  public Void call() throws Exception {
    if (dataPath != null) {
      parent.setDataPath(dataPath);  // Set path in parent class
    }
    parent.loadContainersFromVolumes();

    final ContainerReplicationSource replicationSource =
        new OnDemandContainerReplicationSource(parent.getController());

    for (int i = 0; i < containerCount; i++) {
      replicationSource.prepare(containerId);
      final File destinationFile =
          new File(destination, "container-" + containerId + ".tar");
      try (FileOutputStream fos = new FileOutputStream(destinationFile)) {
        try {
          replicationSource.copyData(containerId, fos, NO_COMPRESSION);
        } catch (StorageContainerException e) {
          if (e.getResult() == CONTAINER_NOT_FOUND) {
            continue;
          }
        }
      }
      LOG.info("Container {} is exported to {}", containerId, destinationFile);
      containerId++;
    }
    return null;
  }
}
