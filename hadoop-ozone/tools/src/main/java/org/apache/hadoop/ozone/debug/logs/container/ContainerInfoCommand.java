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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.ozone.debug.logs.container.utils.ContainerDatanodeDatabase;
import org.apache.hadoop.ozone.debug.logs.container.utils.SQLDBConstants;
import picocli.CommandLine;

/**
 * Command to display detailed information of a single container by ID.
 */

@CommandLine.Command(
    name = "info",
    description = "provides complete state transition history of each replica for a single container along with " +
        "analysis over the container"
)
public class ContainerInfoCommand extends AbstractSubcommand implements Callable<Void> {

  @CommandLine.Parameters(index = "0", description = "Container ID")
  private Long containerId;

  @CommandLine.ParentCommand
  private ContainerLogController parent;

  @Override
  public Void call() throws Exception {

    if (containerId < 0) {
      err().println("Invalid container ID: " + containerId);
      return null;
    }

    Path providedDbPath;
    if (parent.getDbPath() == null) {
      providedDbPath = Paths.get(System.getProperty("user.dir"), SQLDBConstants.DEFAULT_DB_FILENAME);

      if (Files.exists(providedDbPath) && Files.isRegularFile(providedDbPath)) {
        out().println("Using default database file found in current directory: " + providedDbPath);
      } else {
        err().println("No database path provided and default file '" + SQLDBConstants.DEFAULT_DB_FILENAME + "' not " +
            "found in current directory. Please provide a valid database path");
        return null;
      }
    } else {
      providedDbPath = Paths.get(parent.getDbPath());
      Path parentDir = providedDbPath.getParent();

      if (parentDir != null && !Files.exists(parentDir)) {
        err().println("The parent directory of the provided database path does not exist: " + parentDir);
        return null;
      }
    }

    ContainerDatanodeDatabase.setDatabasePath(providedDbPath.toString());

    ContainerDatanodeDatabase cdd = new ContainerDatanodeDatabase();
    try {
      cdd.showContainerDetails(containerId);
    } catch (Exception e) {
      throw e;
    }

    return null;
  }

}
