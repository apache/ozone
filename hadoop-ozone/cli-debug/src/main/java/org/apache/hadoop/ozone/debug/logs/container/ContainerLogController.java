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
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.ozone.debug.logs.container.utils.SQLDBConstants;
import picocli.CommandLine;

/**
 * A controller for managing container log operations like parsing and listing containers.
 */

@CommandLine.Command(
    name = "container",
    subcommands = {
        ContainerInfoCommand.class,    
        ContainerLogParser.class,
        DuplicateOpenContainersCommand.class,
        ListContainers.class
    },
    description = "Tool to parse and store container logs from datanodes into a temporary SQLite database." +
            " Supports querying state transitions of container replicas using various subcommands."
)

public class ContainerLogController extends AbstractSubcommand {
  @CommandLine.Option(names = {"--db"},
      scope = CommandLine.ScopeType.INHERIT,
      description = "Path to the SQLite database file where the parsed information from logs is stored.")
  private String dbPath;

  public String getDbPath() {
    return dbPath;
  }
  
  public void setDbPath(String dbPath) {
    this.dbPath = dbPath;
  }

  public Path resolveDbPath() {
    Path resolvedPath;

    if (dbPath == null) {
      resolvedPath = Paths.get(System.getProperty("user.dir"), SQLDBConstants.DEFAULT_DB_FILENAME);

      if (Files.exists(resolvedPath) && Files.isRegularFile(resolvedPath)) {
        out().println("Using default database file found in current directory: " + resolvedPath);
      } else {
        throw new IllegalArgumentException("No database path provided and default file '" + 
            SQLDBConstants.DEFAULT_DB_FILENAME + "' not " +
            "found in current directory. Please provide a valid database path");
      }
    } else {
      resolvedPath = Paths.get(dbPath);
      Path parentDir = resolvedPath.getParent();

      if (parentDir != null && !Files.exists(parentDir)) {
        throw new IllegalArgumentException("The parent directory of the provided database " +
            "path does not exist: " + parentDir);
      }
    }

    return resolvedPath;
  }
}
