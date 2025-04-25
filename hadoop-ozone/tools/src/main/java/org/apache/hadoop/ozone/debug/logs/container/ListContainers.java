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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.containerlog.parser.ContainerDatanodeDatabase;
import org.apache.hadoop.ozone.containerlog.parser.DBConsts;
import org.apache.hadoop.ozone.shell.ListOptions;
import picocli.CommandLine;


/**
 * List containers based on the parameter given.
 */

@CommandLine.Command(
    name = "list",
    description = "Finds containers from the database based on the option provided."
)
public class ListContainers implements Callable<Void> {
  
  @CommandLine.Option(names = {"--state"},
      description = "Life cycle state of the container.",
      required = true)
  private HddsProtos.LifeCycleState state;

  @CommandLine.Mixin
  private ListOptions listOptions;

  @CommandLine.ParentCommand
  private ContainerLogController parent;

  @Override
  public Void call() throws Exception {
    Path providedDbPath;
    if (parent.getDbPath() == null) {
      providedDbPath = Paths.get(System.getProperty("user.dir"), DBConsts.DEFAULT_DB_FILENAME);

      if (Files.exists(providedDbPath) && Files.isRegularFile(providedDbPath)) {
        System.out.println("Using default database file found in current directory: " + providedDbPath);
      } else {
        System.err.println("No database path provided and default file '" + DBConsts.DEFAULT_DB_FILENAME + "' not " +
            "found in current directory. Please provide a valid database path");
        return null;
      }
    } else {
      providedDbPath = Paths.get(parent.getDbPath());
      Path parentDir = providedDbPath.getParent();

      if (parentDir != null && !Files.exists(parentDir)) {
        System.err.println("The parent directory of the provided database path does not exist: " + parentDir);
        return null;
      }
    }

    ContainerDatanodeDatabase.setDatabasePath(providedDbPath.toString());
    
    ContainerDatanodeDatabase cdd = new ContainerDatanodeDatabase();
    try {
      cdd.listContainersByState(state.name(), listOptions.getLimit());
    } catch (Exception e) {
      throw e;
    }
    
    return null;
  }
}
