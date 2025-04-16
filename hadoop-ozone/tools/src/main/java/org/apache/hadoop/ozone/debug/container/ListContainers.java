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

package org.apache.hadoop.ozone.debug.container;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import org.apache.hadoop.ozone.containerlog.parser.ContainerDatanodeDatabase;
import picocli.CommandLine;


/**
 * List containers based on the parameter given.
 */

@CommandLine.Command(
    name = "list-containers",
    description = "Finds containers from the database based on the option provided."
)
public class ListContainers implements Callable<Void> {
  /**
   * Container states.
   * Used to filter container records during database query.
   */
  public enum ContainerState {
    OPEN, CLOSED, QUASI_CLOSED, CLOSING, DELETED, UNHEALTHY
  }
  
  @CommandLine.Option(names = {"--state", "-s"},
      description = "state of the container")
  private ContainerState state;
  
  @CommandLine.Option(names = {"--limit", "-l"},
          description = "number of rows to display in the output , by default it would be 100 rows")
  private Integer limit;

  @CommandLine.Option(names = {"--file-path", "-p"},
          description = "path to the output file")
  private String path;

  @CommandLine.ParentCommand
  private ContainerLogController parent;

  @Override
  public Void call() throws Exception {
    if (state != null) {
      if (path != null) {
        Path outputPath = Paths.get(path);
        Path parentDir = outputPath.getParent();
        if (parentDir != null && !Files.exists(parentDir)) {
          System.out.println("The directory does not exist: " + parentDir.toAbsolutePath());
          return null;
        }
      }
      ContainerDatanodeDatabase cdd = new ContainerDatanodeDatabase();
      try {
        cdd.listContainersByState(state.name(), path, limit);
      } catch (SQLException e) {
        System.out.println("Error while retrieving containers with state: " + state + e.getMessage());
      } catch (Exception e) {
        System.out.println("Unexpected error while processing state: " + state + e.getMessage());
      }
    } else {
      System.out.println("state not provided");
    }

    return null;
  }

}
