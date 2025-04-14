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

import java.sql.SQLException;
import java.util.concurrent.Callable;
import org.apache.hadoop.ozone.containerlog.parser.ContainerDatanodeDatabase;
import picocli.CommandLine;


/**
 * List containers based on the parameter given.
 */

@CommandLine.Command(
    name = "list_containers",
    description = "list containers"
)
public class ListContainers implements Callable<Void> {
  

  @CommandLine.Option(names = {"--state"},
      description = "state of the container")
  private String state;
  

  @CommandLine.ParentCommand
  private ContainerLogController parent;

  @Override
  public Void call() throws Exception {
    if (state != null) {
      ContainerDatanodeDatabase cdd = new ContainerDatanodeDatabase();
      try {
        cdd.listContainersByState(state);
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
