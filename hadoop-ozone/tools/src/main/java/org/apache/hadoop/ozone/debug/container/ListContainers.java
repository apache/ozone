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

import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.containerlog.parser.ContainerDatanodeDatabase;
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
      description = "state of the container",
      required = true)
  private HddsProtos.LifeCycleState state;

  @CommandLine.Mixin
  private ListOptions listOptions;

  @CommandLine.ParentCommand
  private ContainerLogController parent;

  @Override
  public Void call() throws Exception {
    
    ContainerDatanodeDatabase cdd = new ContainerDatanodeDatabase();
    try {
      cdd.listContainersByState(state.name(), listOptions.getLimit());
    } catch (Exception e) {
      System.err.println("Error while retrieving containers with state: " + state + " " + e.getMessage());
    }
    
    return null;
  }
}
