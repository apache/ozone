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

package org.apache.hadoop.ozone.debug.datanode.container;

import static org.apache.hadoop.ozone.debug.datanode.container.ContainerCommands.outputContainer;

import java.util.concurrent.Callable;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Handles {@code ozone debug datanode container info} command.
 */
@Command(
    name = "info",
    description = "Show container info of a container replica on datanode")
public class InfoSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private ContainerCommands parent;

  @CommandLine.Option(names = {"--container"},
      required = true,
      description = "Container Id")
  private long containerId;

  @Override
  public Void call() throws Exception {
    parent.loadContainersFromVolumes();

    Container container = parent.getController().getContainer(containerId);
    if (container != null) {
      outputContainer(container.getContainerData());
    }

    return null;
  }
}
