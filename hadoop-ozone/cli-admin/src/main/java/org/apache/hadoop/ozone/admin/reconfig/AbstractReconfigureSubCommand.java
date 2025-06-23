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

package org.apache.hadoop.ozone.admin.reconfig;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import picocli.CommandLine;

/**
 * An abstract Class use to ReconfigureSubCommand.
 */
@CommandLine.Command
public abstract class AbstractReconfigureSubCommand implements Callable<Void> {
  @CommandLine.ParentCommand
  private ReconfigureCommands parent;

  @Override
  public Void call() throws Exception {
    if (parent.isBatchReconfigDatanodes()) {
      ExecutorService executorService = Executors.newFixedThreadPool(5);
      List<String> nodes = parent.getAllOperableNodesClientRpcAddress();
      ReconfigureSubCommandUtil.parallelExecute(executorService, nodes,
          this::executeCommand);
    } else {
      if (parent.getAddress() == null || parent.getAddress().isEmpty()) {
        System.out.println("Error: --address must be specified if" +
            " --in-service-datanodes is not given.");
        return null;
      }
      executeCommand(parent.getService(), parent.getAddress());
    }
    return null;
  }

  protected abstract void executeCommand(HddsProtos.NodeType nodeType, String address);
}
