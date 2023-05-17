/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.admin.reconfig;

import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

abstract class AbstractReconfigureSubCommand implements Callable<Void> {
  private static final String BULK_OPERATION_IDENTIFIER = "inservernodes";

  @CommandLine.ParentCommand
  private ReconfigureCommands parent;

  @Override
  public Void call() throws Exception {
    if (parent.getAddress().equals(BULK_OPERATION_IDENTIFIER)) {
      if (!parent.getType().equals("datanode")) {
        System.out.printf("Bulk reconfiguration is not supported for '%s' " +
            "type. It is only supported for 'datanode' type.",
            parent.getType());

        return null;
      }
      ExecutorService executorService = Executors.newFixedThreadPool(5);
      List<String> nodes = parent.getAllOperableNodesClientRpcAddress();
      ReconfigureSubCommandUtil.parallelExecute(executorService, nodes,
          this::executeCommand);
    } else {
      executeCommand(parent.getAddress());
    }
    return null;
  }

  protected abstract void executeCommand(String address);
}
