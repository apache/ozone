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

import org.apache.hadoop.hdds.protocol.ReconfigureProtocol;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Handler of ozone admin reconfig properties command.
 */
@Command(
    name = "properties",
    description = "List reconfigurable properties",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ReconfigurePropertiesSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private ReconfigureCommands parent;

  @Override
  public Void call() throws Exception {
    ReconfigureProtocol reconfigProxy = ReconfigureSubCommandUtil
        .getSingleNodeReconfigureProxy(parent.getAddress());
    List<String> properties = reconfigProxy.listReconfigureProperties();
    System.out.printf("Node [%s] Reconfigurable properties:%n",
        parent.getAddress());
    for (String name : properties) {
      System.out.println(name);
    }
    return null;
  }

}
