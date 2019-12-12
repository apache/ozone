/**
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
package org.apache.hadoop.hdds.scm.cli.datanode;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.MissingSubcommandException;
import org.apache.hadoop.hdds.scm.cli.SCMCLI;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * Subcommand for datanode related operations.
 */
@CommandLine.Command(
    name = "datanode",
    description = "Datanode specific operations",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class,
    subcommands = {
        ListInfoSubcommand.class
    })
public class DatanodeCommands implements Callable<Void> {

  @CommandLine.ParentCommand
  private SCMCLI parent;

  public SCMCLI getParent() {
    return parent;
  }

  @Override
  public Void call() throws Exception {
    throw new MissingSubcommandException(
        this.parent.getCmd().getSubcommands().get("datanode"));
  }
}
