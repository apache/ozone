package org.apache.hadoop.hdds.scm.cli.datanode;
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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import java.util.concurrent.Callable;

/**
 * View status of one or more datanodes.
 */
@Command(
    name = "status",
    description = "Show status of datanodes",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class,
    subcommands = {
        DecommissionStatusSubCommand.class
    })

@MetaInfServices(SubcommandWithParent.class)
public class StatusSubCommand implements Callable<Void>, SubcommandWithParent {

  @CommandLine.ParentCommand
  private DatanodeCommands parent;

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @Override
  public Void call() throws Exception {
    GenericCli.missingSubcommand(spec);
    return null;
  }

  public DatanodeCommands getParent() {
    return parent;
  }

  @VisibleForTesting
  public void setParent(OzoneConfiguration conf) {
    parent = new DatanodeCommands();
    parent.setParent(conf);
  }

  @Override
  public Class<?> getParentType() {
    return DatanodeCommands.class;
  }
}
