/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.cli;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import picocli.CommandLine;

/** Base functionality for all Ozone subcommands. */
@CommandLine.Command(
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public abstract class AbstractSubcommand implements GenericParentCommand {

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  protected CommandLine.Model.CommandSpec spec() {
    return spec;
  }

  protected GenericParentCommand rootCommand() {
    return (GenericParentCommand) spec.root().userObject();
  }

  @Override
  public boolean isVerbose() {
    final GenericParentCommand root = rootCommand();
    // 'this' could be root in unit test, where subcommand is created without parents
    return root != this && root.isVerbose();
  }

  @Override
  public OzoneConfiguration createOzoneConfiguration() {
    final GenericParentCommand root = rootCommand();
    // 'this' could be root in unit test, where subcommand is created without parents
    return root != this ? root.createOzoneConfiguration() : new OzoneConfiguration();
  }

  @Override
  public OzoneConfiguration getOzoneConf() {
    final GenericParentCommand root = rootCommand();
    // 'this' could be root in unit test, where subcommand is created without parents
    return root != this ? root.getOzoneConf() : new OzoneConfiguration();
  }
}
