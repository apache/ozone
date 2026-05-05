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

package org.apache.hadoop.ozone.shell;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.shell.jline3.PicocliCommands.PicocliCommandsFactory;

/**
 * Interactive Shell for all Ozone commands.
 */
public final class OzoneInteractiveShell {

  private static final Logger LOG = LoggerFactory.getLogger(OzoneInteractiveShell.class);

  private OzoneInteractiveShell() {
  }

  public static void main(String[] argv) throws Exception {
    PicocliCommandsFactory factory = new PicocliCommandsFactory();
    CommandLine topCmd = new CommandLine(new TopCommand(), factory);

    // Add known subcommands statically if they are in the same module.
    topCmd.addSubcommand("sh", new OzoneShell().getCmd());
    topCmd.addSubcommand("tenant", new org.apache.hadoop.ozone.shell.tenant.TenantShell().getCmd());
    topCmd.addSubcommand("s3", new org.apache.hadoop.ozone.shell.s3.S3Shell().getCmd());

    // Dynamically add subcommands from other modules to avoid circular dependencies.
    addDynamicSubcommand(topCmd, "admin", "org.apache.hadoop.ozone.admin.OzoneAdmin");
    addDynamicSubcommand(topCmd, "debug", "org.apache.hadoop.ozone.debug.OzoneDebug");
    addDynamicSubcommand(topCmd, "repair", "org.apache.hadoop.ozone.repair.OzoneRepair");

    Shell dummyShell = new Shell() {
      @Override
      public String name() {
        return "ozone";
      }

      @Override
      public String prompt() {
        return "ozone";
      }
    };

    new REPL(dummyShell, topCmd, factory, null);
  }

  private static void addDynamicSubcommand(CommandLine topCmd, String name, String className) {
    try {
      Class<?> clazz = Class.forName(className);
      GenericCli instance = (GenericCli) clazz.getDeclaredConstructor().newInstance();
      topCmd.addSubcommand(name, instance.getCmd());
    } catch (Exception e) {
      LOG.debug("Subcommand {} not loaded: class {} not found or could not be instantiated", 
          name, className, e);
    }
  }

  @Command(name = "ozone", description = "Interactive Shell for all Ozone commands", mixinStandardHelpOptions = true)
  private static class TopCommand implements Runnable {
    @Override
    public void run() {
      // The top-level command is only used to group subcommands and has no execution logic itself.
    }
  }
}
