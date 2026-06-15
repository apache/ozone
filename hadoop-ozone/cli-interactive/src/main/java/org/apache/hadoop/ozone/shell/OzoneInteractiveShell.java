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

import java.util.List;
import org.apache.hadoop.ozone.admin.OzoneAdmin;
import org.apache.hadoop.ozone.debug.OzoneDebug;
import org.apache.hadoop.ozone.shell.s3.S3Shell;
import org.apache.hadoop.ozone.shell.tenant.TenantShell;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.shell.jline3.PicocliCommands.PicocliCommandsFactory;

/**
 * Interactive Shell for all Ozone commands.
 */
public final class OzoneInteractiveShell {

  private OzoneInteractiveShell() {
  }

  public static void main(String[] argv) throws Exception {
    PicocliCommandsFactory factory = new PicocliCommandsFactory();
    CommandLine topCmd = new CommandLine(new TopCommand(), factory);

    topCmd.addSubcommand("sh", new OzoneShell().getCmd());
    topCmd.addSubcommand("tenant", new TenantShell().getCmd());
    topCmd.addSubcommand("s3", new S3Shell().getCmd());
    topCmd.addSubcommand("admin", new OzoneAdmin().getCmd());
    topCmd.addSubcommand("debug", new OzoneDebug().getCmd());

    Shell dummyShell = new Shell() {
      @Override
      public String name() {
        return "ozone";
      }

      @Override
      public String prompt() {
        return "ozone";
      }

      @Override
      protected List<String> interactiveWelcomeLines() {
        return OzoneInteractiveWelcome.lines();
      }
    };

    new REPL(dummyShell, topCmd, factory, null, dummyShell.interactiveWelcomeLines());
  }

  @Command(name = "ozone", description = "Interactive Shell for all Ozone commands",
      mixinStandardHelpOptions = true)
  private static class TopCommand implements Runnable {
    @Override
    public void run() {
      // The top-level command is only used to group subcommands and has no execution logic itself.
    }
  }
}
