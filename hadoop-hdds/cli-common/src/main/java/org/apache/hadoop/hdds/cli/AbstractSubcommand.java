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

package org.apache.hadoop.hdds.cli;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.util.MemoizedSupplier;
import picocli.CommandLine;

/** Base functionality for all Ozone subcommands. */
@CommandLine.Command(
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public abstract class AbstractSubcommand {

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  private final Supplier<GenericParentCommand> rootSupplier =
      MemoizedSupplier.valueOf(() -> findRootCommand(spec));

  protected CommandLine.Model.CommandSpec spec() {
    return spec;
  }

  /** Get the Ozone object annotated with {@link CommandLine.Command}) that was used to run this command.
   * Usually this is some subclass of {@link GenericCli}, but in unit tests it could be any subcommand. */
  protected GenericParentCommand rootCommand() {
    return rootSupplier.get();
  }

  protected boolean isVerbose() {
    return rootCommand().isVerbose();
  }

  /** @see GenericParentCommand#getOzoneConf() */
  protected OzoneConfiguration getOzoneConf() {
    return rootCommand().getOzoneConf();
  }

  static GenericParentCommand findRootCommand(CommandLine.Model.CommandSpec spec) {
    Object root = spec.root().userObject();
    return root instanceof GenericParentCommand
        ? (GenericParentCommand) root
        : new NoParentCommand();
  }

  /** No-op implementation for unit tests, which may bypass creation of GenericCli object. */
  private static class NoParentCommand implements GenericParentCommand {

    private final OzoneConfiguration conf = new OzoneConfiguration();
    private UserGroupInformation user;

    @Override
    public boolean isVerbose() {
      return false;
    }

    @Override
    public OzoneConfiguration getOzoneConf() {
      return conf;
    }

    @Override
    public UserGroupInformation getUser() throws IOException {
      if (user == null) {
        user = UserGroupInformation.getCurrentUser();
      }
      return user;
    }

    @Override
    public void printError(Throwable t) {
      t.printStackTrace();
    }
  }

  protected PrintWriter out() {
    return spec().commandLine().getOut();
  }

  protected PrintWriter err() {
    return spec().commandLine().getErr();
  }
}
