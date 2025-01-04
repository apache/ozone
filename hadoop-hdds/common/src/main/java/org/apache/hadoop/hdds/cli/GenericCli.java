/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.cli;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Strings;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.security.UserGroupInformation;
import picocli.CommandLine;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.Option;

/**
 * This is a generic parent class for all the ozone related cli tools.
 */
@CommandLine.Command
public abstract class GenericCli implements GenericParentCommand {

  public static final int EXECUTION_ERROR_EXIT_CODE = -1;

  private final OzoneConfiguration config = new OzoneConfiguration();
  private final CommandLine cmd;

  private UserGroupInformation user;

  @Option(names = {"--verbose"},
      description = "More verbose output. Show the stack trace of the errors.")
  private boolean verbose;

  @Option(names = {"-D", "--set"})
  public void setConfigurationOverrides(Map<String, String> configOverrides) {
    configOverrides.forEach(config::set);
  }

  @Option(names = {"-conf"})
  public void setConfigurationPath(String configPath) {
    config.addResource(new Path(configPath));
  }

  public GenericCli() {
    this(CommandLine.defaultFactory());
  }

  public GenericCli(CommandLine.IFactory factory) {
    cmd = new CommandLine(this, factory);
    cmd.setExecutionExceptionHandler((ex, commandLine, parseResult) -> {
      printError(ex);
      return EXECUTION_ERROR_EXIT_CODE;
    });

    ExtensibleParentCommand.addSubcommands(cmd);
  }

  public void run(String[] argv) {
    int exitCode = execute(argv);

    if (exitCode != ExitCode.OK) {
      System.exit(exitCode);
    }
  }

  @VisibleForTesting
  public int execute(String[] argv) {
    return cmd.execute(argv);
  }

  protected void printError(Throwable error) {
    //message could be null in case of NPE. This is unexpected so we can
    //print out the stack trace.
    if (verbose || Strings.isNullOrEmpty(error.getMessage())) {
      error.printStackTrace(System.err);
    } else {
      System.err.println(error.getMessage().split("\n")[0]);
    }
  }

  @Override
  public OzoneConfiguration getOzoneConf() {
    return config;
  }

  public UserGroupInformation getUser() throws IOException {
    if (user == null) {
      user = UserGroupInformation.getCurrentUser();
    }
    return user;
  }

  @VisibleForTesting
  public CommandLine getCmd() {
    return cmd;
  }

  @Override
  public boolean isVerbose() {
    return verbose;
  }
}
