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
import java.io.PrintWriter;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.NoSuchFileException;
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
    if (error instanceof FileSystemException) {
      String errorMessage = handleFileSystemException((FileSystemException) error);
      cmd.getErr().println(errorMessage);
    } else {
      //message could be null in case of NPE. This is unexpected so we can
      //print out the stack trace.
      if (verbose || Strings.isNullOrEmpty(error.getMessage())) {
        error.printStackTrace(cmd.getErr());
      } else {
        cmd.getErr().println(error.getMessage().split("\n")[0]);
      }
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

  protected PrintWriter out() {
    return cmd.getOut();
  }

  protected PrintWriter err() {
    return cmd.getErr();
  }

  private String handleFileSystemException(FileSystemException e) {
    // If reason is set, return the exception's message as it is.
    if (e.getReason() != null) {
      return e.getMessage();
    }

    // Otherwise, construct a custom message based on the type of exception
    String errorMessage;
    if (e instanceof NoSuchFileException) {
      errorMessage = String.format("Error: File not found: %s", e.getFile());
    } else if (e instanceof AccessDeniedException) {
      errorMessage = String.format("Error: Access denied to file: %s", e.getFile());
    } else if (e instanceof FileAlreadyExistsException) {
      errorMessage = String.format("Error: File already exists: %s", e.getFile());
    } else {
      errorMessage = String.format("Error with file: %s. Details: %s", e.getFile(), e.getMessage());
    }

    return errorMessage;
  }
}
