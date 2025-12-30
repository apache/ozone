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
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import picocli.CommandLine;
import picocli.shell.jline3.PicocliCommands.PicocliCommandsFactory;

/**
 * Ozone user interface commands.
 * <p>
 * This class uses dispatch method to make calls
 * to appropriate handlers that execute the ozone functions.
 */
@CommandLine.Command
public abstract class Shell extends GenericCli {

  public static final String OZONE_URI_DESCRIPTION =
      "Ozone URI could either be a full URI or short URI.\n" +
          "Full URI should start with o3://, in case of non-HA\nclusters it " +
          "should be followed by the host name and\noptionally the port " +
          "number. In case of HA clusters\nthe service id should be used. " +
          "Service id provides a\nlogical name for multiple hosts and it is " +
          "defined\nin the property ozone.om.service.ids.\n" +
          "Example of a full URI with host name and port number\nfor a key:" +
          "\no3://omhostname:9862/vol1/bucket1/key1\n" +
          "With a service id for a volume:" +
          "\no3://omserviceid/vol1/\n" +
          "Short URI should start from the volume." +
          "\nExample of a short URI for a bucket:" +
          "\nvol1/bucket1\n" +
          "Any unspecified information will be identified from\n" +
          "the config files.\n";

  private String name;

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @CommandLine.ArgGroup
  private ExecutionMode executionMode;

  private static class ExecutionMode {
    @CommandLine.Option(names = {"--interactive"}, description = "Run in interactive mode")
    private boolean interactive;

    @CommandLine.Option(names = {"--execute"}, description = "Run command as part of batch")
    private List<String> command;
  }

  public Shell() {
    super(new PicocliCommandsFactory());
    getCmd().setExecutionStrategy(this::execute);
  }

  public String name() {
    return name;
  }

  // override if custom prompt is needed
  public String prompt() {
    return name();
  }

  private int execute(CommandLine.ParseResult parseResult) {
    name = spec.name();

    if (parseResult.hasMatchedOption("--interactive") || parseResult.hasMatchedOption("--execute")) {
      spec.name(""); // use short name (e.g. "token get" instead of "ozone sh token get")
      installBatchExceptionHandler();
      new REPL(this, getCmd(), (PicocliCommandsFactory) getCmd().getFactory(), executionMode.command);
      return 0;
    }

    TracingUtil.initTracing("shell", getOzoneConf());
    String spanName = spec.name() + " " + String.join(" ", parseResult.originalArgs());
    return TracingUtil.executeInNewSpan(spanName, () -> new CommandLine.RunLast().execute(parseResult));
  }

  private void installBatchExceptionHandler() {
    // exit on first error in batch mode
    if (!executionMode.interactive) {
      CommandLine.IExecutionExceptionHandler handler = getCmd().getExecutionExceptionHandler();
      getCmd().setExecutionExceptionHandler((ex, cli, parseResult) -> {
        try {
          return handler.handleExecutionException(ex, cli, parseResult);
        } finally {
          System.exit(EXECUTION_ERROR_EXIT_CODE);
        }
      });
    }
  }

  @Override
  public void printError(Throwable errorArg) {
    OMException omException = null;

    if (errorArg instanceof OMException) {
      omException = (OMException) errorArg;
    } else if (errorArg.getCause() instanceof OMException) {
      // If the OMException occurred in a method that could not throw a
      // checked exception (like an Iterator implementation), it will be
      // chained to an unchecked exception and thrown.
      omException = (OMException) errorArg.getCause();
    }

    if (omException != null && !isVerbose()) {
      // In non-verbose mode, reformat OMExceptions as error messages to the
      // user.
      err().println(String.format("%s %s", omException.getResult().name(),
              omException.getMessage()));
    } else {
      // Prints the stack trace when in verbose mode.
      super.printError(errorArg);
    }
  }
}
