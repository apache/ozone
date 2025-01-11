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
package org.apache.hadoop.ozone.repair;

import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import picocli.CommandLine;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.Callable;

/** Parent class for all actionable repair commands. */
public abstract class RepairTool extends AbstractSubcommand implements Callable<Void> {

  private static final String WARNING_SYS_USER_MESSAGE =
      "ATTENTION: Running as user %s. Make sure this is the same user used to run the Ozone process." +
          " Are you sure you want to continue (y/N)? ";

  @CommandLine.Option(names = {"--force"},
      description = "Use this flag if you want to bypass the check in false-positive cases.")
  private boolean force;

  /** Hook method for subclasses for performing actual repair task. */
  protected abstract void execute() throws Exception;

  @Override
  public final Void call() throws Exception {
    confirmUser();
    execute();
    return null;
  }

  protected boolean checkIfServiceIsRunning(String serviceName) {
    String runningEnvVar = String.format("OZONE_%s_RUNNING", serviceName);
    String pidEnvVar = String.format("OZONE_%s_PID", serviceName);
    String isServiceRunning = System.getenv(runningEnvVar);
    String servicePid = System.getenv(pidEnvVar);
    if ("true".equals(isServiceRunning)) {
      if (!force) {
        error("Error: %s is currently running on this host with PID %s. " +
            "Stop the service before running the repair tool.", serviceName, servicePid);
        return true;
      } else {
        info("Warning: --force flag used. Proceeding despite %s being detected as running with PID %s.",
            serviceName, servicePid);
      }
    } else {
      info("No running %s service detected. Proceeding with repair.", serviceName);
    }
    return false;
  }

  protected void info(String msg, Object... args) {
    out().println(formatMessage(msg, args));
  }

  protected void error(String msg, Object... args) {
    err().println(formatMessage(msg, args));
  }

  private String formatMessage(String msg, Object[] args) {
    if (args != null && args.length > 0) {
      msg = String.format(msg, args);
    }
    return msg;
  }

  protected void confirmUser() {
    final String currentUser = getSystemUserName();
    final boolean confirmed = "y".equalsIgnoreCase(getConsoleReadLineWithFormat(currentUser));

    if (!confirmed) {
      throw new IllegalStateException("Aborting command.");
    }

    info("Run as user: " + currentUser);
  }

  private String getSystemUserName() {
    return System.getProperty("user.name");
  }

  private String getConsoleReadLineWithFormat(String currentUser) {
    err().printf(WARNING_SYS_USER_MESSAGE, currentUser);
    return new Scanner(System.in, StandardCharsets.UTF_8.name())
        .nextLine()
        .trim();
  }
}
