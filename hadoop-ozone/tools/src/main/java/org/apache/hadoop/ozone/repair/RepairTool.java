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

package org.apache.hadoop.ozone.repair;

import jakarta.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import picocli.CommandLine;

/** Parent class for all actionable repair commands. */
@CommandLine.Command
public abstract class RepairTool extends AbstractSubcommand implements Callable<Void> {

  private static final String WARNING_SYS_USER_MESSAGE =
      "ATTENTION: Running as user %s. Make sure this is the same user used to run the Ozone process." +
          " Are you sure you want to continue (y/N)? ";

  @CommandLine.Option(names = {"--force"},
      description = "Use this flag if you want to bypass the check in false-positive cases.")
  private boolean force;

  @CommandLine.Option(names = {"--dry-run"},
      defaultValue = "false",
      fallbackValue = "true",
      description = "Simulate repair, but do not make any changes")
  private boolean dryRun;

  private Scanner scanner;

  protected Scanner getScanner() {
    if (scanner == null) {
      scanner = new Scanner(System.in, StandardCharsets.UTF_8.name());
    }
    return scanner;
  }

  /** Hook method for subclasses for performing actual repair task. */
  protected abstract void execute() throws Exception;

  /** Which Ozone component should be verified to be offline. */
  @Nullable
  protected Component serviceToBeOffline() {
    return null;
  }

  @Override
  public final Void call() throws Exception {
    try {
      final Component service = serviceToBeOffline();
      if (!dryRun && service != null) { // offline tool
        confirmUser();
      }
      if (isServiceStateOK()) {
        execute();
      }
      return null;
    } finally {
      scanner = null;
    }
  }

  private boolean isServiceStateOK() {
    final Component service = serviceToBeOffline();

    if (service == null) {
      return true; // online tool
    }

    if (dryRun) {
      info("Skipping %s service check in dry run mode.", service);
      return true;
    }

    if (!isServiceRunning(service)) {
      info("No running %s service detected. Proceeding with repair.", service);
      return true;
    }

    String servicePid = getServicePid(service);

    if (force) {
      info("Warning: --force flag used. Proceeding despite %s being detected as running with PID %s.",
          service, servicePid);
      return true;
    }

    error("Error: %s is currently running on this host with PID %s. " +
        "Stop the service before running the repair tool.", service, servicePid);

    return false;
  }

  private static String getServicePid(Component service) {
    return System.getenv(String.format("OZONE_%s_PID", service));
  }

  private static boolean isServiceRunning(Component service) {
    return "true".equals(System.getenv(String.format("OZONE_%s_RUNNING", service)));
  }

  protected boolean isDryRun() {
    return dryRun;
  }

  /** Print to stdout the formatted from {@code msg} and {@code args}. */
  protected void info(String msg, Object... args) {
    out().println(formatMessage(msg, args));
  }

  /** Print to stderr the formatted from {@code msg} and {@code args}. */
  protected void error(String msg, Object... args) {
    err().println(formatMessage(msg, args));
  }

  /** Print to stderr the message formatted from {@code msg} and {@code args},
   * and also print the exception {@code t}. */
  protected void error(Throwable t, String msg, Object... args) {
    error(msg, args);
    rootCommand().printError(t);
  }

  /** Fail with {@link IllegalStateException} using the message formatted from {@code msg} and {@code args}. */
  protected void fatal(String msg, Object... args) {
    String formatted = formatMessage(msg, args);
    throw new IllegalStateException(formatted);
  }

  private String formatMessage(String msg, Object[] args) {
    if (args != null && args.length > 0) {
      msg = String.format(msg, args);
    }
    if (isDryRun()) {
      msg = "[dry run] " + msg;
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
    return getScanner().nextLine().trim();
  }

  /** Ozone component for offline tools. */
  public enum Component {
    DATANODE,
    OM,
    SCM,
  }
}
