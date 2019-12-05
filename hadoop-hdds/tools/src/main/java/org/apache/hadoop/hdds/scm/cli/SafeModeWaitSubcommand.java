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
package org.apache.hadoop.hdds.scm.cli;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.client.ScmClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * This is the handler that process safe mode wait command.
 */
@Command(
    name = "wait",
    description = "Wait until the scm is out from the safe mode.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class SafeModeWaitSubcommand implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(SafeModeWaitSubcommand.class);

  @Option(description =
      "Define timeout (in second) to wait until (exit code 1) "
          + "or until safemode is ended (exit code 0).", defaultValue = "30",
      required = false, names = {
      "-t", "--timeout"})
  private long timeoutSeconds;

  private long startTestTime;

  @ParentCommand
  private SafeModeCommands parent;

  @Override
  public Void call() throws Exception {
    startTestTime = System.currentTimeMillis();

    while (getRemainingTimeInSec() > 0) {
      try (ScmClient scmClient = parent.getParent().createScmClient()) {
        while (getRemainingTimeInSec() > 0) {

          boolean isSafeModeActive = scmClient.inSafeMode();

          if (!isSafeModeActive) {
            LOG.info("SCM is out of safe mode.");
            return null;
          } else {
            LOG.info(
                "SCM is in safe mode. Will retry in 1 sec. Remaining time "
                    + "(sec): {}",
                getRemainingTimeInSec());
            Thread.sleep(1000);
          }
        }
      } catch (Exception ex) {
        LOG.info(
            "SCM is not available (yet?). Error is {}. Will retry in 1 sec. "
                + "Remaining time (sec): {}",
            ex.getMessage(), getRemainingTimeInSec());
        Thread.sleep(1000);
      }
    }
    throw new TimeoutException(
        "Safe mode is not ended within the timeout period.");
  }

  private long getRemainingTimeInSec() {
    return timeoutSeconds - (System.currentTimeMillis() - startTestTime) / 1000;
  }
}
