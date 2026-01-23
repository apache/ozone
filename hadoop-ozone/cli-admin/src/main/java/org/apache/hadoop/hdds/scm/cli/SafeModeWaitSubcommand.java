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

package org.apache.hadoop.hdds.scm.cli;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.util.Time;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * This is the handler that process safe mode wait command.
 */
@Command(
    name = "wait",
    description = "Wait until the scm is out from the safe mode.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class SafeModeWaitSubcommand implements Callable<Void> {

  @Option(description =
      "Define timeout (in second) to wait until (exit code 1) "
          + "or until safemode is ended (exit code 0).", defaultValue = "30",
      names = { "-t", "--timeout"})
  private long timeoutSeconds;

  private long startTestTime;

  @Mixin
  private ScmOption scmOption;

  @Override
  public Void call() throws Exception {
    startTestTime = Time.monotonicNow();

    while (getRemainingTimeInSec() > 0) {
      try (ScmClient scmClient = scmOption.createScmClient()) {
        long remainingTime;
        do {
          if (!scmClient.inSafeMode()) {
            System.out.println("SCM is out of safe mode.");
            return null;
          }

          remainingTime = getRemainingTimeInSec();

          if (remainingTime > 0) {
            System.out.printf(
                "SCM is in safe mode. Will retry in 1 sec. Remaining time "
                    + "(sec): %s%n",
                remainingTime);
            Thread.sleep(1000);
          } else {
            System.out.println("SCM is in safe mode. No more retries.");
          }
        } while (remainingTime > 0);
      } catch (InterruptedException ex) {
        System.out.printf(
            "SCM is not available (yet?). Error is %s. Will retry in 1 sec. "
                + "Remaining time (sec): %s%n",
            ex.getMessage(), getRemainingTimeInSec());
        Thread.sleep(1000);
        Thread.currentThread().interrupt();
      }
    }
    throw new TimeoutException(
        "Safe mode is not ended within the timeout period.");
  }

  private long getRemainingTimeInSec() {
    return timeoutSeconds - (Time.monotonicNow() - startTestTime) / 1000;
  }
}
