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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.security.AccessControlException;
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

  @Mixin
  private ScmOption scmOption;

  @Override
  public Void call() throws Exception {
    try (ScmClient scmClient = scmOption.createScmClient()) {
      execute(scmClient);
    }
    return null;
  }

  void execute(ScmClient scmClient) throws Exception {
    final long deadline = Time.monotonicNow() + TimeUnit.SECONDS.toMillis(timeoutSeconds);
    // Call SCM in another thread, so that the timeout also applies to a call that is still retrying.
    final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
      Thread thread = new Thread(runnable, "SafeModeWait");
      thread.setDaemon(true);
      return thread;
    });
    Throwable lastError = null;
    try {
      for (long remaining = deadline - Time.monotonicNow(); remaining > 0; remaining = deadline - Time.monotonicNow()) {
        final Future<Boolean> inSafeMode = executor.submit(scmClient::inSafeMode);
        try {
          if (!inSafeMode.get(remaining, TimeUnit.MILLISECONDS)) {
            System.out.println("SCM is out of safe mode.");
            return;
          }
          System.out.printf("SCM is in safe mode. Will retry in 1 sec. Remaining time (sec): %s%n",
              getRemainingTimeInSec(deadline));
        } catch (ExecutionException e) {
          if (e.getCause() instanceof AccessControlException) {
            throw (AccessControlException) e.getCause();
          }
          lastError = e.getCause();
          System.out.printf("SCM is not available (yet?). Error is %s. Will retry in 1 sec. Remaining time (sec): %s%n",
              lastError, getRemainingTimeInSec(deadline));
        } catch (TimeoutException e) {
          break;
        }
        Thread.sleep(Math.max(0, Math.min(1000, deadline - Time.monotonicNow())));
      }
    } finally {
      executor.shutdownNow();
    }
    final TimeoutException timeout = new TimeoutException("Safe mode is not ended within the timeout period.");
    timeout.initCause(lastError);
    throw timeout;
  }

  private static long getRemainingTimeInSec(long deadline) {
    return TimeUnit.MILLISECONDS.toSeconds(Math.max(0, deadline - Time.monotonicNow()));
  }
}
