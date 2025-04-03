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

package org.apache.hadoop.ozone.admin.scm;

import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.emitCancellationMsg;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.emitExitMsg;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.emitFinishedMsg;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.emitGeneralErrorMsg;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.handleInvalidRequestAfterInitiatingFinalization;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.isDone;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.isFinalized;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.isInprogress;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.isStarting;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.StatusAndMessages;
import picocli.CommandLine;

/**
 * Handler of Finalize SCM command.
 */
@CommandLine.Command(
    name = "finalizeupgrade",
    description = "Finalize SCM Upgrade",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class FinalizeScmUpgradeSubcommand extends ScmSubcommand {

  @CommandLine.ParentCommand
  private ScmAdmin parent;

  @CommandLine.Option(
      names = {"--takeover"},
      description = "Forces takeover of monitoring from another client, if "
          + "finalization has already been started and did not finish yet."
  )
  private boolean force;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    String upgradeClientID = "Upgrade-Client-" + UUID.randomUUID().toString();
    try {
      StatusAndMessages finalizationResponse =
          scmClient.finalizeScmUpgrade(upgradeClientID);
      if (isFinalized(finalizationResponse.status())) {
        System.out.println("Upgrade has already been finalized.");
        emitExitMsg();
        return;
      } else if (!isStarting(finalizationResponse.status())) {
        System.err.println("Invalid response from Storage Container Manager.");
        System.err.println(
            "Current finalization status is: " + finalizationResponse.status()
        );
        throw new IOException("Exiting...");
      }
    } catch (UpgradeException e) {
      handleInvalidRequestAfterInitiatingFinalization(force, e);
    }
    monitorAndWaitFinalization(scmClient, upgradeClientID);
    return;
  }

  private void monitorAndWaitFinalization(ScmClient client,
                                          String upgradeClientID)
      throws IOException {
    ExecutorService exec = Executors.newSingleThreadExecutor();
    Future<?> monitor =
        exec.submit(new UpgradeMonitor(client, upgradeClientID, force));
    try {
      monitor.get();
      emitFinishedMsg("Storage Container Manager");
    } catch (CancellationException e) {
      emitCancellationMsg("Storage Container Manager");
    } catch (InterruptedException e) {
      emitCancellationMsg("Storage Container Manager");
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      emitGeneralErrorMsg();
      throw new IOException(e.getCause());
    } finally {
      exec.shutdown();
    }
  }

  private static class UpgradeMonitor implements Callable<Void> {

    private ScmClient client;
    private String upgradeClientID;
    private boolean force;

    UpgradeMonitor(
        ScmClient client,
        String upgradeClientID,
        boolean force
    ) {
      this.client = client;
      this.upgradeClientID = upgradeClientID;
      this.force = force;
    }

    @Override
    public Void call() throws Exception {
      boolean finished = false;
      while (!finished) {
        Thread.sleep(500);
        // do not check for exceptions, if one happens during monitoring we
        // should report it and exit.
        UpgradeFinalization.StatusAndMessages progress =
            client.queryUpgradeFinalizationProgress(upgradeClientID, force,
                false);
        // this can happen after trying to takeover the request after the fact
        // when there is already nothing to take over.
        if (isFinalized(progress.status())) {
          System.out.println("Finalization already finished.");
          emitExitMsg();
          return null;
        }
        if (isInprogress(progress.status()) || isDone(progress.status())) {
          progress.msgs().stream().forEachOrdered(System.out::println);
        }
        if (isDone(progress.status())) {
          emitExitMsg();
          finished = true;
        }
      }
      return null;
    }
  }
}
