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

package org.apache.hadoop.ozone.admin.om;

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
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization;
import picocli.CommandLine;

/**
 * Handler of ozone admin om finalizeUpgrade command.
 */
@CommandLine.Command(
    name = "finalizeupgrade",
    description = "Finalizes Ozone Manager's metadata changes and enables new "
        + "features after a software upgrade.\n"
        + "It is possible to specify the service ID for an HA environment, "
        + "or the Ozone manager host in a non-HA environment, if none provided "
        + "the default from configuration is being used if not ambiguous.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class FinalizeUpgradeSubCommand implements Callable<Void> {

  @CommandLine.Mixin
  private OmAddressOptions.OptionalServiceIdOrHostMixin omAddressOptions;

  @CommandLine.Option(
      names = {"--takeover"},
      description = "Forces takeover of monitoring from an other client, if "
          + "finalization has already been started and did not finished yet."
  )
  private boolean force;

  @Override
  public Void call() throws Exception {
    String upgradeClientID = "Upgrade-Client-" + UUID.randomUUID();
    try (OzoneManagerProtocol client = omAddressOptions.newClient()) {
      UpgradeFinalization.StatusAndMessages finalizationResponse =
          client.finalizeUpgrade(upgradeClientID);
      if (isFinalized(finalizationResponse.status())) {
        System.out.println("Upgrade has already been finalized.");
        emitExitMsg();
        return null;
      } else if (!isStarting(finalizationResponse.status())) {
        System.err.println("Invalid response from Ozone Manager.");
        System.err.println(
            "Current finalization status is: " + finalizationResponse.status()
        );
        throw new IOException("Exiting...");
      }
      monitorAndWaitFinalization(client, upgradeClientID);
    } catch (UpgradeException e) {
      handleInvalidRequestAfterInitiatingFinalization(force, e);
    }
    return null;
  }

  private void monitorAndWaitFinalization(OzoneManagerProtocol client,
      String upgradeClientID) throws ExecutionException {
    ExecutorService exec = Executors.newSingleThreadExecutor();
    Future<?> monitor =
        exec.submit(new UpgradeMonitor(client, upgradeClientID, force));
    try {
      monitor.get();
      emitFinishedMsg("Ozone Manager");
    } catch (CancellationException e) {
      emitCancellationMsg("Ozone Manager");
    } catch (InterruptedException e) {
      emitCancellationMsg("Ozone Manager");
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      emitGeneralErrorMsg();
      throw e;
    } finally {
      exec.shutdown();
    }
  }

  private static class UpgradeMonitor implements Callable<Void> {

    private OzoneManagerProtocol client;
    private String upgradeClientID;
    private boolean force;

    UpgradeMonitor(
        OzoneManagerProtocol client,
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
