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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.admin.om;

import static org.apache.hadoop.ozone.admin.scm.FinalizeUpgradeCommandUtil.emitCancellationMsg;
import static org.apache.hadoop.ozone.admin.scm.FinalizeUpgradeCommandUtil.emitExitMsg;
import static org.apache.hadoop.ozone.admin.scm.FinalizeUpgradeCommandUtil.emitFinishedMsg;
import static org.apache.hadoop.ozone.admin.scm.FinalizeUpgradeCommandUtil.emitGeneralErrorMsg;
import static org.apache.hadoop.ozone.admin.scm.FinalizeUpgradeCommandUtil.handleInvalidRequestAfterInitiatingFinalization;
import static org.apache.hadoop.ozone.admin.scm.FinalizeUpgradeCommandUtil.isDone;
import static org.apache.hadoop.ozone.admin.scm.FinalizeUpgradeCommandUtil.isFinalized;
import static org.apache.hadoop.ozone.admin.scm.FinalizeUpgradeCommandUtil.isInprogress;
import static org.apache.hadoop.ozone.admin.scm.FinalizeUpgradeCommandUtil.isStarting;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import picocli.CommandLine;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(
      names = {"-id", "--service-id"},
      description = "Ozone Manager Service ID"
  )
  private String omServiceId;

  @CommandLine.Option(
      names = {"-host", "--service-host"},
      description = "Ozone Manager Host"
  )
  private String omHost;

  @CommandLine.Option(
      names = {"--takeover"},
      description = "Forces takeover of monitoring from an other client, if "
          + "finalization has already been started and did not finished yet."
  )
  private boolean force;

  @Override
  public Void call() throws Exception {
    boolean forceHA = false;
    OzoneManagerProtocol client =
        parent.createOmClient(omServiceId, omHost, forceHA);
    String upgradeClientID = "Upgrade-Client-" + UUID.randomUUID().toString();
    try {
      UpgradeFinalizer.StatusAndMessages finalizationResponse =
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
    } catch (UpgradeException e) {
      handleInvalidRequestAfterInitiatingFinalization(force, e);
    }
    monitorAndWaitFinalization(client, upgradeClientID);
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
        UpgradeFinalizer.StatusAndMessages progress =
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
