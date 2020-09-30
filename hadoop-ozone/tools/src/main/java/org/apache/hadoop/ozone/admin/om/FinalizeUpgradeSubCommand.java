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

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;

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
      if (isFinalized(finalizationResponse.status())){
        System.out.println("Upgrade has already been finalized.");
        emitExitMsg();
        return null;
      } else if (!isStarting(finalizationResponse.status())){
        System.err.println("Invalid response from Ozone Manager.");
        System.err.println(
            "Current finalization status is: " + finalizationResponse.status()
        );
        throw new IOException("Exiting...");
      }
    } catch (OMException e) {
      handleInvalidRequestAfterInitiatingFinalization(e);
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
      emitFinishedMsg();
    } catch (CancellationException|InterruptedException e) {
      emitCancellationMsg();
    } catch (ExecutionException e) {
      emitGeneralErrorMsg();
      throw e;
    } finally {
      exec.shutdown();
    }
  }

  private void handleInvalidRequestAfterInitiatingFinalization(
      OMException e) throws IOException {
    if (e.getResult().equals(INVALID_REQUEST)) {
      if (force) {
        return;
      }
      System.err.println("Finalization is already in progress, it is not"
          + "possible to initiate it again.");
      e.printStackTrace(System.err);
      System.err.println("If you want to track progress from a new client"
          + "for any reason, use --takeover, and the status update will be"
          + "received by the new client. Note that with forcing to monitor"
          + "progress from a new client, the old one initiated the upgrade"
          + "will not be able to monitor the progress further and exit.");
      throw new IOException("Exiting...");
    } else {
      throw e;
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
            client.queryUpgradeFinalizationProgress(upgradeClientID, force);
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
  private static void emitExitMsg() {
    System.out.println("Exiting...");
  }

  private static boolean isFinalized(UpgradeFinalizer.Status status) {
    return status.equals(UpgradeFinalizer.Status.ALREADY_FINALIZED);
  }

  private static boolean isDone(UpgradeFinalizer.Status status) {
    return status.equals(UpgradeFinalizer.Status.FINALIZATION_DONE);
  }

  private static boolean isInprogress(UpgradeFinalizer.Status status) {
    return status.equals(UpgradeFinalizer.Status.FINALIZATION_IN_PROGRESS);
  }

  private static boolean isStarting(UpgradeFinalizer.Status status) {
    return status.equals(UpgradeFinalizer.Status.STARTING_FINALIZATION);
  }

  private static void emitGeneralErrorMsg() {
    System.err.println("Finalization was not successful.");
  }

  private static void emitFinishedMsg() {
    System.out.println("Finalization of Ozone Manager's metadata upgrade "
        + "finished.");
  }

  private static void emitCancellationMsg() {
    System.err.println("Finalization command was cancelled. Note that, this"
        + "will not cancel finalization in Ozone Manager. Progress can be"
        + "monitored in the Ozone Manager's log.");
  }
}
