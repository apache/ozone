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

import static org.apache.hadoop.ozone.OmUtils.getOmHostsFromConfig;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus.PREPARE_COMPLETED;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus;
import org.apache.hadoop.util.Time;
import picocli.CommandLine;

/**
 * Handler of ozone admin om prepare command.
 */
@CommandLine.Command(
    name = "prepare",
    description = "Prepares Ozone Manager for upgrade/downgrade, by applying " +
        "all pending transactions, taking a Ratis snapshot at the last " +
        "transaction and purging all logs on each OM instance. The returned " +
        "transaction #ID corresponds to the last transaction in the quorum in" +
        " which the snapshot is taken.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class PrepareSubCommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Mixin
  private OmAddressOptions.MandatoryServiceIdMixin omServiceOption;

  @CommandLine.Option(
      names = {"-tawt", "--transaction-apply-wait-timeout"},
      description = "Max time in SECONDS to wait for all transactions before" +
          "the prepare request to be applied to the OM DB.",
      defaultValue = "120",
      hidden = true
  )
  private long txnApplyWaitTimeSeconds;

  @CommandLine.Option(
      names = {"-tact", "--transaction-apply-check-interval"},
      description = "Time in SECONDS to wait between successive checks for " +
          "all transactions to be applied to the OM DB.",
      defaultValue = "5",
      hidden = true
  )
  private long txnApplyCheckIntervalSeconds;

  @CommandLine.Option(
      names = {"-pct", "--prepare-check-interval"},
      description = "Time in SECONDS to wait between successive checks for OM" +
          " preparation.",
      defaultValue = "10",
      hidden = true
  )
  private long prepareCheckInterval;

  @CommandLine.Option(
      names = {"-pt", "--prepare-timeout"},
      description = "Max time in SECONDS to wait for all OMs to be prepared",
      defaultValue = "300",
      hidden = true
  )
  private long prepareTimeOut;

  @Override
  public Void call() throws Exception {
    try (OzoneManagerProtocol client = omServiceOption.newClient()) {
      execute(client);
    }
    return null;
  }

  private void execute(OzoneManagerProtocol client) throws Exception {
    long prepareTxnId = client.prepareOzoneManager(txnApplyWaitTimeSeconds,
        txnApplyCheckIntervalSeconds);
    System.out.println("Ozone Manager Prepare Request successfully returned " +
        "with Transaction Id : [" + prepareTxnId + "].");

    Map<String, Boolean> omPreparedStatusMap = new HashMap<>();
    Set<String> omHosts = getOmHostsFromConfig(
        parent.getParent().getOzoneConf(), omServiceOption.getServiceID());
    omHosts.forEach(h -> omPreparedStatusMap.put(h, false));
    Duration pTimeout = Duration.of(prepareTimeOut, ChronoUnit.SECONDS);
    Duration pInterval = Duration.of(prepareCheckInterval, ChronoUnit.SECONDS);

    System.out.println();
    System.out.println("Checking individual OM instances for prepare request " +
        "completion...");
    long endTime = Time.monotonicNow() + pTimeout.toMillis();
    int expectedNumPreparedOms = omPreparedStatusMap.size();
    int currentNumPreparedOms = 0;
    while (Time.monotonicNow() < endTime &&
        currentNumPreparedOms < expectedNumPreparedOms) {
      for (Map.Entry<String, Boolean> e : omPreparedStatusMap.entrySet()) {
        if (!e.getValue()) {
          String omHost = e.getKey();
          try (OzoneManagerProtocol singleOmClient =
                    parent.createOmClient(omServiceOption.getServiceID(), omHost, false)) {
            PrepareStatusResponse response =
                singleOmClient.getOzoneManagerPrepareStatus(prepareTxnId);
            PrepareStatus status = response.getStatus();
            System.out.println("OM : [" + omHost + "], Prepare " +
                "Status : [" + status.name() + "], Current Transaction Id : [" +
                response.getCurrentTxnIndex() + "]");
            if (status.equals(PREPARE_COMPLETED)) {
              e.setValue(true);
              currentNumPreparedOms++;
            }
          } catch (IOException ioEx) {
            System.out.println("Exception while checking preparation " +
                "completeness for [" + omHost +
                "], Error : [" + ioEx.getMessage() + "]");
          }
        }
      }
      if (currentNumPreparedOms < expectedNumPreparedOms) {
        System.out.println("Waiting for " + prepareCheckInterval +
            " seconds before retrying...");
        Thread.sleep(pInterval.toMillis());
      }
    }
    if (currentNumPreparedOms < (expectedNumPreparedOms + 1) / 2) {
      throw new Exception("OM Preparation failed since a majority OMs are not" +
          " prepared yet.");
    } else {
      System.out.println();
      System.out.println("OM Preparation successful! ");
      if (currentNumPreparedOms == expectedNumPreparedOms) {
        System.out.println("All OMs are prepared");
      } else {
        System.out.println("A majority of OMs are prepared. OMs that are not " +
            "prepared : " +
            omPreparedStatusMap.entrySet().stream()
                .filter(e -> !e.getValue())
                .map(e -> e.getKey())
                .collect(Collectors.joining()));
      }
      System.out.println("No new write requests will be allowed until " +
          "preparation is cancelled or upgrade/downgrade is done.");
    }
  }

}
