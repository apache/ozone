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

import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;

import picocli.CommandLine;

/**
 * Handler of ozone admin om finalizeUpgrade command.
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

  @CommandLine.Option(
      names = {"-id", "--service-id"},
      description = "Ozone Manager Service ID",
      required = true
  )
  private String omServiceId;

  @CommandLine.Option(
      names = {"-tawt", "--transaction-apply-wait-timeout"},
      description = "Max time in SECONDS to wait for all transactions before" +
          "the prepare request to be applied to the OM DB.",
      defaultValue = "300",
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

  @Override
  public Void call() throws Exception {
    OzoneManagerProtocol client = parent.createOmClient(omServiceId);
    long prepareTxnId = client.prepareOzoneManager(txnApplyWaitTimeSeconds,
        txnApplyCheckIntervalSeconds);
    System.out.println("Ozone Manager Prepare Request successfully returned " +
        "with Txn Id " + prepareTxnId);
    return null;
  }
}
