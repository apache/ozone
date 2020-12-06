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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;

import picocli.CommandLine;

/**
 * Handler of ozone admin om finalizeUpgrade command.
 */
@CommandLine.Command(
    name = "prepare",
    description = "Prepares OM",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class PrepareSubCommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(
      names = {"-id", "--service-id"},
      description = "Ozone Manager Service ID"
  )
  private String omServiceId;

  @CommandLine.Option(
      names = {"-ft", "--flush-wait-timeout"},
      description = "Max time to wait for OM Double Buffer flush in seconds.",
      defaultValue = "300"
  )
  private long flushWaitTime;

  @Override
  public Void call() throws Exception {
    OzoneManagerProtocol client = parent.createOmClient(omServiceId);
    long prepareTxnId = client.prepareOzoneManager(flushWaitTime, 5);
    System.out.println("Ozone Manager Prepare Request successfully returned " +
        "with Txn Id " + prepareTxnId);
    return null;
  }
}
