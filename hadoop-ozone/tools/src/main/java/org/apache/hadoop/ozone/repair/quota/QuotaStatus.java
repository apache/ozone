/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license
 * agreements. See the NOTICE file distributed with this work for additional
 * information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.repair.quota;

import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;

/**
 * Tool to get status of last triggered quota repair.
 */
@CommandLine.Command(
    name = "status",
    description = "CLI to get the status of last trigger quota repair if available.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class QuotaStatus implements Callable<Void>  {
  @CommandLine.Spec
  private static CommandLine.Model.CommandSpec spec;

  @CommandLine.Option(
      names = {"--service-id", "--om-service-id"},
      description = "Ozone Manager Service ID",
      required = false
  )
  private String omServiceId;

  @CommandLine.Option(
      names = {"--service-host"},
      description = "Ozone Manager Host. If OM HA is enabled, use --service-id instead. "
          + "If you must use --service-host with OM HA, this must point directly to the leader OM. "
          + "This option is required when --service-id is not provided or when HA is not enabled."
  )
  private String omHost;

  @CommandLine.ParentCommand
  private QuotaRepair parent;

  @Override
  public Void call() throws Exception {
    OzoneManagerProtocol ozoneManagerClient =
        parent.createOmClient(omServiceId, omHost, false);
    System.out.println(ozoneManagerClient.getQuotaRepairStatus());
    return null;
  }
}
