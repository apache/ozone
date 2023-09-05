/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.admin.om;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * Handler of om updateranger command.
 *
 * Usage:
 * ozone admin om updateranger -host=om
 * ozone admin om updateranger -id=ozone1
 */
@CommandLine.Command(
    name = "updateranger",
    description = "Trigger Ranger sync background service task on a leader OM "
        + "that pushes policies and roles updates to Ranger. "
        + "This operation requires Ozone administrator privilege.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class UpdateRangerSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(
      names = {"-id", "--service-id"},
      description = "Ozone Manager Service ID"
  )
  private String omServiceId;

  @CommandLine.Option(
      names = {"-host", "--service-host"},
      description = "Ozone Manager Host. If OM HA is enabled, use -id instead. "
          + "If insists on using -host with OM HA, this must point directly "
          + "to the leader OM. "
          + "This option is required when -id is not provided or "
          + "when HA is not enabled."
  )
  private String omHost;

  @CommandLine.Option(names = {"--no-wait"},
      description = "Do not wait for task completion. Exit immediately "
          + "after the Ranger BG sync task is triggered on the OM.")
  private boolean noWait;

  @Override
  public Void call() throws Exception {

    if (StringUtils.isEmpty(omServiceId) && StringUtils.isEmpty(omHost)) {
      System.err.println("Error: Please specify -id or -host");
      return null;
    }

    boolean forceHA = false;
    try (OzoneManagerProtocol client = parent.createOmClient(
        omServiceId, omHost, forceHA)) {

      boolean res = client.triggerRangerBGSync(noWait);

      if (res) {
        System.out.println("Operation completed successfully");
      } else {
        System.err.println("Operation completed with errors. "
            + "Check OM log for details");
      }

    } catch (OzoneClientException ex) {
      System.err.printf("Error: %s", ex.getMessage());
    }
    return null;
  }

}
