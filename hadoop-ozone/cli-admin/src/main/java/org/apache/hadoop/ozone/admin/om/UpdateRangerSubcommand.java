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

import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;

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

  @CommandLine.Mixin
  private OmAddressOptions.OptionalServiceIdOrHostMixin omAddressOptions;

  @CommandLine.Option(names = {"--no-wait"},
      description = "Do not wait for task completion. Exit immediately "
          + "after the Ranger BG sync task is triggered on the OM.")
  private boolean noWait;

  @Override
  public Void call() throws Exception {
    try (OzoneManagerProtocol client = omAddressOptions.newClient()) {

      boolean res = client.triggerRangerBGSync(noWait);

      if (res) {
        System.out.println("Operation completed successfully");
      } else {
        System.err.println("Operation completed with errors. "
            + "Check OM log for details");
      }
    }
    return null;
  }

}
