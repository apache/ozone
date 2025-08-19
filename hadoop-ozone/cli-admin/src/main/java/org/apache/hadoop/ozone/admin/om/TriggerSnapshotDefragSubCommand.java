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
 * Handler of ozone admin om triggerSnapshotDefrag command.
 */
@CommandLine.Command(
    name = "triggerSnapshotDefrag",
    description = "Triggers the Snapshot Defragmentation Service to run " +
        "immediately. This command manually initiates the snapshot " +
        "defragmentation process which compacts snapshot data and removes " +
        "fragmentation to improve storage efficiency.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class TriggerSnapshotDefragSubCommand implements Callable<Void> {

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
      names = {"--no-wait"},
      description = "Do not wait for the defragmentation task to complete. " +
          "The command will return immediately after triggering the task.",
      defaultValue = "false"
  )
  private boolean noWait;

  @Override
  public Void call() throws Exception {
    boolean forceHA = false;

    try (OzoneManagerProtocol client = parent.createOmClient(omServiceId, omHost, forceHA)) {
      System.out.println("Triggering Snapshot Defragmentation Service...");
      boolean result = client.triggerSnapshotDefrag(noWait);

      if (noWait) {
        System.out.println("Snapshot defragmentation task has been triggered " +
            "successfully and is running in the background.");
      } else {
        if (result) {
          System.out.println("Snapshot defragmentation completed successfully.");
        } else {
          System.out.println("Snapshot defragmentation task failed or was " +
              "interrupted.");
        }
      }
    } catch (Exception e) {
      System.err.println("Failed to trigger snapshot defragmentation: " +
          e.getMessage());
      throw e;
    }

    return null;
  }
}
