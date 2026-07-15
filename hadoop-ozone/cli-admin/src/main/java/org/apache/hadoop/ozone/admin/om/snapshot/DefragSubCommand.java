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

package org.apache.hadoop.ozone.admin.om.snapshot;

import java.io.IOException;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.admin.om.OmAddressOptions;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolClientSideImpl;
import org.apache.hadoop.security.UserGroupInformation;
import picocli.CommandLine;

/**
 * Handler of ozone admin om snapshot defrag command.
 */
@CommandLine.Command(
    name = "defrag",
    description = "Triggers the Snapshot Defragmentation Service to run " +
        "immediately. This command manually initiates the snapshot " +
        "defragmentation process which compacts snapshot data and removes " +
        "fragmentation to improve storage efficiency. " +
        "This command works only on OzoneManager HA cluster.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class DefragSubCommand extends AbstractSubcommand implements Callable<Void> {

  @CommandLine.Mixin
  private OmAddressOptions.OptionalServiceIdMixin omServiceOption;

  @CommandLine.Option(
      names = {"--node-id"},
      description = "NodeID of the OM to trigger snapshot defragmentation on.",
      required = false
  )
  private String nodeId;

  @CommandLine.Option(
      names = {"--no-wait"},
      description = "Do not wait for the defragmentation task to complete. " +
          "The command will return immediately after triggering the task.",
      defaultValue = "false"
  )
  private boolean noWait;

  @Override
  public Void call() throws Exception {
    OzoneConfiguration conf = getOzoneConf();
    OMNodeDetails omNodeDetails = OMNodeDetails.getOMNodeDetailsFromConf(
        conf, omServiceOption.getServiceID(), nodeId);

    if (omNodeDetails == null) {
      System.err.println("Error: OMNodeDetails could not be determined with given " +
          "service ID and node ID.");
      return null;
    }

    try (OMAdminProtocolClientSideImpl omAdminProtocolClient = createClient(conf, omNodeDetails)) {
      execute(omAdminProtocolClient);
    } catch (IOException ex) {
      System.err.println("Failed to trigger snapshot defragmentation: " +
          ex.getMessage());
      throw ex;
    }

    return null;
  }

  protected OMAdminProtocolClientSideImpl createClient(
      OzoneConfiguration conf, OMNodeDetails omNodeDetails) throws IOException {
    return OMAdminProtocolClientSideImpl.createProxyForSingleOM(conf,
        UserGroupInformation.getCurrentUser(), omNodeDetails);
  }

  protected void execute(OMAdminProtocolClientSideImpl omAdminProtocolClient)
      throws IOException {
    System.out.println("Triggering Snapshot Defrag Service ...");
    boolean result = omAdminProtocolClient.triggerSnapshotDefrag(noWait);

    if (noWait) {
      System.out.println("Snapshot defragmentation task has been triggered " +
          "successfully and is running in the background.");
    } else {
      if (result) {
        System.out.println("Snapshot defragmentation completed successfully.");
      } else {
        System.out.println("Snapshot defragmentation task failed or was interrupted.");
      }
    }
  }
}
