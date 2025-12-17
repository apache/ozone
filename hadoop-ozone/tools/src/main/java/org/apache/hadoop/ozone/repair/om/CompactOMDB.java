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

package org.apache.hadoop.ozone.repair.om;

import java.io.IOException;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolClientSideImpl;
import org.apache.hadoop.ozone.repair.RepairTool;
import org.apache.hadoop.security.UserGroupInformation;
import picocli.CommandLine;

/**
 * Tool to perform compaction on a column family of an om.db.
 */
@CommandLine.Command(
    name = "compact",
    description = "CLI to compact a column family in the om.db. " +
        "The compaction happens asynchronously. Requires admin privileges." +
        " OM should be running for this tool.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class CompactOMDB extends RepairTool {

  @CommandLine.Option(names = {"--column-family", "--column_family", "--cf"},
      required = true,
      description = "Column family name")
  private String columnFamilyName;

  @CommandLine.Option(
      names = {"--service-id", "--om-service-id"},
      description = "Ozone Manager Service ID",
      required = false
  )
  private String omServiceId;

  @CommandLine.Option(
      names = {"--node-id"},
      description = "NodeID of the OM for which db needs to be compacted.",
      required = false
  )
  private String nodeId;

  @Override
  public void execute() throws Exception {

    OzoneConfiguration conf = getOzoneConf();
    OMNodeDetails omNodeDetails = OMNodeDetails.getOMNodeDetailsFromConf(
        conf, omServiceId, nodeId);
    if (!isDryRun()) {
      try (OMAdminProtocolClientSideImpl omAdminProtocolClient =
               OMAdminProtocolClientSideImpl.createProxyForSingleOM(conf,
                   UserGroupInformation.getCurrentUser(), omNodeDetails)) {
        omAdminProtocolClient.compactOMDB(columnFamilyName);
        info("Compaction request issued for om.db of om node: %s, column-family: %s.", nodeId, columnFamilyName);
        info("Please check role logs of %s for completion status.", nodeId);
      } catch (IOException ex) {
        error("Couldn't compact column %s. \nException: %s", columnFamilyName, ex);
      }
    }
  }
}
