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

package org.apache.hadoop.ozone.admin.upgrade;

import java.io.PrintWriter;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.admin.om.OmAddressOptions;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.QueryUpgradeStatusResponse;
import picocli.CommandLine;

/**
 * Sub command to query the overall upgrade status of the cluster, returning information about the finalization
 * status of SCM, the datanodes and OM. The command makes a single call to OM which returns the status of the other
 * components as well as itself.
 */
@CommandLine.Command(
    name = "status",
    description = "Show status of the cluster upgrade",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class StatusSubCommand extends AbstractSubcommand implements Callable<Integer> {

  @CommandLine.Mixin
  private OmAddressOptions.OptionalServiceIdOrHostMixin omAddressOptions;

  @CommandLine.Option(names = {"--json"},
      defaultValue = "false",
      description = "Format output as JSON.")
  private boolean json;

  @Override
  public Integer call() throws Exception {
    try (OzoneManagerProtocol client = getClient()) {
      OzoneManagerVersion omVersion = RpcClient.getOmVersion(client.getServiceInfo());
      if (!OzoneManagerVersion.ZDU.isSupportedBy(omVersion)) {
        err().println("OM does not support zero downtime upgrade. The cluster upgrade status should be queried with " +
            "`ozone admin scm finalizationstatus` and `ozone admin om finalizationstatus`");
        return 1;
      }
      QueryUpgradeStatusResponse status = client.queryUpgradeStatus();

      if (json) {
        out().println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(UpgradeStatusDto.from(status)));
      } else if (isVerbose()) {
        printVerbose(status, out());
      } else {
        printBasic(status, out());
      }
    }
    return 0;
  }

  /** Basic, non-verbose human-readable status. */
  static void printBasic(QueryUpgradeStatusResponse status, PrintWriter out) {
    out.println("Upgrade status:");
    out.println("    OM Finalized? " + status.getOmFinalized());
    out.println("    SCM Finalized? " + status.getHddsStatus().getScmFinalized());
    out.println("    Datanodes finalized: " + status.getHddsStatus().getNumDatanodesFinalized()
        + "/" + status.getHddsStatus().getNumDatanodesTotal());
  }

  /**
   * Verbose human-readable status that includes the apparent versions reported by OM and SCM, and the
   * range of apparent versions across healthy datanodes.
   */
  static void printVerbose(QueryUpgradeStatusResponse status, PrintWriter out) {
    HddsProtos.UpgradeStatus hdds = status.getHddsStatus();
    out.println("Upgrade status:");
    out.println("    OM Finalized?            " + status.getOmFinalized());
    out.println("    OM Apparent Version:     "
        + OzoneManagerVersion.deserialize(status.getOmApparentVersion()).toString());
    out.println("    SCM Finalized?           " + hdds.getScmFinalized());
    out.println("    SCM Apparent Version:    " + HDDSVersion.deserialize(hdds.getScmApparentVersion()).toString());
    out.println("    Datanodes finalized:     " + hdds.getNumDatanodesFinalized() + "/" + hdds.getNumDatanodesTotal());
    out.println("    Min Datanode Apparent Version: "
        + HDDSVersion.deserialize(hdds.getMinDatanodeApparentVersion()).toString());
    out.println("    Max Datanode Apparent Version: "
        + HDDSVersion.deserialize(hdds.getMaxDatanodeApparentVersion()).toString());
  }

  protected OzoneManagerProtocol getClient() throws Exception {
    return omAddressOptions.newClient();
  }

  /**
   * JSON-friendly DTO mirroring {@link QueryUpgradeStatusResponse}.
   */
  public static final class UpgradeStatusDto {
    private boolean omFinalized;
    private String omApparentVersion;
    private boolean scmFinalized;
    private String scmApparentVersion;
    private int datanodesFinalized;
    private int datanodesTotal;
    private String minDatanodeApparentVersion;
    private String maxDatanodeApparentVersion;

    public static UpgradeStatusDto from(QueryUpgradeStatusResponse status) {
      HddsProtos.UpgradeStatus hdds = status.getHddsStatus();
      UpgradeStatusDto dto = new UpgradeStatusDto();
      dto.omFinalized = status.getOmFinalized();
      dto.scmFinalized = hdds.getScmFinalized();
      dto.datanodesFinalized = hdds.getNumDatanodesFinalized();
      dto.datanodesTotal = hdds.getNumDatanodesTotal();
      dto.omApparentVersion = OzoneManagerVersion.deserialize(status.getOmApparentVersion()).toString();
      dto.scmApparentVersion = HDDSVersion.deserialize(hdds.getScmApparentVersion()).toString();
      dto.minDatanodeApparentVersion = HDDSVersion.deserialize(hdds.getMinDatanodeApparentVersion()).toString();
      dto.maxDatanodeApparentVersion = HDDSVersion.deserialize(hdds.getMaxDatanodeApparentVersion()).toString();
      return dto;
    }

    public boolean isOmFinalized() {
      return omFinalized;
    }

    public String getOmApparentVersion() {
      return omApparentVersion;
    }

    public boolean isScmFinalized() {
      return scmFinalized;
    }

    public String getScmApparentVersion() {
      return scmApparentVersion;
    }

    public int getDatanodesFinalized() {
      return datanodesFinalized;
    }

    public int getDatanodesTotal() {
      return datanodesTotal;
    }

    public String getMinDatanodeApparentVersion() {
      return minDatanodeApparentVersion;
    }

    public String getMaxDatanodeApparentVersion() {
      return maxDatanodeApparentVersion;
    }
  }

}
