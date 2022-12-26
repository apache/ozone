/*
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

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.Callable;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_INTERNAL_SERVICE_ID;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_PORT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;

/**
 * Handler of ozone admin om transfer command.
 */
@CommandLine.Command(
    name = "transfer",
    description = "Manually transfer the raft leadership to the target node.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class TransferOmLeaderSubCommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(
      names = {"-id", "--service-id"},
      description = "Ozone Manager Service ID."
  )
  private String omServiceId;

  @CommandLine.ArgGroup(multiplicity = "1")
  private TransferOption configGroup;

  static class TransferOption {
    @CommandLine.Option(
        names = {"-n", "--nodeId"},
        description = "The target Node Id of OM to transfer leadership." +
            " Will be converted to host with ratis port. E.g OM1."
    )
    private String omNodeId;

    @CommandLine.Option(
        names = {"-o", "--host"},
        description = "The target leader Ozone Manager Host to " +
            "transfer leadership in IP:PORT format."
    )
    private String omHost;

    @CommandLine.Option(names = {"-r", "--random"},
        description = "Randomly choose a follower to transfer leadership.")
    private boolean isRandom;
  }

  @Override
  public Void call() throws Exception {
    convertToHost();
    OzoneManagerProtocol client =
        parent.createOmClient(omServiceId, null, true);
    client.transferLeadership(configGroup.omHost, configGroup.isRandom);
    System.out.println("Transfer leadership success");
    return null;
  }

  /**
   * Convert the omNodeId to IP:PORT format.
   *
   * @throws IOException
   */
  private void convertToHost() throws IOException {
    if (configGroup.omNodeId != null) {
      OzoneConfiguration conf = parent.getParent().getOzoneConf();
      if (omServiceId == null) {
        omServiceId = conf.getTrimmed(OZONE_OM_INTERNAL_SERVICE_ID);
        if (omServiceId == null) {
          Collection<String> serviceIds = conf.getTrimmedStringCollection(
              OZONE_OM_SERVICE_IDS_KEY);
          if (serviceIds.size() == 1) {
            omServiceId = serviceIds.iterator().next();
          } else {
            throw new IOException("Find " + serviceIds.size() + " " +
                OZONE_OM_SERVICE_IDS_KEY + ". Please specify one with " +
                "-id/--service-id");
          }
        }
      }
      Objects.requireNonNull(omServiceId);
      String rpcAddrKey = ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
          omServiceId, configGroup.omNodeId);
      String rpcAddrStr = OmUtils.getOmRpcAddress(conf, rpcAddrKey);
      if (rpcAddrStr == null || rpcAddrStr.isEmpty()) {
        throw new IllegalArgumentException("Configuration does not have any" +
            " value set for " + rpcAddrKey + ". OM RPC Address should be" +
            " set for all nodes in an OM service.");
      }
      String ratisPortKey = ConfUtils.addKeySuffixes(OZONE_OM_RATIS_PORT_KEY,
          omServiceId, configGroup.omNodeId);
      int ratisPort = conf.getInt(ratisPortKey, OZONE_OM_RATIS_PORT_DEFAULT);
      // Remove possible RPC port
      if (rpcAddrStr.contains(":")) {
        rpcAddrStr = rpcAddrStr.split(":")[0];
      }
      configGroup.omHost = rpcAddrStr.concat(":")
          .concat(String.valueOf(ratisPort));
    }
  }
}
