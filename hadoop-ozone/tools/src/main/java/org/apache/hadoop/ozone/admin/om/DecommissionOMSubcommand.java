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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocol.OMConfiguration;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolClientSideImpl;
import org.apache.hadoop.security.UserGroupInformation;
import picocli.CommandLine;

import java.util.concurrent.Callable;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DECOMMISSIONED_NODES_KEY;

/**
 * Handler of om roles command.
 */
@CommandLine.Command(
    name = "decommission",
    customSynopsis = "ozone admin om decommission -id=<om-service-id> " +
        "-nodeid=<decommission-om-node-id> " +
        "-hostname=<decommission-om-node-address> [options]",
    description = "Decommission an OzoneManager." +
        "\nNote - Add the node to be decommissioned to " +
        OZONE_OM_DECOMMISSIONED_NODES_KEY + "config in ozone-site.xml of all " +
        "OzoneManagers before proceeding with decommission." +
        "\nNOTE THAT DECOMMISSIONING AN OM MIGHT RENDER THE CLUSTER TO LOSE " +
        "HIGH AVAILABILITY",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DecommissionOMSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(names = {"-id", "--service-id"},
      description = "OM Service ID",
      required = true)
  private String omServiceId;

  @CommandLine.Option(names = {"-nodeid"},
      description = "NodeID of the OM to be decommissioned.",
      required = true)
  private String decommNodeId;

  @CommandLine.Option(names = {"-hostname", "--node-host-address"},
      description = "Host name/address of the OM to be decommissioned.",
      required = true)
  private String hostname;

  private InetAddress hostInetAddress;

  @CommandLine.Option(
      names = {"--force"},
      description = "This option will skip checking whether OM configs " +
          "have been updated with the decommissioned node added to " +
          "ozone.om.decommissioned.nodes config in ozone-site.xml."
  )
  private boolean force;

  private OzoneConfiguration ozoneConf;
  private UserGroupInformation user;

  @Override
  public Void call() throws IOException {
    ozoneConf = parent.getParent().getOzoneConf();
    user = parent.getParent().getUser();

    verifyNodeIdAndHostAddress();
    if (!force) {
      verifyConfigUpdatedOnAllOMs();
    }

    // Proceed with decommissioning the OM by contacting the current OM
    // leader.
    try (OMAdminProtocolClientSideImpl omAdminProtocolClient =
             OMAdminProtocolClientSideImpl.createProxyForOMHA(ozoneConf, user,
                 omServiceId)) {
      OMNodeDetails decommNodeDetails = new OMNodeDetails.Builder()
          .setOMNodeId(decommNodeId)
          .setHostAddress(hostInetAddress.getHostAddress())
          .build();
      omAdminProtocolClient.decommission(decommNodeDetails);

      System.out.println("Successfully decommissioned OM " + decommNodeId);
    } catch (IOException e) {
      System.out.println("Failed to decommission OM " + decommNodeId);
      throw e;
    }
    return null;
  }

  /**
   * Verify that the provided nodeId and host address correspond to the same
   * OM in the configs.
   */
  private void verifyNodeIdAndHostAddress() throws IOException {
    String rpcAddrKey = ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
        omServiceId, decommNodeId);
    String rpcAddrStr = OmUtils.getOmRpcAddress(ozoneConf, rpcAddrKey);
    if (rpcAddrStr == null || rpcAddrStr.isEmpty()) {
      throw new IOException("There is no OM corresponding to " + decommNodeId
          + "in the configuration.");
    }

    hostInetAddress = InetAddress.getByName(hostname);
    InetAddress rpcAddressFromConfig = InetAddress.getByName(
        rpcAddrStr.split(":")[0]);

    if (!hostInetAddress.equals(rpcAddressFromConfig.getAddress())) {
      throw new IOException("OM " + decommNodeId + "'s host address in " +
          "config - " + rpcAddressFromConfig.getAddress() + " does not match " +
          "the provided host address " + hostInetAddress);
    }
  }

  /**
   * Verify that the to be decommissioned node is added to the
   * OZONE_OM_DECOMMISSIONED_NODES_KEY.<SERVICE_ID> config in ozone-site.xml
   * of all OMs.
   */
  private void verifyConfigUpdatedOnAllOMs() throws IOException {
    String decommNodesKey = ConfUtils.addKeySuffixes(
        OZONE_OM_DECOMMISSIONED_NODES_KEY, omServiceId);
    Collection<String> decommNodes = ozoneConf.getTrimmedStringCollection(
        decommNodesKey);
    if (!decommNodes.contains(decommNodeId)) {
      throw new IOException("Please add the to be decommissioned OM "
          + decommNodeId + " to the " + decommNodesKey + " config in " +
          "ozone-site.xml of all nodes.");
    }

    // For each OM, we need to get the reloaded config and check that the
    // decommissioned node is either removed from ozone.om.nodes config or
    // added to ozone.om.decommissioned.nodes
    List<OMNodeDetails> activeOMNodeDetails = OmUtils.getAllOMHAAddresses(
        ozoneConf, omServiceId, false);
    for (OMNodeDetails nodeDetails : activeOMNodeDetails) {
      verifyOMConfigUpdate(nodeDetails);
    }
  }

  /**
   * Verify that the to be decommissioned node is added to the
   * OZONE_OM_DECOMMISSIONED_NODES_KEY.<SERVICE_ID> config in ozone-site.xml
   * of given OM.
   */
  private void verifyOMConfigUpdate(OMNodeDetails omNodeDetails)
      throws IOException {
    try (OMAdminProtocolClientSideImpl omAdminProtocolClient =
             OMAdminProtocolClientSideImpl.createProxyForSingleOM(ozoneConf,
                 user, omNodeDetails)) {
      OMConfiguration omConfig = omAdminProtocolClient.getOMConfiguration();
      OMNodeDetails decommNodeDetails = omConfig
          .getDecommissionedNodesInNewConf().get(decommNodeId);
      if (decommNodeDetails == null) {
        throw new IOException("OM " + omNodeDetails.getNodeId() + "'s " +
            "reloaded config does not have OM " + decommNodeId + " in the " +
            "decommissioned nodes list.");
      }
      if (!decommNodeDetails.getRpcAddress().getAddress().equals(
          hostInetAddress)) {
        throw new IOException("OM " + omNodeDetails.getNodeId() + "'s " +
            "reloaded config has decommissioning OM " + decommNodeId + "'s " +
            "address as " + decommNodeDetails.getRpcAddress().getAddress() +
            " whereas provided host address is " + hostInetAddress);
      }
    }
  }
}
