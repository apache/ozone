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

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;

import java.io.IOException;
import java.util.Collection;
import org.apache.hadoop.hdds.cli.AdminSubcommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.admin.OzoneAdmin;
import org.apache.hadoop.ozone.admin.om.lease.LeaseSubCommand;
import org.apache.hadoop.ozone.admin.om.snapshot.SnapshotSubCommand;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.protocolPB.Hadoop3OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.ClientId;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

/**
 * Subcommand for admin operations related to OM.
 */
@CommandLine.Command(
    name = "om",
    description = "Ozone Manager specific admin operations",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class,
    subcommands = {
        FinalizeUpgradeSubCommand.class,
        ListOpenFilesSubCommand.class,
        GetServiceRolesSubcommand.class,
        PrepareSubCommand.class,
        CancelPrepareSubCommand.class,
        FinalizationStatusSubCommand.class,
        DecommissionOMSubcommand.class,
        UpdateRangerSubcommand.class,
        TransferOmLeaderSubCommand.class,
        FetchKeySubCommand.class,
        LeaseSubCommand.class,
        SnapshotSubCommand.class
    })
@MetaInfServices(AdminSubcommand.class)
public class OMAdmin implements AdminSubcommand {

  @CommandLine.ParentCommand
  private OzoneAdmin parent;

  public OzoneAdmin getParent() {
    return parent;
  }

  public ClientProtocol createClient(String omServiceId) throws Exception {
    OzoneConfiguration conf = parent.getOzoneConf();
    if (OmUtils.isOmHAServiceId(conf, omServiceId)) {
      return OzoneClientFactory.getRpcClient(omServiceId, conf).getObjectStore()
        .getClientProxy();
    } else {
      throw new OzoneClientException("This command works only on OzoneManager" +
            " HA cluster. Service ID specified does not match" +
            " with " + OZONE_OM_SERVICE_IDS_KEY + " defined in the " +
            "configuration. Configured " + OZONE_OM_SERVICE_IDS_KEY + " are " +
            conf.getTrimmedStringCollection(OZONE_OM_SERVICE_IDS_KEY) + "\n");
    }
  }

  public OzoneManagerProtocolClientSideTranslatorPB createOmClient(
      String omServiceID,
      String omHost,
      boolean forceHA
  ) throws Exception {
    return createOmClient(parent.getOzoneConf(), parent.getUser(), omServiceID, omHost, forceHA);
  }

  public static OzoneManagerProtocolClientSideTranslatorPB createOmClient(
      OzoneConfiguration conf,
      UserGroupInformation ugi,
      String omServiceID,
      String omHost,
      boolean forceHA
  ) throws IOException {
    if (omHost != null && !omHost.isEmpty()) {
      omServiceID = null;
      conf.set(OZONE_OM_ADDRESS_KEY, omHost);
    } else if (omServiceID == null || omServiceID.isEmpty()) {
      omServiceID = getTheOnlyConfiguredOmServiceIdOrThrow(conf);
    }
    RPC.setProtocolEngine(conf, OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);
    String clientId = ClientId.randomId().toString();
    if (!forceHA || (forceHA && OmUtils.isOmHAServiceId(conf, omServiceID))) {
      OmTransport omTransport = new Hadoop3OmTransportFactory()
          .createOmTransport(conf, ugi, omServiceID);
      return new OzoneManagerProtocolClientSideTranslatorPB(omTransport,
          clientId);
    } else {
      throw new OzoneClientException("This command works only on OzoneManager" +
            " HA cluster. Service ID specified does not match" +
            " with " + OZONE_OM_SERVICE_IDS_KEY + " defined in the " +
            "configuration. Configured " + OZONE_OM_SERVICE_IDS_KEY + " are " +
            conf.getTrimmedStringCollection(OZONE_OM_SERVICE_IDS_KEY) + "\n");
    }
  }

  private static String getTheOnlyConfiguredOmServiceIdOrThrow(OzoneConfiguration conf) {
    Collection<String> configuredServiceIds = getConfiguredServiceIds(conf);
    if (configuredServiceIds.size() != 1) {
      throw new IllegalArgumentException("There is no Ozone Manager service ID "
          + "specified, but there are either zero, or more than one service ID"
          + "configured.");
    }
    return configuredServiceIds.iterator().next();
  }

  private static Collection<String> getConfiguredServiceIds(OzoneConfiguration conf) {
    return conf.getTrimmedStringCollection(OZONE_OM_SERVICE_IDS_KEY);
  }
}
