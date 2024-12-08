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

package org.apache.hadoop.ozone.repair.quota;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.RepairSubcommand;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.om.protocolPB.Hadoop3OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.repair.OzoneRepair;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.ClientId;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;

/**
 * Ozone Repair CLI for quota.
 */
@CommandLine.Command(name = "quota",
    subcommands = {
        QuotaStatus.class,
        QuotaTrigger.class,
    },
    description = "Operational tool to repair quota in OM DB.")
@MetaInfServices(RepairSubcommand.class)
public class QuotaRepair implements Callable<Void>, RepairSubcommand {

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @CommandLine.ParentCommand
  private OzoneRepair parent;

  @Override
  public Void call() {
    GenericCli.missingSubcommand(spec);
    return null;
  }

  public OzoneManagerProtocolClientSideTranslatorPB createOmClient(
      String omServiceID,
      String omHost,
      boolean forceHA
  ) throws Exception {
    OzoneConfiguration conf = parent.getOzoneConf();
    if (omHost != null && !omHost.isEmpty()) {
      omServiceID = null;
      conf.set(OZONE_OM_ADDRESS_KEY, omHost);
    } else if (omServiceID == null || omServiceID.isEmpty()) {
      omServiceID = getTheOnlyConfiguredOmServiceIdOrThrow();
    }
    RPC.setProtocolEngine(conf, OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);
    String clientId = ClientId.randomId().toString();
    if (!forceHA || (forceHA && OmUtils.isOmHAServiceId(conf, omServiceID))) {
      OmTransport omTransport = new Hadoop3OmTransportFactory()
          .createOmTransport(conf, getUser(), omServiceID);
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

  private String getTheOnlyConfiguredOmServiceIdOrThrow() {
    if (getConfiguredServiceIds().size() != 1) {
      throw new IllegalArgumentException("There is no Ozone Manager service ID "
          + "specified, but there are either zero, or more than one service ID"
          + "configured.");
    }
    return getConfiguredServiceIds().iterator().next();
  }

  private Collection<String> getConfiguredServiceIds() {
    OzoneConfiguration conf = parent.getOzoneConf();
    Collection<String> omServiceIds =
        conf.getTrimmedStringCollection(OZONE_OM_SERVICE_IDS_KEY);
    return omServiceIds;
  }

  public UserGroupInformation getUser() throws IOException {
    return UserGroupInformation.getCurrentUser();
  }

  protected OzoneRepair getParent() {
    return parent;
  }
}
