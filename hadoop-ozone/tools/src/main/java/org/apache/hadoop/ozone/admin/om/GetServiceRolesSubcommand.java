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

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRoleInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Handler of om roles command.
 */
@CommandLine.Command(
    name = "roles", aliases = "getserviceroles",
    description = "List all OMs and their respective Ratis server roles",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class GetServiceRolesSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(names = {"-id", "--service-id"},
      description = "OM Service ID",
      required = true)
  private String omServiceId;

  private OzoneManagerProtocol ozoneManagerClient;

  @Override
  public Void call() throws Exception {
    try {
      ozoneManagerClient =  parent.createOmClient(omServiceId);
      getOmServerRoles(ozoneManagerClient.getServiceList());
    } catch (OzoneClientException ex) {
      System.out.printf("Error: %s", ex.getMessage());
    } finally {
      if (ozoneManagerClient != null) {
        ozoneManagerClient.close();
      }
    }
    return null;
  }

  private void getOmServerRoles(List<ServiceInfo> serviceList) {
    for (ServiceInfo serviceInfo : serviceList) {
      OMRoleInfo omRoleInfo = serviceInfo.getOmRoleInfo();
      if (omRoleInfo != null &&
          serviceInfo.getNodeType() == HddsProtos.NodeType.OM) {
        System.out.println(
            serviceInfo.getOmRoleInfo().getNodeId() + " : " +
                serviceInfo.getOmRoleInfo().getServerRole() + " (" +
                serviceInfo.getHostname() + ")");
      }
    }
  }
}
