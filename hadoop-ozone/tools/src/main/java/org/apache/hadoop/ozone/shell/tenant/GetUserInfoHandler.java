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
package org.apache.hadoop.ozone.shell.tenant;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.TenantUserInfoValue;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExtendedUserAccessIdInfo;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ozone tenant user info.
 */
@CommandLine.Command(name = "info",
    description = "Get tenant related information of a user")
public class GetUserInfoHandler extends TenantHandler {

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @CommandLine.Parameters(description = "List of user principal(s)")
  private List<String> userPrincipals = new ArrayList<>();

  // TODO: HDDS-6340. Add an option to print JSON result

  private boolean isEmptyList(List<String> list) {
    return list == null || list.size() == 0;
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) {
    final ObjectStore objStore = client.getObjectStore();

    if (isEmptyList(userPrincipals)) {
      GenericCli.missingSubcommand(spec);
      return;
    }

    for (final String userPrincipal : userPrincipals) {
      try {
        final TenantUserInfoValue tenantUserInfo =
            objStore.tenantGetUserInfo(userPrincipal);
        List<ExtendedUserAccessIdInfo> accessIdInfoList =
            tenantUserInfo.getAccessIdInfoList();
        if (accessIdInfoList.size() == 0) {
          err().println("User '" + userPrincipal +
              "' is not assigned to any tenant.");
          continue;
        }
        out().println("User '" + userPrincipal + "' is assigned to:");

        for (ExtendedUserAccessIdInfo accessIdInfo : accessIdInfoList) {
          // Get admin info
          final String adminInfoString;
          if (accessIdInfo.getIsAdmin()) {
            adminInfoString = accessIdInfo.getIsDelegatedAdmin() ?
                " delegated admin" : " admin";
          } else {
            adminInfoString = "";
          }
          out().format("- Tenant '%s'%s with accessId '%s'%n",
              accessIdInfo.getTenantId(),
              adminInfoString,
              accessIdInfo.getAccessId());
        }

      } catch (IOException e) {
        err().println("Failed to GetUserInfo of user '" + userPrincipal
            + "': " + e.getMessage());
      }
    }
  }
}
