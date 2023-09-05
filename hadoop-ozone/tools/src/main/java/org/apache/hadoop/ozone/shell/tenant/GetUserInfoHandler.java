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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.TenantUserInfoValue;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExtendedUserAccessIdInfo;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.jooq.tools.StringUtils;
import picocli.CommandLine;

import java.io.IOException;
import java.util.List;

/**
 * ozone tenant user info.
 */
@CommandLine.Command(name = "info",
    description = "Get tenant related information of a user")
public class GetUserInfoHandler extends TenantHandler {

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @CommandLine.Parameters(description = "User name (principal)", arity = "1..1")
  private String userPrincipal;

  @CommandLine.Option(names = {"--json", "-j"},
      description = "Print result in JSON")
  private boolean printJson;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    if (StringUtils.isEmpty(userPrincipal)) {
      GenericCli.missingSubcommand(spec);
      return;
    }

    final TenantUserInfoValue tenantUserInfo =
        client.getObjectStore().tenantGetUserInfo(userPrincipal);
    final List<ExtendedUserAccessIdInfo> accessIdInfoList =
        tenantUserInfo.getAccessIdInfoList();
    if (accessIdInfoList.size() == 0) {
      err().println("User '" + userPrincipal +
          "' is not assigned to any tenant.");
      return;
    }

    if (!printJson) {
      out().println("User '" + userPrincipal + "' is assigned to:");
      accessIdInfoList.forEach(accessIdInfo -> {
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
      });
    } else {

      final JsonObject resObj = new JsonObject();
      resObj.addProperty("user", userPrincipal);

      final JsonArray arr = new JsonArray();
      accessIdInfoList.forEach(accessIdInfo -> {
        final JsonObject tenantObj = new JsonObject();
        tenantObj.addProperty("accessId", accessIdInfo.getAccessId());
        tenantObj.addProperty("tenantId", accessIdInfo.getTenantId());
        tenantObj.addProperty("isAdmin", accessIdInfo.getIsAdmin());
        tenantObj.addProperty("isDelegatedAdmin",
            accessIdInfo.getIsDelegatedAdmin());
        arr.add(tenantObj);
      });

      resObj.add("tenants", arr);

      final Gson gson = new GsonBuilder().setPrettyPrinting().create();
      out().println(gson.toJson(resObj));
    }

  }
}
