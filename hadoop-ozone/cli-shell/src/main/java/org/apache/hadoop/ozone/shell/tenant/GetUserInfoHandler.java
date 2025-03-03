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

package org.apache.hadoop.ozone.shell.tenant;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.TenantUserInfoValue;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExtendedUserAccessIdInfo;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

/**
 * ozone tenant user info.
 */
@CommandLine.Command(name = "info",
    description = "Get tenant related information of a user")
public class GetUserInfoHandler extends TenantHandler {

  @CommandLine.Parameters(description = "User name (principal)", arity = "1..1")
  private String userPrincipal;

  @CommandLine.Option(names = {"--json", "-j"},
      description = "Print result in JSON")
  private boolean printJson;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    final TenantUserInfoValue tenantUserInfo =
        client.getObjectStore().tenantGetUserInfo(userPrincipal);
    final List<ExtendedUserAccessIdInfo> accessIdInfoList =
        tenantUserInfo.getAccessIdInfoList();
    if (accessIdInfoList.isEmpty()) {
      err().println("User '" + userPrincipal +
          "' is not assigned to any tenant.");
      return;
    }

    if (!printJson) {
      out().println("User '" + userPrincipal + "' is assigned to:");
      accessIdInfoList.forEach(accessIdInfo -> {
        final String adminInfoString = accessIdInfo.getIsAdmin() ?
            (accessIdInfo.getIsDelegatedAdmin() ? " delegated admin" :
                " admin") : "";
        out().format("- Tenant '%s'%s with accessId '%s'%n",
            accessIdInfo.getTenantId(),
            adminInfoString,
            accessIdInfo.getAccessId());
      });
    } else {
      ObjectNode resObj = JsonUtils.createObjectNode(null);
      resObj.put("user", userPrincipal);

      ArrayNode arr = JsonUtils.createArrayNode();
      accessIdInfoList.forEach(accessIdInfo -> {
        ObjectNode tenantObj = JsonUtils.createObjectNode(null);
        tenantObj.put("accessId", accessIdInfo.getAccessId());
        tenantObj.put("tenantId", accessIdInfo.getTenantId());
        tenantObj.put("isAdmin", accessIdInfo.getIsAdmin());
        tenantObj.put("isDelegatedAdmin", accessIdInfo.getIsDelegatedAdmin());
        arr.add(tenantObj);
      });

      resObj.set("tenants", arr);
      String prettyJson =
          JsonUtils.toJsonStringWithDefaultPrettyPrinter(resObj);
      out().println(prettyJson);
    }

  }
}
