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

import java.io.IOException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.s3.S3Handler;

import picocli.CommandLine;

/**
 * Command to list users in a tenant along with corresponding accessId.
 */
@CommandLine.Command(name = "list",
    aliases = {"ls"},
    description = "List users in a tenant")
public class TenantListUsersHandler extends S3Handler {

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @CommandLine.Parameters(description = "Tenant name", arity = "1..1")
  private String tenantId;

  @CommandLine.Option(names = {"--prefix", "-p"},
      description = "Filter users with this prefix.")
  private String prefix;

  @CommandLine.Option(names = {"--json", "-j"},
      description = "Print detailed result in JSON")
  private boolean printJson;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    final TenantUserList usersInTenant =
        client.getObjectStore().listUsersInTenant(tenantId, prefix);

    if (!printJson) {
      usersInTenant.getUserAccessIds().forEach(accessIdInfo -> {
        out().println("- User '" + accessIdInfo.getUserPrincipal() +
            "' with accessId '" + accessIdInfo.getAccessId() + "'");
      });
    } else {
      final JsonArray resArray = new JsonArray();
      usersInTenant.getUserAccessIds().forEach(accessIdInfo -> {
        final JsonObject obj = new JsonObject();
        obj.addProperty("user", accessIdInfo.getUserPrincipal());
        obj.addProperty("accessId", accessIdInfo.getAccessId());
        resArray.add(obj);
      });
      final Gson gson = new GsonBuilder().setPrettyPrinting().create();
      out().println(gson.toJson(resArray));
    }

  }
}
