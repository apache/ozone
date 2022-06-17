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
import com.google.gson.JsonObject;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

import java.io.IOException;

/**
 * ozone tenant user assign-admin.
 *
 * User must already be assigned to the tenant, will be rejected otherwise.
 */
@CommandLine.Command(name = "assign-admin",
    aliases = {"assignadmin"},
    description = "Assign admin role to accessIds in a tenant")
public class TenantAssignAdminHandler extends TenantHandler {

  @CommandLine.Parameters(description = "Access ID", arity = "1..1")
  private String accessId;

  @CommandLine.Option(names = {"-t", "--tenant"},
      description = "Tenant name")
  private String tenantId;

  @CommandLine.Option(names = {"-d", "--delegated"}, defaultValue = "false",
      description = "Assign delegated admin. If unspecified, assign "
          + "non-delegated admin (the default)")
  private boolean delegated;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    client.getObjectStore().tenantAssignAdmin(accessId, tenantId, delegated);

    if (isVerbose()) {
      final JsonObject obj = new JsonObject();
      obj.addProperty("accessId", accessId);
      obj.addProperty("tenantId", tenantId);
      obj.addProperty("isAdmin", true);
      obj.addProperty("isDelegatedAdmin", delegated);
      final Gson gson = new GsonBuilder().setPrettyPrinting().create();
      out().println(gson.toJson(obj));
    }

  }
}
