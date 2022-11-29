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
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.TenantStateList;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

import java.io.IOException;

/**
 * ozone tenant list.
 */
@CommandLine.Command(name = "list",
    aliases = {"ls"},
    description = "List tenants")
public class TenantListHandler extends TenantHandler {

  @CommandLine.Option(names = {"--json", "-j"},
      description = "Print detailed result in JSON")
  private boolean printJson;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    TenantStateList tenantStateList = client.getObjectStore().listTenant();

    if (!printJson) {
      tenantStateList.getTenantStateList().forEach(tenantState ->
          out().println(tenantState.getTenantId()));
    } else {
      final JsonArray resArray = new JsonArray();
      tenantStateList.getTenantStateList().forEach(tenantState -> {
        final JsonObject obj = new JsonObject();
        obj.addProperty("tenantId", tenantState.getTenantId());
        obj.addProperty("bucketNamespaceName",
            tenantState.getBucketNamespaceName());
        obj.addProperty("userRoleName", tenantState.getUserRoleName());
        obj.addProperty("adminRoleName", tenantState.getAdminRoleName());
        obj.addProperty("bucketNamespacePolicyName",
            tenantState.getBucketNamespacePolicyName());
        obj.addProperty("bucketPolicyName",
            tenantState.getBucketPolicyName());
        resArray.add(obj);
      });
      final Gson gson = new GsonBuilder().setPrettyPrinting().create();
      out().println(gson.toJson(resArray));
    }

  }
}
