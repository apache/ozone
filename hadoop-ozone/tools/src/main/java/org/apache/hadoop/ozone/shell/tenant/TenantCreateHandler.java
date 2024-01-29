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
import org.apache.hadoop.ozone.client.TenantArgs;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

import java.io.IOException;

/**
 * ozone tenant create.
 */
@CommandLine.Command(name = "create",
    description = "Create a tenant."
        + " This can create a new Ozone volume for the tenant.")
public class TenantCreateHandler extends TenantHandler {

  @CommandLine.Parameters(description = "Tenant name", arity = "1..1")
  private String tenantId;

  @CommandLine.Option(names = {"-f", "--force"},
      description = "(Optional) Force tenant creation even when volume exists. "
          + "This does NOT override other errors like Ranger failure.",
      hidden = true)
  // This option is intentionally hidden to avoid abuse.
  private boolean forceCreationWhenVolumeExists;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    final TenantArgs tenantArgs = TenantArgs.newBuilder()
        .setVolumeName(tenantId)
        .setForceCreationWhenVolumeExists(forceCreationWhenVolumeExists)
        .build();

    client.getObjectStore().createTenant(tenantId, tenantArgs);
    // RpcClient#createTenant prints INFO level log of tenant and volume name

    if (isVerbose()) {
      final JsonObject obj = new JsonObject();
      obj.addProperty("tenantId", tenantId);
      final Gson gson = new GsonBuilder().setPrettyPrinting().create();
      out().println(gson.toJson(obj));
    }

  }
}
