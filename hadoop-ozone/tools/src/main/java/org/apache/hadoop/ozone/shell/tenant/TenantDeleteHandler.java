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
import org.apache.hadoop.ozone.om.helpers.DeleteTenantState;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

import java.io.IOException;

/**
 * ozone tenant delete.
 */
@CommandLine.Command(name = "delete", aliases = "remove",
    description = "Delete an empty tenant. "
        + "Will not remove the associated volume.")
public class TenantDeleteHandler extends TenantHandler {

  @CommandLine.Parameters(description = "Tenant name", arity = "1..1")
  private String tenantId;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    final DeleteTenantState resp =
        client.getObjectStore().deleteTenant(tenantId);

    err().println("Deleted tenant '" + tenantId + "'.");
    long volumeRefCount = resp.getVolRefCount();
    assert (volumeRefCount >= 0L);
    final String volumeName = resp.getVolumeName();
    final String extraPrompt =
        "But the associated volume '" + volumeName + "' is not removed. ";
    if (volumeRefCount == 0L) {
      err().println(extraPrompt + "To delete it, run"
          + "\n    ozone sh volume delete " + volumeName + "\n");
    } else {
      err().println(extraPrompt + "And it is still referenced by some "
          + "other Ozone features (refCount is " + volumeRefCount + ").");
    }

    if (isVerbose()) {
      final JsonObject obj = new JsonObject();
      obj.addProperty("tenantId", tenantId);
      obj.addProperty("volumeName", resp.getVolumeName());
      obj.addProperty("volumeRefCount", resp.getVolRefCount());
      final Gson gson = new GsonBuilder().setPrettyPrinting().create();
      // Print raw response to stderr if verbose
      out().println(gson.toJson(obj));
    }

  }
}
