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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

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
      ObjectNode obj = JsonUtils.createObjectNode(null);
      obj.put("accessId", accessId);
      obj.put("tenantId", tenantId);
      obj.put("isAdmin", true);
      obj.put("isDelegatedAdmin", delegated);

      String jsonString = JsonUtils.toJsonStringWithDefaultPrettyPrinter(obj);
      out().println(jsonString);
    }
  }
}
