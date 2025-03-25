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
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.TenantStateList;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

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
      ArrayNode resArray = JsonUtils.createArrayNode();
      tenantStateList.getTenantStateList().forEach(tenantState -> {
        ObjectNode obj = JsonUtils.createObjectNode(null);
        obj.put("tenantId", tenantState.getTenantId());
        obj.put("bucketNamespaceName", tenantState.getBucketNamespaceName());
        obj.put("userRoleName", tenantState.getUserRoleName());
        obj.put("adminRoleName", tenantState.getAdminRoleName());
        obj.put("bucketNamespacePolicyName",
            tenantState.getBucketNamespacePolicyName());
        obj.put("bucketPolicyName", tenantState.getBucketPolicyName());
        resArray.add(obj);
      });
      // Serialize and print the JSON string with pretty printing
      String jsonString = JsonUtils.toJsonStringWithDefaultPrettyPrinter(resArray);
      out().println(jsonString);
    }
  }
}
