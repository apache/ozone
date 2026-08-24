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

import static org.apache.hadoop.ozone.OzoneConsts.TENANT_ID_USERNAME_DELIMITER;

import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

/**
 * ozone tenant user assign.
 */
@CommandLine.Command(name = "assign",
    description = "Assign user accessId to tenant")
public class TenantAssignUserAccessIdHandler extends TenantHandler {

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @CommandLine.Parameters(description = "User name", arity = "1..1")
  private String userPrincipal;

  @CommandLine.Option(names = {"-t", "--tenant"},
      description = "Tenant name", required = true)
  private String tenantId;

  @CommandLine.Option(names = {"-a", "--access-id", "--accessId"},
      description = "(Optional) Specify the accessId for user in this tenant. "
          + "If unspecified, accessId would be in the form of "
          + "TenantName$Principal.",
      hidden = true)
  // This option is intentionally hidden for now. Because if accessId isn't
  //  restricted in any way this might cause `ozone s3 getsecret` to
  //  unintentionally leak secret if an admin isn't careful.
  private String accessId;

  private String getDefaultAccessId(String userPrinc) {
    return tenantId + TENANT_ID_USERNAME_DELIMITER + userPrinc;
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    if (StringUtils.isEmpty(accessId)) {
      accessId = getDefaultAccessId(userPrincipal);
    }

    final S3SecretValue resp = client.getObjectStore()
        .tenantAssignUserAccessId(userPrincipal, tenantId, accessId);

    out().println(
        "export AWS_ACCESS_KEY_ID='" + resp.getAwsAccessKey() + "'");
    out().println(
        "export AWS_SECRET_ACCESS_KEY='" + resp.getAwsSecret() + "'");

    if (isVerbose()) {
      err().println("Assigned '" + userPrincipal + "' to '" + tenantId +
          "' with accessId '" + accessId + "'.");
    }

  }
}
