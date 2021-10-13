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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.ozone.OzoneConsts.TENANT_NAME_USER_NAME_DELIMITER;

/**
 * ozone tenant user assign.
 */
@CommandLine.Command(name = "assign",
    description = "Assign user accessId to tenant")
public class TenantAssignUserAccessIdHandler extends TenantHandler {

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @CommandLine.Parameters(description = "List of user principals",
      arity = "1..")
  private List<String> userPrincipals = new ArrayList<>();

  @CommandLine.Option(names = {"-t", "--tenant"},
      description = "Tenant name", required = true)
  private String tenantName;

  @CommandLine.Option(names = {"-a", "--access-id", "--accessId"},
      description = "(Optional) Specify the accessId for user in this tenant. "
          + "If unspecified, accessId would be in the form of "
          + "TenantName$Principal.",
      hidden = true)
  // This option is intentionally hidden for now. Because accessId isn't
  //  restricted in any way so far and this could cause some conflict with
  //  `s3 getsecret` and leak the secret if an admin isn't careful.
  private String accessId;

  private String getDefaultAccessId(String principal) {
    return tenantName + TENANT_NAME_USER_NAME_DELIMITER + principal;
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) {
    final ObjectStore objStore = client.getObjectStore();

    if (StringUtils.isEmpty(accessId)) {
      accessId = getDefaultAccessId(userPrincipals.get(0));
    } else if (userPrincipals.size() > 1) {
      err().println("Manually specifying accessId is only supported when there "
          + "is one user principal in the command line. Reduce the number of "
          + "principal to one and try again.");
      return;
    }

    for (int i = 0; i < userPrincipals.size(); i++) {
      final String principal = userPrincipals.get(i);
      try {
        if (i >= 1) {
          accessId = getDefaultAccessId(principal);
        }
        final S3SecretValue resp =
            objStore.tenantAssignUserAccessId(principal, tenantName, accessId);
        err().println("Assigned '" + principal + "' to '" + tenantName +
            "' with accessId '" + accessId + "'.");
        out().println("export AWS_ACCESS_KEY_ID='" +
            resp.getAwsAccessKey() + "'");
        out().println("export AWS_SECRET_ACCESS_KEY='" +
            resp.getAwsSecret() + "'");
      } catch (IOException e) {
        err().println("Failed to assign '" + principal + "' to '" +
            tenantName + "': " + e.getMessage());
        if (e instanceof OMException) {
          final OMException omException = (OMException) e;
          if (omException.getResult().equals(
              OMException.ResultCodes.TENANT_NOT_FOUND)) {
            // If tenant does not exist, don't bother continuing the loop
            break;
          }
        }
      }
    }
  }
}
