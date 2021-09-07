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
package org.apache.hadoop.ozone.shell.s3;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.cli.GenericCli;
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
 * ozone s3 user assign.
 */
@CommandLine.Command(name = "assign",
    description = "Assign user to tenant")
public class AssignUserToTenantHandler extends S3Handler {

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @CommandLine.Parameters(description = "List of user Kerberos principal(s)")
  private List<String> principals = new ArrayList<>();

  @CommandLine.Option(names = {"-t", "--tenant"},
      description = "Tenant name")
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

  // TODO: support dry-run?
//  @CommandLine.Option(names = {"--dry-run"},
//      description = "Dry-run")
//  private boolean dryRun;

  private boolean isEmptyList(List<String> list) {
    return list == null || list.size() == 0;
  }

  private String getDefaultAccessId(String principal) {
    return tenantName + TENANT_NAME_USER_NAME_DELIMITER + principal;
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) {
    final ObjectStore objStore = client.getObjectStore();

    if (isEmptyList(principals)) {
      GenericCli.missingSubcommand(spec);
      return;
    }

    if (StringUtils.isEmpty(tenantName)) {
      err().println("Please specify a tenant name with -t.");
      return;
    }

    if (StringUtils.isEmpty(accessId)) {
      accessId = getDefaultAccessId(principals.get(0));
    } else if (principals.size() > 1) {
      err().println("Manually specifying accessId is only supported when there "
          + "is one user principal in the command line. Reduce the number of "
          + "principal to one and try again.");
      return;
    }

    for (int i = 0; i < principals.size(); i++) {
      final String principal = principals.get(i);
      try {
        if (i >= 1) {
          accessId = getDefaultAccessId(principal);
        }
        final S3SecretValue resp =
            objStore.assignUserToTenant(principal, tenantName, accessId);
        err().println("Assigned '" + principal + "' to '" + tenantName +
            "' under accessId '" + accessId + "'.");
        out().println("export AWS_ACCESS_KEY_ID=" + resp.getAwsAccessKey());
        out().println("export AWS_SECRET_ACCESS_KEY=" + resp.getAwsSecret());
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
