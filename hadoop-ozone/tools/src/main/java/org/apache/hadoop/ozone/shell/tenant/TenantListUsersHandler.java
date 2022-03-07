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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantUserAccessId;
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

  @CommandLine.Option(names = {"-t", "--tenant"},
      description = "Tenant name")
  private String tenantId;

  @CommandLine.Option(names = {"-p", "--prefix"},
      description = "Filter users with this prefix.")
  private String prefix;

  // TODO: HDDS-6340. Add an option to print JSON result

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) {
    final ObjectStore objStore = client.getObjectStore();

    if (StringUtils.isEmpty(tenantId)) {
      err().println("Please specify a tenant name with -t.");
      return;
    }
    try {
      TenantUserList usersInTenant =
          objStore.listUsersInTenant(tenantId, prefix);
      for (TenantUserAccessId accessIdInfo : usersInTenant.getUserAccessIds()) {
        out().println("- User '" + accessIdInfo.getUserPrincipal() +
            "' with accessId '" + accessIdInfo.getAccessId() + "'");
      }
    } catch (IOException e) {
      err().println("Failed to Get Users in tenant '" + tenantId
          + "': " + e.getMessage());
    }
  }
}
