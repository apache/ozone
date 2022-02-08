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

import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PERMISSION_DENIED;

/**
 * ozone tenant user revoke-admin.
 */
@CommandLine.Command(name = "revoke-admin",
    aliases = {"revokeadmin"},
    description = "Revoke admin role from accessIds in a tenant")
public class TenantRevokeAdminHandler extends TenantHandler {

  @CommandLine.Parameters(description = "List of accessIds", arity = "1..")
  private List<String> accessIds = new ArrayList<>();

  @CommandLine.Option(names = {"-t", "--tenant"},
      description = "Tenant name")
  private String tenantId;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) {
    final ObjectStore objStore = client.getObjectStore();

    for (final String accessId : accessIds) {
      try {
        // TODO: Make tenantRevokeAdmin return accessId, tenantName, user later.
        objStore.tenantRevokeAdmin(accessId, tenantId);
        err().println("Revoked admin role of '" + accessId +
            (tenantId != null ? "' from tenant '" + tenantId : "") + "'.");
      } catch (IOException e) {
        err().println("Failed to revoke admin role of '" + accessId +
            (tenantId != null ? "' from tenant '" + tenantId : "") + "'" +
            ": " + e.getMessage());
        if (e instanceof OMException) {
          final OMException omEx = (OMException) e;
          // Don't bother continuing the loop if current user isn't Ozone admin
          if (omEx.getResult().equals(PERMISSION_DENIED)) {
            break;
          }
        }
      }
    }
  }
}
