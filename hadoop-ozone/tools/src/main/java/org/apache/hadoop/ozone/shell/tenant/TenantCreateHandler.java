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

import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

import java.io.IOException;

/**
 * ozone tenant create.
 */
@CommandLine.Command(name = "create",
    description = "Create a tenant."
        + " This will also create a new Ozone volume for the tenant.")
public class TenantCreateHandler extends TenantHandler {

  @CommandLine.Parameters(description = "Tenant name", arity = "1..1")
  private String tenantId;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {
    try {
      client.getObjectStore().createTenant(tenantId);
      // TODO: Add return value and print volume name?
      out().println("Created tenant '" + tenantId + "'.");
    } catch (IOException e) {
      // Throw exception to make client exit code non-zero
      throw new IOException("Failed to create tenant '" + tenantId + "'", e);
    }
  }
}
