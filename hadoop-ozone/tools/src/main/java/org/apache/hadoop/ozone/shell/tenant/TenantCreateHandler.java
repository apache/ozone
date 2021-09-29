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

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ozone tenant create.
 */
@CommandLine.Command(name = "create",
    description = "Create one or more tenants")
public class TenantCreateHandler extends TenantHandler {

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @CommandLine.Parameters(description = "List of tenant names")
  private List<String> tenants = new ArrayList<>();

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) {
    if (tenants.size() > 0) {
      for (String tenantName : tenants) {
        try {
          client.getObjectStore().createTenant(tenantName);
          out().println("Created tenant '" + tenantName + "'.");
        } catch (IOException e) {
          out().println("Failed to create tenant '" + tenantName + "': " +
              e.getMessage());
        }
      }
    } else {
      GenericCli.missingSubcommand(spec);
    }
  }
}
