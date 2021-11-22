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
import java.util.ArrayList;
import java.util.List;

/**
 * ozone tenant delete.
 */
@CommandLine.Command(name = "delete", aliases = "remove",
    description = "Delete one or more empty tenants")
public class TenantDeleteHandler extends TenantHandler {

  @CommandLine.Parameters(description = "List of tenant names", arity = "1..")
  private final List<String> tenants = new ArrayList<>();

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) {
    for (final String tenantId : tenants) {
      try {
        client.getObjectStore().deleteTenant(tenantId);
        out().println("Deleted tenant '" + tenantId + "'.");
      } catch (IOException e) {
        err().println("Failed to delete tenant '" + tenantId + "': " +
            e.getMessage());
      }
    }
  }
}
