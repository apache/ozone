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

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ozone s3 user create.
 */
@CommandLine.Command(name = "create",
    description = "Create one or more tenant users")
public class TenantUserCreateHandler extends S3Handler {

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @CommandLine.Parameters(description = "List of tenant user short names")
  private List<String> usernames = new ArrayList<>();

  @CommandLine.Option(names = "-t",
      description = "Tenant name")
  private String tenantName;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) {
    final ObjectStore objStore = client.getObjectStore();
    if (tenantName == null || tenantName.length() == 0) {
      tenantName = objStore.getS3VolumeName();
    }
    if (usernames.size() > 0) {
      for (String username : usernames) {
        try {
          S3SecretValue res = objStore.createTenantUser(username, tenantName);
          out().println("Successfully created user " + username + ":");
          out().println("export AWS_ACCESS_KEY_ID=" + res.getAwsAccessKey());
          out().println("export AWS_SECRET_ACCESS_KEY=" + res.getAwsSecret());
        } catch (IOException e) {
          out().println("Failed to create user " + username + ": " +
              e.getMessage());
        }
      }
    } else {
      GenericCli.missingSubcommand(spec);
    }
  }
}
