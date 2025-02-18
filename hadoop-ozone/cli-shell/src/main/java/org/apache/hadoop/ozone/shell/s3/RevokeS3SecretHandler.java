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

package org.apache.hadoop.ozone.shell.s3;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.security.UserGroupInformation;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Executes revokesecret calls.
 */
@Command(name = "revokesecret",
    description = "Revoke s3 secret for current user")
public class RevokeS3SecretHandler extends S3Handler {

  @Option(names = "-u",
      description = "Specify the user name to perform the operation on "
          + "(admins only)'")
  private String username;

  @Option(names = "-y",
      description = "Continue without interactive user confirmation")
  private boolean yes;

  @Override
  protected boolean isApplicable() {
    return securityEnabled();
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {
    if (username == null || username.isEmpty()) {
      username = UserGroupInformation.getCurrentUser().getUserName();
    }

    if (!yes) {
      // Ask for user confirmation
      out().print("Enter 'y' to confirm S3 secret revocation for '" +
          username + "': ");
      out().flush();
      Scanner scanner = new Scanner(new InputStreamReader(
          System.in, StandardCharsets.UTF_8));
      String confirmation = scanner.next().trim().toLowerCase();
      if (!confirmation.equals("y")) {
        out().println("Operation cancelled.");
        return;
      }
    }

    client.getObjectStore().revokeS3Secret(username);
    out().println("S3 secret revoked.");
  }
}
