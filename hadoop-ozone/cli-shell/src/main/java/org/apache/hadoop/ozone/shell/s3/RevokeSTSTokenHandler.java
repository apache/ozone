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
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Executes revocation of STS tokens.
 *
 * <p>This command marks the specified STS token as revoked by adding it to the OM's revoked STS token table.
 * Subsequent S3 requests using the same session token will be rejected once the revocation
 * state has propagated.</p>
 */
@Command(name = "revokeststoken",
    description = "Revoke S3 STS token for the given session token")
public class RevokeSTSTokenHandler extends S3Handler {

  @Option(names = "-t",
      required = true,
      description = "STS session token")
  private String sessionToken;

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

    if (!yes) {
      out().print("Enter 'y' to confirm STS token revocation for sessionToken '" +
          sessionToken + "': ");
      out().flush();
      final Scanner scanner = new Scanner(new InputStreamReader(System.in, StandardCharsets.UTF_8));
      final String confirmation = scanner.next().trim().toLowerCase();
      if (!"y".equals(confirmation)) {
        out().println("Revoke STS token operation cancelled.");
        return;
      }
    }

    client.getObjectStore().revokeSTSToken(sessionToken);
    out().println("STS token revoked for sessionToken '" + sessionToken + "'.");
  }
}
