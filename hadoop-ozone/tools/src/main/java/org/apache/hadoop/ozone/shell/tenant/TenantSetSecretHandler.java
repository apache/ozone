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
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

import java.io.IOException;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.ACCESSID_NOT_FOUND;

/**
 * ozone tenant user set-secret.
 */
@CommandLine.Command(name = "set-secret",
    aliases = {"setsecret"},
    description = "Set secret for a tenant user accessId.")
public class TenantSetSecretHandler extends TenantHandler {

  @CommandLine.Parameters(description = "AccessId", arity = "1")
  private String accessId;

  @CommandLine.Option(names = {"-s", "--secret"},
          description = "Secret key", required = true)
  private String secretKey;

  @CommandLine.Option(names = {"-e", "--export"},
          description = "Print out variables together with 'export' prefix")
  private boolean export;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
          throws IOException {

    final ObjectStore objectStore = client.getObjectStore();

    try {
      final S3SecretValue accessIdSecretKeyPair =
              objectStore.setS3Secret(accessId, secretKey);
      if (export) {
        out().println("export AWS_ACCESS_KEY_ID='" +
                accessIdSecretKeyPair.getAwsAccessKey() + "'");
        out().println("export AWS_SECRET_ACCESS_KEY='" +
                accessIdSecretKeyPair.getAwsSecret() + "'");
      } else {
        out().println(accessIdSecretKeyPair);
      }
    } catch (OMException omEx) {
      if (omEx.getResult().equals(ACCESSID_NOT_FOUND)) {
        // Print to stderr here in order not to contaminate stdout just in
        // case -e is specified.
        err().println("AccessId '" + accessId + "' doesn't exist");
      } else {
        throw omEx;
      }
    }
  }
}
