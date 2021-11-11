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
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.ACCESSID_NOT_FOUND;

/**
 * ozone tenant user get-secret.
 */
@CommandLine.Command(name = "get-secret",
    aliases = {"getsecret"},
    description = "Get secret for tenant user accessIds. " +
        "This differs from `ozone s3 getsecret` that this would not " +
        "generate a key/secret pair when the accessId doesn't exist.")
public class TenantGetSecretHandler extends TenantHandler {

  @CommandLine.Parameters(description = "List of accessIds", arity = "1..")
  private List<String> accessIds = new ArrayList<>();

  @CommandLine.Option(names = "-e",
      description = "Print out variables together with 'export' prefix")
  private boolean export;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {
    final ObjectStore objectStore = client.getObjectStore();

    for (final String accessId : accessIds) {

      try {
        final S3SecretValue secret =
            objectStore.getS3Secret(accessId, false);
        if (export) {
          out().println("export AWS_ACCESS_KEY_ID='" +
              secret.getAwsAccessKey() + "'");
          out().println("export AWS_SECRET_ACCESS_KEY='" +
              secret.getAwsSecret() + "'");
        } else {
          out().println(secret);
        }
      } catch (OMException omEx) {
        if (omEx.getResult().equals(ACCESSID_NOT_FOUND)) {
          // Print to stderr here in order not to contaminate stdout just in
          // case -e is specified.
          err().println("AccessId '" + accessId + "' doesn't exist");
          // Continue the loop if it's just ACCESSID_NOT_FOUND
        } else {
          throw omEx;
        }
      }

    }
  }
}
