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
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.security.UserGroupInformation;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * ozone s3 setsecret.
 */
@Command(name = "setsecret", aliases = "set-secret",
    description = "Set s3 secret for current user")
public class SetS3SecretHandler extends S3Handler {

  @Option(names = "-u",
      description = "Specify the user to perform the operation on "
          + "(Admins only)'")
  private String username;

  @CommandLine.Option(names = {"-s", "--secret", "--secretKey"},
      description = "Secret key", required = true)
  private String secretKey;

  @Option(names = "-e",
      description = "Print out variables together with 'export' prefix, to "
          + "use it from 'eval $(ozone s3 setsecret)'")
  private boolean export;

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

    final S3SecretValue accessIdSecretKeyPair =
        client.getObjectStore().setS3Secret(username, secretKey);
    if (export) {
      out().println("export AWS_ACCESS_KEY_ID='" +
          accessIdSecretKeyPair.getAwsAccessKey() + "'");
      out().println("export AWS_SECRET_ACCESS_KEY='" +
          accessIdSecretKeyPair.getAwsSecret() + "'");
    } else {
      out().println(accessIdSecretKeyPair);
    }
  }

}
