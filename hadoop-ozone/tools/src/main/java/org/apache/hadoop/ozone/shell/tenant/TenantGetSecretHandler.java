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
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

import java.io.IOException;

/**
 * ozone tenant user get-secret.
 */
@CommandLine.Command(name = "get-secret",
    aliases = {"getsecret"},
    description = "Get secret given tenant user accessId. " +
        "This differs from `ozone s3 getsecret` that this would not " +
        "generate secret when the given access ID doesn't exist.")
public class TenantGetSecretHandler extends TenantHandler {

  @CommandLine.Parameters(description = "Access ID", arity = "1..1")
  private String accessId;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    final S3SecretValue accessIdSecretKeyPair =
        client.getObjectStore().getS3Secret(accessId, false);
    // Always print in export format
    out().printf("export AWS_ACCESS_KEY_ID='%s'%n",
        accessIdSecretKeyPair.getAwsAccessKey());
    out().printf("export AWS_SECRET_ACCESS_KEY='%s'%n",
        accessIdSecretKeyPair.getAwsSecret());

  }
}
