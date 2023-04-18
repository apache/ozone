/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.ozone.s3secret;

import org.apache.hadoop.ozone.om.helpers.S3SecretValue;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.io.IOException;

/**
 * Endpoint to generate and return S3 secret.
 */
@Path("/secret/generate")
@S3SecretEnabled
public class S3SecretGenerateEndpoint extends S3SecretEndpointBase {

  @GET
  public Response get() throws IOException {
    S3SecretResponse s3SecretResponse = new S3SecretResponse();
    S3SecretValue s3SecretValue = generateS3Secret();
    s3SecretResponse.setAwsSecret(s3SecretValue.getAwsSecret());
    s3SecretResponse.setAwsAccessKey(s3SecretValue.getAwsAccessKey());
    return Response.ok(s3SecretResponse).build();
  }

  private S3SecretValue generateS3Secret() throws IOException {
    return getClient().getObjectStore().getS3Secret(shortNameFromRequest());
  }
}
