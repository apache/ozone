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

import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import java.io.IOException;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;

/**
 * Endpoint to manage S3 secret.
 */
@Path("/secret")
@S3SecretEnabled
public class S3SecretManagementEndpoint extends S3SecretEndpointBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(S3SecretManagementEndpoint.class);

  @PUT
  public Response generate() throws IOException {
    return generateInternal(null);
  }

  @PUT
  @Path("/{username}")
  public Response generate(@PathParam("username") String username)
      throws IOException {
    return generateInternal(username);
  }

  private Response generateInternal(@Nullable String username)
      throws IOException {
    S3SecretResponse s3SecretResponse = new S3SecretResponse();
    S3SecretValue s3SecretValue = generateS3Secret(username);
    s3SecretResponse.setAwsSecret(s3SecretValue.getAwsSecret());
    s3SecretResponse.setAwsAccessKey(s3SecretValue.getAwsAccessKey());
    AUDIT.logReadSuccess(buildAuditMessageForSuccess(
        S3GAction.GENERATE_SECRET, getAuditParameters()));
    return Response.ok(s3SecretResponse).build();
  }

  private S3SecretValue generateS3Secret(@Nullable String username)
      throws IOException {
    String actualUsername = username == null ? userNameFromRequest() : username;
    return getClient().getObjectStore().getS3Secret(actualUsername);
  }

  @DELETE
  public Response revoke() throws IOException {
    return revokeInternal(null);
  }

  @DELETE
  @Path("/{username}")
  public Response revoke(@PathParam("username") String username)
      throws IOException {
    return revokeInternal(username);
  }

  private Response revokeInternal(@Nullable String username)
      throws IOException {
    try {
      revokeSecret(username);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          S3GAction.REVOKE_SECRET, getAuditParameters()));
      return Response.ok().build();
    } catch (OMException e) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          S3GAction.REVOKE_SECRET, getAuditParameters(), e));
      if (e.getResult() == OMException.ResultCodes.S3_SECRET_NOT_FOUND) {
        return Response.status(NOT_FOUND.getStatusCode(),
            OMException.ResultCodes.S3_SECRET_NOT_FOUND.toString())
            .build();
      } else {
        LOG.error("Can't execute revoke secret request: ", e);
        return Response.serverError().build();
      }
    }
  }

  private void revokeSecret(@Nullable String username) throws IOException {
    String actualUsername = username == null ? userNameFromRequest() : username;
    getClient().getObjectStore().revokeS3Secret(actualUsername);
  }
}
