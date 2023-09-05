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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.io.IOException;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;

/**
 * Revoke secret endpoint.
 */
@Path("/secret/revoke")
@S3SecretEnabled
public class S3SecretRevokeEndpoint extends S3SecretEndpointBase {

  private static final Logger LOG =
          LoggerFactory.getLogger(S3SecretRevokeEndpoint.class);


  @POST
  public Response revoke() throws IOException {
    try {
      revokeSecret();
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

  private void revokeSecret() throws IOException {
    getClient().getObjectStore().revokeS3Secret(userNameFromRequest());
  }

}
