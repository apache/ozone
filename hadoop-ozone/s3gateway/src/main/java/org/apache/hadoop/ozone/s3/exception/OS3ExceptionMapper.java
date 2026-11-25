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

package org.apache.hadoop.ozone.s3.exception;

import javax.inject.Inject;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.apache.hadoop.ozone.s3.RequestIdentifier;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Class the represents various errors returned by the
 *  Ozone S3 service.
 */
@Provider
public class OS3ExceptionMapper implements ExceptionMapper<OS3Exception> {

  private static final String EXPIRED_TOKEN = "ExpiredToken";

  private static final Logger LOG =
      LoggerFactory.getLogger(OS3ExceptionMapper.class);

  @Inject
  private RequestIdentifier requestIdentifier;

  @Inject
  private SignatureInfo signatureInfo;

  @Override
  public Response toResponse(OS3Exception exception) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Returning exception. ex: {}", exception.toString());
    }
    exception.setRequestId(requestIdentifier.getRequestId());
    exception.setHostId(requestIdentifier.getAmzId());
    if (EXPIRED_TOKEN.equals(exception.getCode()) && signatureInfo != null) {
      final String sessionToken = signatureInfo.getSessionToken();
      if (sessionToken != null && !sessionToken.isEmpty()) {
        exception.setToken0(sessionToken);
      }
    }
    return Response.status(exception.getHttpCode())
        .entity(exception.toXml())
        .type(MediaType.APPLICATION_XML)
        .build();
  }
}
