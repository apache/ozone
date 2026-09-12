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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.STS_VALIDATION_ERROR;

import com.google.common.annotations.VisibleForTesting;
import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.s3.RequestIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps unmatched STS endpoint paths to AWS STS compatible XML errors.
 * <p>
 * Without this mapper, Jersey returns Jetty HTML 404 pages for paths such as {@code /sts},
 * which AWS clients report as {@code Unknown}.
 */
@Provider
public class OSTSNotFoundExceptionMapper implements ExceptionMapper<NotFoundException> {

  private static final Logger LOG = LoggerFactory.getLogger(OSTSNotFoundExceptionMapper.class);

  @Inject
  private RequestIdentifier requestIdentifier;

  @Context
  private UriInfo uriInfo;

  @Override
  public Response toResponse(NotFoundException exception) {
    final String requestPath = getRequestPath();
    final String validationMessage = buildValidationMessage(requestPath);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Returning STS validation error for unmatched path: {}", requestPath);
    }
    final OSTSException stsException = new OSTSException(STS_VALIDATION_ERROR).withMessage(validationMessage);
    stsException.setRequestId(requestIdentifier.getRequestId());
    return Response.status(stsException.getHttpCode())
        .entity(stsException.toXml())
        .type(MediaType.APPLICATION_XML)
        .build();
  }

  private String getRequestPath() {
    if (uriInfo == null) {
      return "/";
    }
    final String path = uriInfo.getPath();
    if (StringUtils.isBlank(path)) {
      return "/";
    }
    return path.startsWith("/") ? path : "/" + path;
  }

  private static String buildValidationMessage(String requestPath) {
    return "1 validation error detected: Invalid STS endpoint path '" + requestPath + "'. "
        + "Ozone STS is served at the root path /.";
  }

  @VisibleForTesting
  public void setRequestIdentifier(RequestIdentifier requestIdentifier) {
    this.requestIdentifier = requestIdentifier;
  }

  @VisibleForTesting
  public void setUriInfo(UriInfo uriInfo) {
    this.uriInfo = uriInfo;
  }
}
