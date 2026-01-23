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

package org.apache.hadoop.ozone.s3;

import java.io.IOException;
import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;

/**
 * Filter to adjust request headers for compatible reasons.
 *
 * It should be executed AFTER signature check (VirtualHostStyleFilter) as the
 * original Content-Type could be part of the base of the signature.
 */
@Provider
@PreMatching
@Priority(HeaderPreprocessor.PRIORITY)
public class HeaderPreprocessor implements ContainerRequestFilter {

  public static final int PRIORITY = VirtualHostStyleFilter.PRIORITY +
      S3GatewayHttpServer.FILTER_PRIORITY_DO_AFTER;

  public static final String MULTIPART_UPLOAD_MARKER = "ozone/mpu";

  public static final String CONTENT_TYPE = "Content-Type";

  public static final String ORIGINAL_CONTENT_TYPE =
      "X-Ozone-Original-Content-Type";

  @Override
  public void filter(ContainerRequestContext requestContext) throws
      IOException {
    MultivaluedMap<String, String> queryParameters =
        requestContext.getUriInfo().getQueryParameters();

    if (requestContext.getHeaderString(CONTENT_TYPE) != null) {
      requestContext.getHeaders().putSingle(ORIGINAL_CONTENT_TYPE,
          requestContext.getHeaderString(CONTENT_TYPE));
    }

    if (queryParameters.containsKey("delete")) {
      //aws cli doesn't send proper Content-Type and by default POST requests
      //processed as form-url-encoded. Here we can fix this.
      requestContext.getHeaders()
          .putSingle(CONTENT_TYPE, MediaType.APPLICATION_XML);
    }

    if (queryParameters.containsKey("uploadId")) {
      //aws cli doesn't send proper Content-Type and by default POST requests
      //processed as form-url-encoded. Here we can fix this.
      requestContext.getHeaders()
          .putSingle(CONTENT_TYPE, MediaType.APPLICATION_XML);
    } else if (queryParameters.containsKey("uploads")) {
      // uploads defined but uploadId is not --> this is the creation of the
      // multi-part-upload requests.
      //
      //In  AWS SDK for go uses application/octet-stream which also
      //should be fixed to route the request to the right jaxrs method.
      //
      //Should be empty instead of XML as the body is empty which can not be
      //serialized as as CompleteMultipartUploadRequest
      requestContext.getHeaders()
          .putSingle(CONTENT_TYPE, MULTIPART_UPLOAD_MARKER);
    }

  }

}
