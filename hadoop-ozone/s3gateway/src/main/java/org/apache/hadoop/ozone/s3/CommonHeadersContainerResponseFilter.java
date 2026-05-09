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
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.helpers.CorsRule;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class adds common header responses for all the requests.
 */
@Provider
public class CommonHeadersContainerResponseFilter implements
    ContainerResponseFilter {

  private static final Logger LOG =
      LoggerFactory.getLogger(CommonHeadersContainerResponseFilter.class);

  @Inject
  private RequestIdentifier requestIdentifier;

  @Override
  public void filter(ContainerRequestContext containerRequestContext,
      ContainerResponseContext containerResponseContext) throws IOException {

    containerResponseContext.getHeaders().add("Server", "Ozone");
    containerResponseContext.getHeaders()
        .add("x-amz-id-2", requestIdentifier.getAmzId());
    containerResponseContext.getHeaders()
        .add("x-amz-request-id", requestIdentifier.getRequestId());

    addCorsHeaders(containerRequestContext, containerResponseContext);
  }

  private void addCorsHeaders(ContainerRequestContext requestContext,
      ContainerResponseContext responseContext) {
    String origin = requestContext.getHeaderString(S3Consts.ORIGIN_HEADER);
    if (StringUtils.isBlank(origin)
        || "OPTIONS".equalsIgnoreCase(requestContext.getMethod())) {
      return;
    }

    String bucketName = getBucketName(requestContext);
    if (StringUtils.isBlank(bucketName)) {
      return;
    }

    try {
      OzoneBucket bucket = getCachedBucket(requestContext, bucketName);
      if (bucket == null) {
        return;
      }
      Optional<CorsRule> rule = S3CorsHeaders.findMatchingRule(
          bucket.getCorsConfiguration(), origin, requestContext.getMethod(),
          null);
      rule.ifPresent(corsRule -> S3CorsHeaders.applyHeaders(
          responseContext.getHeaders(), corsRule, origin, null, false));
    } catch (Exception ex) {
      LOG.debug("Unable to add CORS headers for bucket {}", bucketName, ex);
    }
  }

  @SuppressWarnings("unchecked")
  private static OzoneBucket getCachedBucket(
      ContainerRequestContext requestContext, String bucketName) {
    Map<String, OzoneBucket> buckets =
        (Map<String, OzoneBucket>) requestContext.getProperty(
            S3Consts.CACHED_BUCKETS_CONTEXT_PROPERTY);
    return buckets == null ? null : buckets.get(bucketName);
  }

  private static String getBucketName(ContainerRequestContext requestContext) {
    String path = requestContext.getUriInfo().getPath(false);
    if (StringUtils.isBlank(path)) {
      return null;
    }
    String normalizedPath = path.charAt(0) == '/' ? path.substring(1) : path;
    int delimiter = normalizedPath.indexOf('/');
    return delimiter < 0 ? normalizedPath : normalizedPath.substring(0,
        delimiter);
  }
}
