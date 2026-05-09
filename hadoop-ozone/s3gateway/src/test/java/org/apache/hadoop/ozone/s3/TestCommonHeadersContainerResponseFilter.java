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

import static org.apache.hadoop.ozone.s3.util.S3Consts.ACCESS_CONTROL_ALLOW_METHODS;
import static org.apache.hadoop.ozone.s3.util.S3Consts.ACCESS_CONTROL_ALLOW_ORIGIN;
import static org.apache.hadoop.ozone.s3.util.S3Consts.ACCESS_CONTROL_EXPOSE_HEADERS;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CACHED_BUCKETS_CONTEXT_PROPERTY;
import static org.apache.hadoop.ozone.s3.util.S3Consts.ORIGIN_HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.helpers.CorsConfiguration;
import org.apache.hadoop.ozone.om.helpers.CorsRule;
import org.junit.jupiter.api.Test;

/**
 * Tests common S3 response headers.
 */
public class TestCommonHeadersContainerResponseFilter {

  @Test
  public void addsCorsHeadersToActualResponseWhenRuleMatches()
      throws Exception {
    String bucketName = "cors-filter-bucket";
    String origin = "https://example.com";
    OzoneBucket bucket = mock(OzoneBucket.class);
    when(bucket.getCorsConfiguration()).thenReturn(corsConfiguration(origin));

    CommonHeadersContainerResponseFilter filter =
        new CommonHeadersContainerResponseFilter();
    setField(filter, "requestIdentifier", new RequestIdentifier());
    MultivaluedMap<String, Object> responseHeaders =
        new MultivaluedHashMap<>();
    Map<String, OzoneBucket> cachedBuckets = new HashMap<>();
    cachedBuckets.put(bucketName, bucket);

    filter.filter(request("GET", "/" + bucketName + "/key", origin,
            cachedBuckets),
        response(responseHeaders));

    assertThat(responseHeaders.getFirst(ACCESS_CONTROL_ALLOW_ORIGIN))
        .isEqualTo(origin);
    assertThat(responseHeaders.getFirst(ACCESS_CONTROL_EXPOSE_HEADERS))
        .isEqualTo("ETag");
    assertThat(responseHeaders.getFirst(ACCESS_CONTROL_ALLOW_METHODS))
        .isNull();
  }

  @Test
  public void usesCachedBucketForCorsHeaders() throws Exception {
    String bucketName = "cors-filter-bucket";
    String origin = "https://example.com";
    OzoneBucket bucket = mock(OzoneBucket.class);
    when(bucket.getCorsConfiguration()).thenReturn(corsConfiguration(origin));
    CommonHeadersContainerResponseFilter filter =
        new CommonHeadersContainerResponseFilter();
    setField(filter, "requestIdentifier", new RequestIdentifier());
    MultivaluedMap<String, Object> responseHeaders =
        new MultivaluedHashMap<>();
    Map<String, OzoneBucket> cachedBuckets = new HashMap<>();
    cachedBuckets.put(bucketName, bucket);

    filter.filter(request("GET", "/" + bucketName + "/key", origin,
            cachedBuckets),
        response(responseHeaders));

    assertThat(responseHeaders.getFirst(ACCESS_CONTROL_ALLOW_ORIGIN))
        .isEqualTo(origin);
    assertThat(responseHeaders.getFirst(ACCESS_CONTROL_EXPOSE_HEADERS))
        .isEqualTo("ETag");
  }

  private static ContainerRequestContext request(String method, String path,
      String origin, Map<String, OzoneBucket> cachedBuckets) {
    ContainerRequestContext request = mock(ContainerRequestContext.class);
    UriInfo uriInfo = mock(UriInfo.class);
    when(request.getMethod()).thenReturn(method);
    when(request.getHeaderString(ORIGIN_HEADER)).thenReturn(origin);
    when(request.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPath(false)).thenReturn(path);
    when(request.getProperty(CACHED_BUCKETS_CONTEXT_PROPERTY))
        .thenReturn(cachedBuckets);
    return request;
  }

  private static ContainerResponseContext response(
      MultivaluedMap<String, Object> responseHeaders) {
    ContainerResponseContext response = mock(ContainerResponseContext.class);
    when(response.getHeaders()).thenReturn(responseHeaders);
    return response;
  }

  private static void setField(Object target, String name, Object value)
      throws ReflectiveOperationException {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static CorsConfiguration corsConfiguration(String origin) {
    return CorsConfiguration.newBuilder()
        .addRule(CorsRule.newBuilder()
            .setAllowedOrigins(Collections.singletonList(origin))
            .setAllowedMethods(Arrays.asList("GET", "HEAD"))
            .setExposeHeaders(Collections.singletonList("ETag"))
            .build())
        .build();
  }
}
