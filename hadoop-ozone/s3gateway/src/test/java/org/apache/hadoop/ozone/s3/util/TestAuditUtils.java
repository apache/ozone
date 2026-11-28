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

package org.apache.hadoop.ozone.s3.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AuditUtils}.
 */
public class TestAuditUtils {

  @Test
  public void testPresignedRequestCredentialsAreNotAudited() {
    MultivaluedMap<String, String> query = new MultivaluedHashMap<>();
    // SigV4 and SigV2 credentials, including a differently cased name
    query.add("X-Amz-Signature", "secret");
    query.add("X-Amz-Credential", "secret");
    query.add("X-Amz-Security-Token", "secret");
    query.add("AWSAccessKeyId", "secret");
    query.add("Signature", "secret");
    query.add("x-amz-SIGNATURE", "secret");
    query.add("X-Amz-Date", "20130524T000000Z");
    query.add("prefix", "photos/");

    Map<String, String> params = AuditUtils.getAuditParameters(requestContext(query));

    assertThat(params).containsOnlyKeys("bucket", "X-Amz-Date", "prefix");
  }

  private static ContainerRequestContext requestContext(MultivaluedMap<String, String> query) {
    MultivaluedMap<String, String> path = new MultivaluedHashMap<>();
    path.add("bucket", "bucket1");
    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getPathParameters()).thenReturn(path);
    when(uriInfo.getQueryParameters()).thenReturn(query);
    ContainerRequestContext context = mock(ContainerRequestContext.class);
    when(context.getUriInfo()).thenReturn(uriInfo);
    return context;
  }
}
