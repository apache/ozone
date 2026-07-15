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

package org.apache.hadoop.ozone.s3.signature;

import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AWSSignatureProcessor}.
 */
public class TestAWSSignatureProcessor {

  @Test
  public void testLowerCaseHeaderMapRemovesKeysCaseInsensitively() {
    AWSSignatureProcessor.LowerCaseKeyStringMap headers =
        new AWSSignatureProcessor.LowerCaseKeyStringMap();
    headers.put("Authorization", "AWS4-HMAC-SHA256 value");

    assertEquals("AWS4-HMAC-SHA256 value",
        headers.remove("AUTHORIZATION"));
    assertFalse(headers.containsKey("authorization"));
  }

  @Test
  public void testOutOfRangeExpiresPreSignedUrlReturns403() throws Exception {
    // A pre-signed URL whose X-Amz-Expires is out of range must be rejected
    // with 403 AccessDenied, not 400 MalformedHeader.
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.putSingle("X-Amz-Algorithm", "AWS4-HMAC-SHA256");
    queryParams.putSingle("X-Amz-Credential",
        "AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request");
    queryParams.putSingle("X-Amz-Date", "20130524T000000Z");
    queryParams.putSingle("X-Amz-Expires", "604801");
    queryParams.putSingle("X-Amz-SignedHeaders", "host");
    queryParams.putSingle("X-Amz-Signature",
        "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404");

    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getQueryParameters()).thenReturn(queryParams);
    when(uriInfo.getPathParameters()).thenReturn(new MultivaluedHashMap<>());
    when(uriInfo.getPath()).thenReturn("key");
    ContainerRequestContext context = mock(ContainerRequestContext.class);
    when(context.getUriInfo()).thenReturn(uriInfo);
    when(context.getHeaders()).thenReturn(new MultivaluedHashMap<>());
    when(context.getMethod()).thenReturn("GET");

    AWSSignatureProcessor processor = new AWSSignatureProcessor();
    processor.setContext(context);

    assertErrorResponse(S3ErrorTable.ACCESS_DENIED, processor::parseSignature);
  }
}
