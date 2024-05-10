/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3.throttler;

import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.s3.signature.SignatureProcessor.DATE_FORMATTER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for UserIdentityProvider.
 */
class TestUserIdentityProvider {
  private final String curDate = DATE_FORMATTER.format(LocalDate.now());

  private final IdentityProvider identityProvider = new UserIdentityProvider();

  private static Request getRequest(String authHeader) {
    return new Request() {
      @Override
      public Map<String, String> getHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", authHeader);
        return headers;
      }

      @Override
      public Map<String, String> getQueryParameters() {
        return new HashMap<>();
      }
    };
  }

  @Test
  void testMakeIdentityValidCredential() throws Exception {
    String auth = "AWS4-HMAC-SHA256 " +
        "Credential=ozone/" + curDate + "/us-east-1/s3/aws4_request, " +
        "SignedHeaders=host;range;x-amz-date, " +
        "Signature=fe5f80f77d5fa3beca038a248ff027";

    Request request = getRequest(auth);
    String identity = identityProvider.makeIdentity(request);

    assertEquals("ozone", identity);
  }

  @Test
  void testMakeIdentityInvalidCredential() {
    String auth = "AWS4-HMAC-SHA256 " +
        "Credential=" + curDate + "/us-east-1/s3/aws4_request, " +
        "SignedHeaders=host;range;x-amz-date, " +
        "Signature=fe5f80f77d5fa3beca038a248ff027";
    Request request = getRequest(auth);

    OS3Exception ex = assertThrows(OS3Exception.class,
        () -> identityProvider.makeIdentity(request));
    assertEquals("AuthorizationHeaderMalformed", ex.getCode());
  }
}
