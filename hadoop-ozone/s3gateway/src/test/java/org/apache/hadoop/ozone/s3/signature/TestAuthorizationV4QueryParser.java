/*
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

package org.apache.hadoop.ozone.s3.signature;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import org.apache.kerby.util.Hex;
import org.apache.hadoop.ozone.s3.signature.AWSSignatureProcessor.LowerCaseKeyStringMap;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link AuthorizationV4QueryParser}.
 */
public class TestAuthorizationV4QueryParser {

  private static final String DATETIME = StringToSignProducer.TIME_FORMATTER.
          format(LocalDateTime.now());
  @Test(expected = MalformedResourceException.class)
  public void testExpiredHeaders() throws Exception {

    //GIVEN
    Map<String, String> parameters = new HashMap<>();
    parameters.put("X-Amz-Algorithm", "AWS4-HMAC-SHA256");
    parameters.put("X-Amz-Credential",
        "AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request");
    parameters.put("X-Amz-Date", "20160801T083241Z");
    parameters.put("X-Amz-Expires", "10000");
    parameters.put("X-Amz-SignedHeaders", "host");
    parameters.put("X-Amz-Signature",
        "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404");

    AuthorizationV4QueryParser parser =
        new AuthorizationV4QueryParser(parameters);

    //WHEN
    parser.parseSignature();

    //THEN
    Assert.fail("Expired header is not detected");
  }

  @Test()
  public void testUnExpiredHeaders() throws Exception {

    //GIVEN
    Map<String, String> parameters = new HashMap<>();
    parameters.put("X-Amz-Algorithm", "AWS4-HMAC-SHA256");
    parameters.put("X-Amz-Credential",
        "AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request");
    parameters.put("X-Amz-Date",
        ZonedDateTime.now().format(StringToSignProducer.TIME_FORMATTER));
    parameters.put("X-Amz-Expires", "10000");
    parameters.put("X-Amz-SignedHeaders", "host");
    parameters.put("X-Amz-Signature",
        "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404");


    AuthorizationV4QueryParser parser =
        new AuthorizationV4QueryParser(parameters);

    //WHEN
    parser.parseSignature();

    //THEN
    //passed
  }

  @Test(expected = MalformedResourceException.class)
  public void testWithoutExpiration() throws Exception {

    //GIVEN
    Map<String, String> parameters = new HashMap<>();
    parameters.put("X-Amz-Algorithm", "AWS4-HMAC-SHA256");
    parameters.put("X-Amz-Credential",
        "AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request");
    parameters.put("X-Amz-Date", "20130524T000000Z");
    parameters.put("X-Amz-SignedHeaders", "host");
    parameters.put("X-Amz-Signature",
        "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404");

    AuthorizationV4QueryParser parser =
        new AuthorizationV4QueryParser(parameters);

    //WHEN
    parser.parseSignature();

    //THEN
    // X-Amz-Expires must be passed according to AWS documentation
    Assert.fail("Expiration duration must be passed.");
  }

  /**
   * Based on <a href="https://docs.aws.amazon.com
   * /AmazonS3/latest/API/sigv4-query-string-auth.html">AWS example</a>.
   */
  @Test
  public void testWithAWSExample() throws Exception {

    Map<String, String> queryParams = new HashMap<>();

    queryParams.put("X-Amz-Algorithm", "AWS4-HMAC-SHA256");
    queryParams.put("X-Amz-Credential",
        "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request");
    queryParams.put("X-Amz-Date", DATETIME);
    queryParams.put("X-Amz-Expires", "86400");
    queryParams.put("X-Amz-SignedHeaders", "host");
    queryParams.put("X-Amz-Signature",
        "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404");

    String canonicalRequest = "GET\n"
            + "/test.txt\n"
            + "X-Amz-Algorithm=AWS4-HMAC-SHA256&"
            + "X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2F"
            + "us-east-1%2Fs3%2Faws4_request&X-Amz-Date=" + DATETIME
            + "&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\n"
            + "host:localhost\n"
            + "\n"
            + "host\n"
            + "UNSIGNED-PAYLOAD";

    AuthorizationV4QueryParser parser =
        new AuthorizationV4QueryParser(queryParams) {
          @Override
          protected void validateDateAndExpires() {
            //noop
          }
        };

    final SignatureInfo signatureInfo = parser.parseSignature();

    LowerCaseKeyStringMap headers = new LowerCaseKeyStringMap();
    headers.put("host", "localhost");

    final String stringToSign =
        StringToSignProducer.createSignatureBase(signatureInfo, "https", "GET",
            "/test.txt", headers,
            queryParams);

    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update(canonicalRequest.getBytes(StandardCharsets.UTF_8));

    Assert.assertEquals("AWS4-HMAC-SHA256\n"
            + DATETIME + "\n"
            + "20130524/us-east-1/s3/aws4_request\n"
            + Hex.encode(md.digest()).toLowerCase(),
        stringToSign);
  }

}
