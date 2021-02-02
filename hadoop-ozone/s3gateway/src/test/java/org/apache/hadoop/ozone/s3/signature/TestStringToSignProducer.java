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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.ozone.s3.HeaderPreprocessor;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.signature.AWSSignatureProcessor.LowerCaseKeyStringMap;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test string2sign creation.
 */
public class TestStringToSignProducer {

  @Test
  public void test() throws Exception {

    LowerCaseKeyStringMap headers = new LowerCaseKeyStringMap();
    headers.put("Content-Length", "123");
    headers.put("Host", "0.0.0.0:9878");
    headers.put("X-AMZ-Content-Sha256", "Content-SHA");
    headers.put("X-AMZ-Date", "123");
    headers.put("Content-Type", "ozone/mpu");
    headers.put(HeaderPreprocessor.ORIGINAL_CONTENT_TYPE, "streaming");

    String authHeader =
        "AWS4-HMAC-SHA256 Credential=AKIAJWFJK62WUTKNFJJA/20181009/us-east-1"
            + "/s3/aws4_request, "
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date;"
            + "content-type, "
            + "Signature"
            +
            "=db81b057718d7c1b3b8dffa29933099551c51d787b3b13b9e0f9ebed45982bf2";

    headers.put("Authorization",
        authHeader);

    Map<String, String> queryParameters = new HashMap<>();

    final SignatureInfo signatureInfo =
        new AuthorizationV4HeaderParser(authHeader, "123") {
          @Override
          public void validateDateRange(Credential credentialObj)
              throws OS3Exception {
            //NOOP
          }
        }.parseSignature();

    headers.fixContentType();

    final String signatureBase =
        StringToSignProducer.createSignatureBase(
            signatureInfo,
            "http",
            "GET",
            "/buckets",
            headers,
            queryParameters);

    Assert.assertEquals(
        "String to sign is invalid",
        "AWS4-HMAC-SHA256\n"
            + "123\n"
            + "20181009/us-east-1/s3/aws4_request\n"
            +
            "f20d4de80af2271545385e8d4c7df608cae70a791c69b97aab1527ed93a0d665",
        signatureBase);
  }

}