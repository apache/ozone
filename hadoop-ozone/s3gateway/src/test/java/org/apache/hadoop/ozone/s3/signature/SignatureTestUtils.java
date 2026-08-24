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

import static java.nio.charset.StandardCharsets.UTF_8;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Shared AWS Signature Version 4 helpers for tests.
 */
public final class SignatureTestUtils {

  private SignatureTestUtils() {
  }

  /**
   * Derive the SigV4 signing key: {@code HMAC(HMAC(HMAC(HMAC("AWS4"+secret,
   * date), region), service), "aws4_request")}.
   */
  public static byte[] signingKey(String secretKey, String date, String region,
      String service) {
    byte[] key = hmac(("AWS4" + secretKey).getBytes(UTF_8), date);
    key = hmac(key, region);
    key = hmac(key, service);
    return hmac(key, "aws4_request");
  }

  /** @return {@code HMAC-SHA256(key, msg)}. */
  public static byte[] hmac(byte[] key, String msg) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(key, "HmacSHA256"));
      return mac.doFinal(msg.getBytes(UTF_8));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
