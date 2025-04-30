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

package org.apache.hadoop.ozone.security;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.util.StringUtils;
import org.apache.kerby.util.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AWS v4 authentication payload validator. For more details refer to AWS
 * documentation https://docs.aws.amazon.com/general/latest/gr/
 * sigv4-create-canonical-request.html.
 **/
final class AWSV4AuthValidator {

  private static final Logger LOG =
      LoggerFactory.getLogger(AWSV4AuthValidator.class);
  private static final String HMAC_SHA256_ALGORITHM = "HmacSHA256";

  /**
   * ThreadLocal cache of Mac instances.
   */
  private static final ThreadLocal<Mac> THREAD_LOCAL_MAC =
      ThreadLocal.withInitial(() -> {
        try {
          return Mac.getInstance(HMAC_SHA256_ALGORITHM);
        } catch (NoSuchAlgorithmException nsa) {
          throw new IllegalArgumentException(
              "Failed to initialize Mac instance that implements the " +
                  HMAC_SHA256_ALGORITHM + " algorithm.", nsa);
        }
      });

  private AWSV4AuthValidator() {
  }

  public static String hash(String payload) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update(payload.getBytes(StandardCharsets.UTF_8));
    return String.format("%064x", new java.math.BigInteger(1, md.digest()));
  }

  private static byte[] sign(byte[] key, String msg) {
    try {
      SecretKeySpec signingKey = new SecretKeySpec(key, HMAC_SHA256_ALGORITHM);
      // Returns the cached Mac instance for the current thread or creates a
      // new one if none exists.
      Mac mac = THREAD_LOCAL_MAC.get();
      mac.init(signingKey);
      return mac.doFinal(msg.getBytes(StandardCharsets.UTF_8));
    } catch (GeneralSecurityException gse) {
      throw new RuntimeException(gse);
    }
  }

  /**
   * Returns signing key.
   *
   * @param key
   * @param strToSign
   *
   * SignatureKey = HMAC-SHA256(HMAC-SHA256(HMAC-SHA256(HMAC-SHA256("AWS4" +
   * "<YourSecretAccessKey>","20130524"),"us-east-1"),"s3"),"aws4_request")
   *
   * For more details refer to AWS documentation: https://docs.aws.amazon
   * .com/AmazonS3/latest/API/sig-v4-header-based-auth.html
   *
   * */
  private static byte[] getSigningKey(String key, String strToSign) {
    String[] signData = StringUtils.split(StringUtils.split(strToSign,
        '\n')[2], '/');
    String dateStamp = signData[0];
    String regionName = signData[1];
    String serviceName = signData[2];
    byte[] kDate = sign(("AWS4" + key)
        .getBytes(StandardCharsets.UTF_8), dateStamp);
    byte[] kRegion = sign(kDate, regionName);
    byte[] kService = sign(kRegion, serviceName);
    byte[] kSigning = sign(kService, "aws4_request");
    if (LOG.isDebugEnabled()) {
      LOG.debug(Hex.encode(kSigning));
    }
    return kSigning;
  }

  /**
   * Validate request by comparing Signature from request. Returns true if
   * aws request is legit else returns false.
   * Signature = HEX(HMAC_SHA256(key, String to Sign))
   *
   * For more details refer to AWS documentation: https://docs.aws.amazon.com
   * /AmazonS3/latest/API/sigv4-streaming.html
   */
  public static boolean validateRequest(String strToSign, String signature,
      String userKey) {
    String expectedSignature = Hex.encode(sign(getSigningKey(userKey,
        strToSign), strToSign));
    return expectedSignature.equals(signature);
  }
}
