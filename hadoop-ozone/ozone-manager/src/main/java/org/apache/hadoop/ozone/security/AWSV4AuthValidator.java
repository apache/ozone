/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.security;

import org.apache.hadoop.util.StringUtils;
import org.apache.kerby.util.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Pattern;

/**
 * AWS v4 authentication payload validator. For more details refer to AWS
 * documentation https://docs.aws.amazon.com/general/latest/gr/
 * sigv4-create-canonical-request.html.
 **/
final class AWSV4AuthValidator {

  private static final Logger LOG =
      LoggerFactory.getLogger(AWSV4AuthValidator.class);
  private static final String HMAC_SHA256_ALGORITHM = "HmacSHA256";
  private static final Pattern STR_TO_SIGN_PATTERN =
          Pattern.compile("^AWS4-HMAC-SHA256\n" +
                  "\\d{8}T\\d{6}Z\n" +
                  "\\d{8}/[a-z\\-\\d]+/[a-zA-Z0-9]+/aws4_request\n" +
                  "[a-fA-F0-9]+$");

  private AWSV4AuthValidator() {
  }

  public static String hash(String payload) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update(payload.getBytes(StandardCharsets.UTF_8));
    return String.format("%064x", new java.math.BigInteger(1, md.digest()));
  }

  /**
   * Returns true if strToSign is valid.
   *
   * @param strToSign
   *
   * strToSignFormat = "AWS4-HMAC-SHA256" + "\n" +
   * timeStampISO8601Format + "\n" +
   * YYYYMMDD/<region>/<service>/aws4_request + "\n" +
   * Hex(SHA256Hash(<CanonicalRequest>))
   *
   * For more details refer to AWS documentation: https://docs.aws.amazon.com
   * /general/latest/gr/create-signed-request.html#create-string-to-sign
   *
   * */
  private static boolean validateStrToSign(String strToSign) {
    return STR_TO_SIGN_PATTERN.matcher(strToSign).find();
  }

  private static byte[] sign(byte[] key, String msg) {
    try {
      SecretKeySpec signingKey = new SecretKeySpec(key, HMAC_SHA256_ALGORITHM);
      Mac mac = Mac.getInstance(HMAC_SHA256_ALGORITHM);
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
    if (!validateStrToSign(strToSign)) {
      return false;
    }
    String expectedSignature = Hex.encode(sign(getSigningKey(userKey,
        strToSign), strToSign));
    return expectedSignature.equals(signature);
  }
}
