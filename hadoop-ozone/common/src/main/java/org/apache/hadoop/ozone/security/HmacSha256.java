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
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** HMAC-SHA256 signing with a per-thread Mac instance. */
public final class HmacSha256 {

  public static final String ALGORITHM = "HmacSHA256";
  private static final ThreadLocal<Mac> MAC = new ThreadLocal<>();

  private HmacSha256() {
  }

  public static byte[] sign(SecretKeySpec key, String message) throws NoSuchAlgorithmException, InvalidKeyException {
    Mac mac = MAC.get();
    if (mac == null) {
      mac = Mac.getInstance(ALGORITHM);
      MAC.set(mac);
    }
    // A thread can serve different requests and signing-key derivation steps.
    mac.init(key);
    return mac.doFinal(message.getBytes(StandardCharsets.UTF_8));
  }
}
