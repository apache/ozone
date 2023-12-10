/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.security.symmetric;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * Contains utility to test secret key logic.
 */
public final class SecretKeyTestUtil {
  private SecretKeyTestUtil() {
  }

  public static ManagedSecretKey generateKey(
      String algorithm, Instant creationTime, Duration validDuration)
      throws NoSuchAlgorithmException {
    KeyGenerator keyGen = KeyGenerator.getInstance(algorithm);
    SecretKey secretKey = keyGen.generateKey();
    return new ManagedSecretKey(
        UUID.randomUUID(),
        creationTime,
        creationTime.plus(validDuration),
        secretKey
    );
  }

  public static ManagedSecretKey generateHmac(
      Instant creationTime, Duration validDuration)
      throws NoSuchAlgorithmException {
    return generateKey("HmacSHA256", creationTime, validDuration);
  }
}
