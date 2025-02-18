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

import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;

/**
 * Test implementation of {@link SecretKeyClient}.
 */
public class SecretKeyTestClient implements SecretKeyClient {
  private final Map<UUID, ManagedSecretKey> keysMap = new HashMap<>();
  private ManagedSecretKey current;

  public SecretKeyTestClient() {
    rotate();
  }

  public void rotate() {
    this.current = generateKey();
    keysMap.put(current.getId(), current);
  }

  @Override
  public ManagedSecretKey getCurrentSecretKey() {
    return current;
  }

  @Override
  public ManagedSecretKey getSecretKey(UUID id) {
    return keysMap.get(id);
  }

  private ManagedSecretKey generateKey() {
    KeyGenerator keyGen = null;
    try {
      keyGen = KeyGenerator.getInstance("HmacSHA256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Should never happen", e);
    }
    SecretKey secretKey = keyGen.generateKey();
    return new ManagedSecretKey(
        UUID.randomUUID(),
        Instant.now(),
        Instant.now().plus(Duration.ofHours(1)),
        secretKey
    );
  }
}
