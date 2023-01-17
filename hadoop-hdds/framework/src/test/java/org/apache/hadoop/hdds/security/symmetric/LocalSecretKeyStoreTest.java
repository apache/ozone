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

package org.apache.hadoop.hdds.security.symmetric;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link LocalSecretKeyStore}
 */
public class LocalSecretKeyStoreTest {
  private SecretKeyStore secretKeyStore;
  private Path testSecretFile;

  @BeforeEach
  private void setup() throws Exception {
    testSecretFile = Files.createTempFile("key-strore-test", ".json");
    secretKeyStore = new LocalSecretKeyStore(testSecretFile);
  }

  public static Stream<Arguments> saveAndLoadTestCases() throws Exception {
    return Stream.of(
        // empty
        Arguments.of(ImmutableList.of()),
        // single secret keys.
        Arguments.of(newArrayList(
            generateKey("HmacSHA256")
        )),
        // multiple secret keys.
        Arguments.of(newArrayList(
            generateKey("HmacSHA1"),
            generateKey("HmacSHA256")
        ))
    );
  }

  @ParameterizedTest
  @MethodSource("saveAndLoadTestCases")
  public void testSaveAndLoad(List<ManagedSecretKey> keys) throws IOException {
    secretKeyStore.save(keys);

    // Ensure the intended file exists and is readable and writeable to
    // file owner only.
    File file = testSecretFile.toFile();
    assertTrue(file.exists());
    Set<PosixFilePermission> permissions =
        Files.getPosixFilePermissions(file.toPath());
    assertEquals(newHashSet(OWNER_READ, OWNER_WRITE), permissions);

    List<ManagedSecretKey> reloadedKeys = secretKeyStore.load();
    assertEqualKeys(keys, reloadedKeys);
  }

  @Test
  public void testOverwrite() throws Exception {
    List<ManagedSecretKey> initialKeys =
        newArrayList(generateKey("HmacSHA256"));
    secretKeyStore.save(initialKeys);

    List<ManagedSecretKey> updatedKeys = newArrayList(
        generateKey("HmacSHA1"),
        generateKey("HmacSHA256")
    );
    secretKeyStore.save(updatedKeys);

    assertEqualKeys(updatedKeys, secretKeyStore.load());
  }

  private void assertEqualKeys(List<ManagedSecretKey> keys,
                         List<ManagedSecretKey> reloadedKeys) {
    assertEquals(keys.size(), reloadedKeys.size());
    for (int i = 0; i < keys.size(); i++) {
      ManagedSecretKey key = keys.get(i);
      ManagedSecretKey reloadedKey = reloadedKeys.get(i);

      assertEquals(key.getId(), reloadedKey.getId());
      assertEquals(key.getCreationTime().toEpochMilli(),
          reloadedKey.getCreationTime().toEpochMilli());
      assertEquals(key.getExpiryTime(),
          reloadedKey.getExpiryTime());
      assertEquals(key.getSecretKey(), reloadedKey.getSecretKey());
    }
  }

  private static ManagedSecretKey generateKey(String algorithm)
      throws Exception {
    KeyGenerator keyGen = KeyGenerator.getInstance(algorithm);
    SecretKey secretKey = keyGen.generateKey();
    Instant now = Instant.now();
    return new ManagedSecretKey(
        UUID.randomUUID(),
        now,
        now.plus(Duration.ofHours(1)),
        secretKey
    );
  }
}
