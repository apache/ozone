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

package org.apache.hadoop.hdds.security.symmetric;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test cases for {@link LocalSecretKeyStore}.
 */
class TestLocalKeyStore {
  private SecretKeyStore secretKeyStore;
  private Path testSecretFile;

  @TempDir
  private Path tempDir;

  @BeforeEach
  void setup() throws IOException {
    testSecretFile = Files.createFile(tempDir.resolve("key-store-test.json"));
    secretKeyStore = new LocalSecretKeyStore(testSecretFile);
  }

  static Stream<Arguments> saveAndLoadTestCases() throws Exception {
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
  void testSaveAndLoad(List<ManagedSecretKey> keys) throws IOException {
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

  /**
   * Verifies that secret keys are overwritten by subsequent writes.
   */
  @Test
  void testOverwrite() throws Exception {
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

  /**
   * This scenario verifies if an existing secret keys file can be loaded.
   * The intention of this is to ensure a saved file can be loaded after
   * future changes to {@link ManagedSecretKey} schema.
   *
   * Please don't just change the content of test json if this
   * test fails, instead, analyse the backward-compatibility of the change.
   */
  @Test
  void testLoadExistingFile() throws Exception {
    // copy test file content to the backing file.
    String testJson = "[\n" +
        "  {\n" +
        "    \"id\":\"78864cfb-793b-4157-8ad6-714c9f950a16\",\n" +
        "    \"creationTime\":\"2007-12-03T10:15:30Z\",\n" +
        "    \"expiryTime\":\"2007-12-03T11:15:30Z\",\n" +
        "    \"algorithm\":\"HmacSHA256\",\n" +
        "    \"encoded\":\"YSeCdJRB4RclxoeE69ENmTe2Cv8ybyKhHP3mq4M1r8o=\"\n" +
        "  }\n" +
        "]";
    Files.write(testSecretFile, Collections.singletonList(testJson),
        StandardOpenOption.WRITE);

    Instant date = Instant.parse("2007-12-03T10:15:30.00Z");
    ManagedSecretKey secretKey = new ManagedSecretKey(
        UUID.fromString("78864cfb-793b-4157-8ad6-714c9f950a16"),
        date,
        date.plus(Duration.ofHours(1)),
        new SecretKeySpec(
            Base64.getDecoder().decode(
                "YSeCdJRB4RclxoeE69ENmTe2Cv8ybyKhHP3mq4M1r8o="),
            "HmacSHA256"
        ));

    List<ManagedSecretKey> expectedKeys = newArrayList(secretKey);
    assertEqualKeys(expectedKeys, secretKeyStore.load());
  }

  private void assertEqualKeys(List<ManagedSecretKey> expected,
                               List<ManagedSecretKey> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      ManagedSecretKey expectedKey = expected.get(i);
      ManagedSecretKey actualKey = actual.get(i);

      assertEquals(expectedKey.getId(), actualKey.getId());
      assertEquals(expectedKey.getCreationTime().toEpochMilli(),
          actualKey.getCreationTime().toEpochMilli());
      assertEquals(expectedKey.getExpiryTime(),
          actualKey.getExpiryTime());
      assertEquals(expectedKey.getSecretKey(), actualKey.getSecretKey());
    }
  }

  private static ManagedSecretKey generateKey(String algorithm)
      throws Exception {
    return generateKey(algorithm, Instant.now());
  }

  private static ManagedSecretKey generateKey(String algorithm,
                                              Instant creationTime)
      throws Exception {
    KeyGenerator keyGen = KeyGenerator.getInstance(algorithm);
    SecretKey secretKey = keyGen.generateKey();
    return new ManagedSecretKey(
        UUID.randomUUID(),
        creationTime,
        creationTime.plus(Duration.ofHours(1)),
        secretKey
    );
  }
}
