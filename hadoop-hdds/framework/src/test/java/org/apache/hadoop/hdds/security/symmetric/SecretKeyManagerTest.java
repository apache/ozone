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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.newArrayList;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.of;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests cases for {@link SecretKeyManager} implementation.
 */
public class SecretKeyManagerTest {
  private static final Duration VALIDITY_DURATION = Duration.ofDays(3);
  private static final Duration ROTATION_DURATION = Duration.ofDays(1);
  private static final String ALGORITHM = "HmacSHA256";

  private SecretKeyStore mockedKeyStore;

  @BeforeEach
  private void setup() {
    mockedKeyStore = Mockito.mock(SecretKeyStore.class);
  }

  public static Stream<Arguments> loadSecretKeysTestCases() throws Exception {
    ManagedSecretKey k0 = generateKey(now());
    ManagedSecretKey k1 = generateKey(now().minus(1, DAYS));
    ManagedSecretKey k2 = generateKey(now().minus(2, DAYS));
    ManagedSecretKey k3 = generateKey(now().minus(3, DAYS));
    ManagedSecretKey k4 = generateKey(now().minus(4, DAYS));
    ManagedSecretKey k5 = generateKey(now().minus(5, DAYS));
    return Stream.of(
        // first start
        of(ImmutableList.of(), null, null),

        // restart => nothing is filtered
        of(newArrayList(k0, k1, k2), k0, newArrayList(k0, k1, k2)),

        // stop 1 day and start
        of(newArrayList(k1, k2, k3), k1, newArrayList(k1, k2)),

        // stop 2 day and start => expired keys are filtered
        of(newArrayList(k2, k3, k4), k2, newArrayList(k2)),

        // stop 3 day and start, all saved keys are filtered
        of(newArrayList(k3, k4, k5), null, null)
    );
  }

  /**
   * Verify how SecretKeyManager initializes its keys under different scenarios,
   * e.g. with or without the present of saved keys.
   */
  @ParameterizedTest
  @MethodSource("loadSecretKeysTestCases")
  public void testLoadSecretKeys(List<ManagedSecretKey> savedSecretKey,
                                 ManagedSecretKey expectedCurrentKey,
                                 List<ManagedSecretKey> expectedLoadedKeys)
      throws TimeoutException {
    SecretKeyState state = new TestSecretKeyState(mockedKeyStore);
    SecretKeyManager lifeCycleManager =
        new SecretKeyManager(state, mockedKeyStore,
            ROTATION_DURATION, VALIDITY_DURATION, ALGORITHM);

    when(mockedKeyStore.load()).thenReturn(savedSecretKey);
    lifeCycleManager.initialize();

    if (expectedCurrentKey != null) {
      assertEquals(state.getCurrentKey(), expectedCurrentKey);
      List<ManagedSecretKey> allKeys = state.getAllKeys();
      assertSameKeys(expectedLoadedKeys, allKeys);
    } else {
      // expect the current key is newly generated.
      assertFalse(savedSecretKey.contains(state.getCurrentKey()));
      assertEquals(1, state.getAllKeys().size());
      assertTrue(state.getAllKeys().contains(
          state.getCurrentKey()));
    }
  }

  private static void assertSameKeys(Collection<ManagedSecretKey> expected,
                                     Collection<ManagedSecretKey> actual) {
    assertEquals(expected.size(), actual.size());
    for (ManagedSecretKey expectedKey : expected) {
      assertTrue(actual.contains(expectedKey));
    }
  }

  public static Stream<Arguments> rotationTestCases() throws Exception {
    ManagedSecretKey k0 = generateKey(now());
    ManagedSecretKey k1 = generateKey(now().minus(1, DAYS));
    ManagedSecretKey k2 = generateKey(now().minus(2, DAYS));
    ManagedSecretKey k3 = generateKey(now().minus(3, DAYS));
    ManagedSecretKey k4 = generateKey(now().minus(4, DAYS));
    return Stream.of(

        // Currentkey is new, not rotate.
        of(newArrayList(k0, k1, k2), k0, false, null),

        // Current key just exceeds the rotation period.
        of(newArrayList(k1, k2, k3), k1, true, newArrayList(k1, k2)),

        // Current key exceeds the rotation period for a significant time (2d).
        of(newArrayList(k2, k3, k4), k2, true, newArrayList(k2))
    );
  }

  /**
   * Verify rotation behavior under different scenarios.
   */
  @ParameterizedTest
  @MethodSource("rotationTestCases")
  public void testRotate(List<ManagedSecretKey> initialKeys,
                         ManagedSecretKey initialCurrentKey,
                         boolean expectRotate,
                         List<ManagedSecretKey> expectedRetainedKeys)
      throws TimeoutException {

    SecretKeyState state = new TestSecretKeyState(mockedKeyStore);

    SecretKeyManager lifeCycleManager =
        new SecretKeyManager(state, mockedKeyStore,
            ROTATION_DURATION, VALIDITY_DURATION, ALGORITHM);

    // Set the initial state.
    state.updateKeys(initialCurrentKey, initialKeys);
    Mockito.reset(mockedKeyStore);

    assertEquals(lifeCycleManager.checkAndRotate(), expectRotate);

    if (expectRotate) {
      // Verify rotation behavior.

      // 1. A new key is generated as current key.
      ManagedSecretKey currentKey = state.getCurrentKey();
      assertNotEquals(initialCurrentKey, currentKey);
      assertFalse(initialKeys.contains(currentKey));

      // 2. keys are correctly rotated, expired ones are excluded.
      List<ManagedSecretKey> expectedAllKeys = expectedRetainedKeys;
      expectedAllKeys.add(currentKey);
      assertSameKeys(expectedAllKeys, state.getAllKeys());

      // 3. All keys are stored.
      ArgumentCaptor<Collection<ManagedSecretKey>> storedKeyCaptor =
          ArgumentCaptor.forClass(Collection.class);
      verify(mockedKeyStore).save(storedKeyCaptor.capture());
      assertSameKeys(expectedAllKeys, storedKeyCaptor.getValue());

      // 4. The new generated key has correct data.
      assertEquals(ALGORITHM, currentKey.getSecretKey().getAlgorithm());
      assertEquals(0,
          Duration.between(currentKey.getCreationTime(), now()).toMinutes());
      Instant expectedExpiryTime = now().plus(VALIDITY_DURATION);
      assertEquals(0,
          Duration.between(currentKey.getExpiryTime(),
              expectedExpiryTime).toMinutes());
    } else {
      assertEquals(initialCurrentKey, state.getCurrentKey());
      assertSameKeys(initialKeys, state.getAllKeys());
    }
  }

  private static ManagedSecretKey generateKey(Instant creationTime)
      throws Exception {
    KeyGenerator keyGen = KeyGenerator.getInstance(ALGORITHM);
    SecretKey secretKey = keyGen.generateKey();
    return new ManagedSecretKey(
        UUID.randomUUID(),
        creationTime,
        creationTime.plus(VALIDITY_DURATION),
        secretKey
    );
  }

  private static class TestSecretKeyState implements SecretKeyState {
    private ManagedSecretKey currentKey;
    private Map<UUID, ManagedSecretKey> allKeys;
    private SecretKeyStore keyStore;

    TestSecretKeyState(SecretKeyStore keyStore) {
      this.keyStore = keyStore;
    }

    @Override
    public ManagedSecretKey getCurrentKey() {
      return currentKey;
    }

    @Override
    public List<ManagedSecretKey> getAllKeys() {
      return new ArrayList<>(allKeys.values());
    }

    @Override
    public ManagedSecretKey getKeyById(UUID id) {
      return allKeys.get(id);
    }

    @Override
    public void updateKeys(ManagedSecretKey newCurrentKey,
                           List<ManagedSecretKey> newAllKeys) {
      this.currentKey = newCurrentKey;
      this.allKeys = newAllKeys.stream().collect(
          toMap(ManagedSecretKey::getId, identity()));
      keyStore.save(newAllKeys);
    }

    @Override
    public void updateKeysInternal(ManagedSecretKey newCurrentKey,
                                   List<ManagedSecretKey> newAllKeys) {
      updateKeys(newCurrentKey, newAllKeys);
    }
  }

}
