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

package org.apache.hadoop.ozone.om.security;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.MANAGED_S3_ACCESS_KEY_SECRET_UNAVAILABLE;
import static org.apache.hadoop.ozone.om.security.ManagedS3AccessKeySecretRetrievalManager.Operation.CREATE;
import static org.apache.hadoop.ozone.om.security.ManagedS3AccessKeySecretRetrievalManager.Operation.ROTATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.google.protobuf.ByteString;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ManagedS3AccessKeySecretRetrievalManager}.
 */
public class TestManagedS3AccessKeySecretRetrievalManager {

  private static final String CALLER = "om-admin";
  private static final String ACCESS_KEY_ID = "managed-access-key";
  private static final byte[] SECRET = "plain-secret".getBytes(UTF_8);
  private static final ByteString OPERATION_HASH =
      ByteString.copyFromUtf8("operation-hash");
  private static final ByteString OTHER_OPERATION_HASH =
      ByteString.copyFromUtf8("other-operation-hash");

  @Test
  public void retrieveSucceedsOnce() throws Exception {
    Fixture fixture = new Fixture(Duration.ofSeconds(60), 4);
    String handle = fixture.manager.store(CALLER, ACCESS_KEY_ID, CREATE,
        SECRET.clone());

    assertArrayEquals(SECRET,
        fixture.manager.retrieve(CALLER, ACCESS_KEY_ID, handle));

    assertUnavailable(() ->
        fixture.manager.retrieve(CALLER, ACCESS_KEY_ID, handle));
  }

  @Test
  public void expiredHandleFailsAndIsRemoved() throws Exception {
    Fixture fixture = new Fixture(Duration.ofMillis(10), 4);
    String handle = fixture.manager.store(CALLER, ACCESS_KEY_ID, CREATE,
        SECRET.clone());
    fixture.now.addAndGet(10);

    assertUnavailable(() ->
        fixture.manager.retrieve(CALLER, ACCESS_KEY_ID, handle));
    assertThat(fixture.manager.size()).isZero();
  }

  @Test
  public void unknownHandleFailsClosed() {
    Fixture fixture = new Fixture(Duration.ofSeconds(60), 4);

    assertUnavailable(() ->
        fixture.manager.retrieve(CALLER, ACCESS_KEY_ID, "unknown"));
  }

  @Test
  public void wrongCallerFailsWithoutConsumingHandle() throws Exception {
    Fixture fixture = new Fixture(Duration.ofSeconds(60), 4);
    String handle = fixture.manager.store(CALLER, ACCESS_KEY_ID, CREATE,
        SECRET.clone());

    assertUnavailable(() ->
        fixture.manager.retrieve("bob", ACCESS_KEY_ID, handle));
    assertArrayEquals(SECRET,
        fixture.manager.retrieve(CALLER, ACCESS_KEY_ID, handle));
  }

  @Test
  public void accessKeyIdMismatchFailsWithoutConsumingHandle()
      throws Exception {
    Fixture fixture = new Fixture(Duration.ofSeconds(60), 4);
    String handle = fixture.manager.store(CALLER, ACCESS_KEY_ID, ROTATE,
        SECRET.clone());

    assertUnavailable(() ->
        fixture.manager.retrieve(CALLER, "other-access-key", handle));
    assertArrayEquals(SECRET,
        fixture.manager.retrieve(CALLER, ACCESS_KEY_ID, handle));
  }

  @Test
  public void failoverLostHandleFailsClosed() throws Exception {
    Fixture leader = new Fixture(Duration.ofSeconds(60), 4);
    String handle = leader.manager.store(CALLER, ACCESS_KEY_ID, CREATE,
        SECRET.clone());
    Fixture newLeader = new Fixture(Duration.ofSeconds(60), 4);

    assertUnavailable(() ->
        newLeader.manager.retrieve(CALLER, ACCESS_KEY_ID, handle));
  }

  @Test
  public void mapFullPurgesExpiredEntriesBeforeStore() throws Exception {
    Fixture fixture = new Fixture(Duration.ofMillis(10), 1);
    fixture.manager.store(CALLER, ACCESS_KEY_ID, CREATE, SECRET.clone());
    fixture.now.addAndGet(10);

    String handle = fixture.manager.store(CALLER, "new-access-key", CREATE,
        "new-secret".getBytes(UTF_8));

    assertThat(fixture.manager.size()).isOne();
    assertArrayEquals("new-secret".getBytes(UTF_8),
        fixture.manager.retrieve(CALLER, "new-access-key", handle));
  }

  @Test
  public void mapFullAfterPurgeFailsBeforeStore() throws Exception {
    Fixture fixture = new Fixture(Duration.ofSeconds(60), 1);
    String first = fixture.manager.store(CALLER, ACCESS_KEY_ID, CREATE,
        SECRET.clone());

    assertUnavailable(() ->
        fixture.manager.store(CALLER, "new-access-key", CREATE,
            "new-secret".getBytes(UTF_8)));
    assertArrayEquals(SECRET,
        fixture.manager.retrieve(CALLER, ACCESS_KEY_ID, first));
  }

  @Test
  public void removeDropsPendingHandle() throws Exception {
    Fixture fixture = new Fixture(Duration.ofSeconds(60), 4);
    String handle = fixture.manager.store(CALLER, ACCESS_KEY_ID, CREATE,
        SECRET.clone());

    fixture.manager.remove(handle);

    assertUnavailable(() ->
        fixture.manager.retrieve(CALLER, ACCESS_KEY_ID, handle));
  }

  @Test
  public void clearDropsAllHandlesForFailover() throws Exception {
    Fixture fixture = new Fixture(Duration.ofSeconds(60), 4);
    String handle = fixture.manager.store(CALLER, ACCESS_KEY_ID, CREATE,
        SECRET.clone());
    fixture.manager.bindResponseHandle("client", ACCESS_KEY_ID, CREATE,
        OPERATION_HASH, handle);

    fixture.manager.clear();

    assertUnavailable(() ->
        fixture.manager.retrieve(CALLER, ACCESS_KEY_ID, handle));
    assertThat(fixture.manager.responseHandle("client", ACCESS_KEY_ID, CREATE,
        OPERATION_HASH))
        .isEmpty();
  }

  @Test
  public void retrieveWithEmptyInputsFailsClosed() {
    Fixture fixture = new Fixture(Duration.ofSeconds(60), 4);

    assertUnavailable(() ->
        fixture.manager.retrieve("", ACCESS_KEY_ID, "handle"));
    assertUnavailable(() ->
        fixture.manager.retrieve(CALLER, "", "handle"));
    assertUnavailable(() ->
        fixture.manager.retrieve(CALLER, ACCESS_KEY_ID, ""));
  }

  @Test
  public void responseHandleIsRemovedWithSecretEntry() throws Exception {
    Fixture fixture = new Fixture(Duration.ofSeconds(60), 4);
    String handle = fixture.manager.store(CALLER, ACCESS_KEY_ID, CREATE,
        SECRET.clone());
    fixture.manager.bindResponseHandle("client", ACCESS_KEY_ID, CREATE,
        OPERATION_HASH, handle);

    assertThat(fixture.manager.responseHandle("client", ACCESS_KEY_ID, CREATE,
        OPERATION_HASH))
        .isEqualTo(handle);

    fixture.manager.remove(handle);

    assertThat(fixture.manager.responseHandle("client", ACCESS_KEY_ID, CREATE,
        OPERATION_HASH))
        .isEmpty();
  }

  @Test
  public void responseHandlesAreIsolatedByOperationHash() throws Exception {
    Fixture fixture = new Fixture(Duration.ofSeconds(60), 4);
    String first = fixture.manager.store(CALLER, ACCESS_KEY_ID, CREATE,
        "first-secret".getBytes(UTF_8));
    String second = fixture.manager.store(CALLER, ACCESS_KEY_ID, CREATE,
        "second-secret".getBytes(UTF_8));
    fixture.manager.bindResponseHandle("client", ACCESS_KEY_ID, CREATE,
        OPERATION_HASH, first);
    fixture.manager.bindResponseHandle("client", ACCESS_KEY_ID, CREATE,
        OTHER_OPERATION_HASH, second);

    assertThat(fixture.manager.responseHandle("client", ACCESS_KEY_ID, CREATE,
        OPERATION_HASH)).isEqualTo(first);
    assertThat(fixture.manager.responseHandle("client", ACCESS_KEY_ID, CREATE,
        OTHER_OPERATION_HASH)).isEqualTo(second);

    fixture.manager.removeResponseHandle("client", ACCESS_KEY_ID, CREATE,
        OTHER_OPERATION_HASH);

    assertThat(fixture.manager.responseHandle("client", ACCESS_KEY_ID, CREATE,
        OPERATION_HASH)).isEqualTo(first);
    assertThat(fixture.manager.responseHandle("client", ACCESS_KEY_ID, CREATE,
        OTHER_OPERATION_HASH)).isEmpty();
  }

  @Test
  public void storedSecretIsCopiedFromCallerArray() throws Exception {
    Fixture fixture = new Fixture(Duration.ofSeconds(60), 4);
    byte[] callerSecret = SECRET.clone();
    String handle = fixture.manager.store(CALLER, ACCESS_KEY_ID, CREATE,
        callerSecret);
    Arrays.fill(callerSecret, (byte) 0);

    assertArrayEquals(SECRET,
        fixture.manager.retrieve(CALLER, ACCESS_KEY_ID, handle));
  }

  @Test
  public void generatedHandleHasAtLeast128BitsOfEntropy() throws Exception {
    Fixture fixture = new Fixture(Duration.ofSeconds(60), 4);
    String handle = fixture.manager.store(CALLER, ACCESS_KEY_ID, CREATE,
        SECRET.clone());

    assertThat(handle).doesNotContain("=");
    assertThat(handle.length()).isGreaterThanOrEqualTo(22);
  }

  @Test
  public void handleHashPrefixDoesNotExposeFullHandle() throws Exception {
    Fixture fixture = new Fixture(Duration.ofSeconds(60), 4);
    String handle = fixture.manager.store(CALLER, ACCESS_KEY_ID, CREATE,
        SECRET.clone());

    String prefix = fixture.manager.handleHashPrefix(handle);

    assertThat(prefix).isNotEmpty();
    assertThat(prefix).isNotEqualTo(handle);
    assertThat(prefix.length()).isLessThan(handle.length());
  }

  private static void assertUnavailable(ThrowingRunnable runnable) {
    OMException exception = assertThrows(OMException.class,
        runnable::run);
    assertThat(exception.getResult())
        .isEqualTo(MANAGED_S3_ACCESS_KEY_SECRET_UNAVAILABLE);
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private static final class Fixture {
    private final AtomicLong now = new AtomicLong();
    private final ManagedS3AccessKeySecretRetrievalManager manager;

    private Fixture(Duration ttl, int maxEntries) {
      manager = new ManagedS3AccessKeySecretRetrievalManager(
          ttl, maxEntries, deterministicRandom(), now::get);
    }
  }

  private static SecureRandom deterministicRandom() {
    SecureRandom random = mock(SecureRandom.class);
    AtomicInteger value = new AtomicInteger();
    doAnswer(invocation -> {
      byte[] target = invocation.getArgument(0);
      int seed = value.incrementAndGet();
      for (int i = 0; i < target.length; i++) {
        target[i] = (byte) (seed + i);
      }
      return null;
    }).when(random).nextBytes(any(byte[].class));
    return random;
  }
}
