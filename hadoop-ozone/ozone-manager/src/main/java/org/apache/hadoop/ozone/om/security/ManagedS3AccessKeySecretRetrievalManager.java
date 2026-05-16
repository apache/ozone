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

import com.google.protobuf.ByteString;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.util.Time;

/**
 * Leader-local store for one-time managed S3 secret retrieval handles.
 */
public final class ManagedS3AccessKeySecretRetrievalManager {

  private static final int HANDLE_ENTROPY_BYTES = 32;
  private static final int MAX_GENERATE_ATTEMPTS = 16;
  private static final int HANDLE_HASH_PREFIX_LENGTH = 16;

  private final Duration ttl;
  private final int maxEntries;
  private final SecureRandom random;
  private final LongSupplier clock;
  private final Map<String, Entry> entries = new HashMap<>();
  private final Map<ResponseKey, String> responseHandles = new HashMap<>();

  public ManagedS3AccessKeySecretRetrievalManager(Duration ttl,
      int maxEntries) {
    this(ttl, maxEntries, new SecureRandom(), Time::monotonicNow);
  }

  ManagedS3AccessKeySecretRetrievalManager(Duration ttl, int maxEntries,
      SecureRandom random, LongSupplier clock) {
    if (ttl == null || ttl.isZero() || ttl.isNegative()) {
      throw new IllegalArgumentException("ttl must be positive");
    }
    if (maxEntries <= 0) {
      throw new IllegalArgumentException("maxEntries must be positive");
    }
    this.ttl = ttl;
    this.maxEntries = maxEntries;
    this.random = Objects.requireNonNull(random);
    this.clock = Objects.requireNonNull(clock);
  }

  public synchronized String store(String caller, String accessKeyId,
      Operation operation, byte[] plaintextSecret) throws OMException {
    requireNonEmpty(caller, "caller");
    requireNonEmpty(accessKeyId, "accessKeyId");
    Objects.requireNonNull(operation, "operation == null");
    requireSecret(plaintextSecret);

    long now = clock.getAsLong();
    purgeExpired(now);
    if (entries.size() >= maxEntries) {
      throw unavailable();
    }

    for (int attempt = 0; attempt < MAX_GENERATE_ATTEMPTS; attempt++) {
      String handle = newHandle();
      if (!entries.containsKey(handle)) {
        long expiresAt = now + ttl.toMillis();
        entries.put(handle, new Entry(caller, accessKeyId, operation,
            expiresAt, plaintextSecret.clone()));
        return handle;
      }
    }
    throw unavailable();
  }

  public synchronized byte[] retrieve(String caller, String accessKeyId,
      String handle) throws OMException {
    if (isEmpty(caller) || isEmpty(accessKeyId) || isEmpty(handle)) {
      throw unavailable();
    }

    long now = clock.getAsLong();
    Entry entry = entries.get(handle);
    if (entry == null) {
      throw unavailable();
    }
    if (entry.isExpired(now)) {
      removeAndClear(handle);
      throw unavailable();
    }
    if (!entry.matches(caller, accessKeyId)) {
      throw unavailable();
    }
    entries.remove(handle);
    responseHandles.values().removeIf(handle::equals);
    return entry.secret;
  }

  public synchronized void remove(String handle) {
    if (handle != null) {
      removeAndClear(handle);
    }
  }

  public synchronized void bindResponseHandle(String clientId,
      String accessKeyId, Operation operation, ByteString operationHash,
      String handle) {
    requireNonEmpty(clientId, "clientId");
    requireNonEmpty(accessKeyId, "accessKeyId");
    Objects.requireNonNull(operation, "operation == null");
    requireNonEmpty(operationHash, "operationHash");
    requireNonEmpty(handle, "retrievalHandle");
    responseHandles.put(
        new ResponseKey(clientId, accessKeyId, operation, operationHash),
        handle);
  }

  public synchronized String responseHandle(String clientId,
      String accessKeyId, Operation operation, ByteString operationHash) {
    requireNonEmpty(clientId, "clientId");
    requireNonEmpty(accessKeyId, "accessKeyId");
    Objects.requireNonNull(operation, "operation == null");
    requireNonEmpty(operationHash, "operationHash");
    purgeExpired(clock.getAsLong());
    String handle = responseHandles.get(
        new ResponseKey(clientId, accessKeyId, operation, operationHash));
    return handle == null ? "" : handle;
  }

  public synchronized void removeResponseHandle(String clientId,
      String accessKeyId, Operation operation, ByteString operationHash) {
    if (clientId != null && accessKeyId != null && operation != null &&
        operationHash != null) {
      String handle = responseHandles.remove(
          new ResponseKey(clientId, accessKeyId, operation, operationHash));
      removeAndClear(handle);
    }
  }

  public synchronized void clear() {
    for (Entry entry : entries.values()) {
      ManagedS3AccessKeySecretEnvelopeCodec.clear(entry.secret);
    }
    entries.clear();
    responseHandles.clear();
  }

  public synchronized int size() {
    purgeExpired(clock.getAsLong());
    return entries.size();
  }

  public String handleHashPrefix(String handle) {
    if (handle == null || handle.isEmpty()) {
      return "";
    }
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(handle.getBytes(UTF_8));
      String encoded = Base64.getUrlEncoder().withoutPadding()
          .encodeToString(hash);
      return encoded.substring(0,
          Math.min(HANDLE_HASH_PREFIX_LENGTH, encoded.length()));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  private void purgeExpired(long now) {
    Iterator<Map.Entry<String, Entry>> iterator =
        entries.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, Entry> mapEntry = iterator.next();
      if (mapEntry.getValue().isExpired(now)) {
        ManagedS3AccessKeySecretEnvelopeCodec.clear(
            mapEntry.getValue().secret);
        responseHandles.values().removeIf(mapEntry.getKey()::equals);
        iterator.remove();
      }
    }
  }

  private void removeAndClear(String handle) {
    if (handle == null) {
      return;
    }
    Entry entry = entries.remove(handle);
    if (entry != null) {
      ManagedS3AccessKeySecretEnvelopeCodec.clear(entry.secret);
    }
    responseHandles.values().removeIf(handle::equals);
  }

  private String newHandle() {
    byte[] bytes = new byte[HANDLE_ENTROPY_BYTES];
    random.nextBytes(bytes);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }

  private static OMException unavailable() {
    return new OMException("Managed S3 access key secret is unavailable",
        MANAGED_S3_ACCESS_KEY_SECRET_UNAVAILABLE);
  }

  private static void requireNonEmpty(String value, String field) {
    if (isEmpty(value)) {
      throw new IllegalArgumentException(field + " must not be empty");
    }
  }

  private static void requireNonEmpty(ByteString value, String field) {
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException(field + " must not be empty");
    }
  }

  private static boolean isEmpty(String value) {
    return value == null || value.isEmpty();
  }

  private static void requireSecret(byte[] plaintextSecret) {
    if (plaintextSecret == null || plaintextSecret.length == 0) {
      throw new IllegalArgumentException(
          "plaintextSecret must not be empty");
    }
  }

  /**
   * Lifecycle operation that produced a retrieval handle.
   */
  public enum Operation {
    CREATE,
    ROTATE
  }

  private static final class Entry {
    private final String caller;
    private final String accessKeyId;
    private final Operation operation;
    private final long expiresAt;
    private final byte[] secret;

    private Entry(String caller, String accessKeyId, Operation operation,
        long expiresAt, byte[] secret) {
      this.caller = caller;
      this.accessKeyId = accessKeyId;
      this.operation = operation;
      this.expiresAt = expiresAt;
      this.secret = secret;
    }

    private boolean isExpired(long now) {
      return now >= expiresAt;
    }

    private boolean matches(String expectedCaller, String expectedAccessKeyId) {
      return caller.equals(expectedCaller) &&
          accessKeyId.equals(expectedAccessKeyId) &&
          operation != null;
    }
  }

  private static final class ResponseKey {
    private final String clientId;
    private final String accessKeyId;
    private final Operation operation;
    private final ByteString operationHash;

    private ResponseKey(String clientId, String accessKeyId,
        Operation operation, ByteString operationHash) {
      this.clientId = clientId;
      this.accessKeyId = accessKeyId;
      this.operation = operation;
      this.operationHash = operationHash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof ResponseKey)) {
        return false;
      }
      ResponseKey that = (ResponseKey) obj;
      return clientId.equals(that.clientId) &&
          accessKeyId.equals(that.accessKeyId) &&
          operation == that.operation &&
          operationHash.equals(that.operationHash);
    }

    @Override
    public int hashCode() {
      return Objects.hash(clientId, accessKeyId, operation, operationHash);
    }
  }
}
