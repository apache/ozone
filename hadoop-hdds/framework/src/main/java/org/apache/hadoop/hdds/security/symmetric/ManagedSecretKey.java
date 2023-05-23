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

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ProtobufUtils;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.UUID;

/**
 * Enclosed a symmetric {@link SecretKey} with additional data for life-cycle
 * management.
 */
public final class ManagedSecretKey {
  private final UUID id;
  private final Instant creationTime;
  private final Instant expiryTime;
  private final SecretKey secretKey;
  private final ThreadLocal<Mac> macInstances;

  public ManagedSecretKey(UUID id,
                          Instant creationTime,
                          Instant expiryTime,
                          SecretKey secretKey) {
    this.id = id;
    this.creationTime = creationTime;
    this.expiryTime = expiryTime;
    this.secretKey = secretKey;

    // This help reuse Mac instances for the same thread.
    macInstances = ThreadLocal.withInitial(() -> {
      try {
        return Mac.getInstance(secretKey.getAlgorithm());
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalArgumentException(
            "Invalid algorithm " + secretKey.getAlgorithm(), e);
      }
    });
  }

  public boolean isExpired() {
    return expiryTime.isBefore(Instant.now());
  }

  public UUID getId() {
    return id;
  }

  public SecretKey getSecretKey() {
    return secretKey;
  }

  public Instant getCreationTime() {
    return creationTime;
  }

  public Instant getExpiryTime() {
    return expiryTime;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ManagedSecretKey)) {
      return false;
    }
    ManagedSecretKey that = (ManagedSecretKey) obj;
    return this.id.equals(that.id);
  }

  @Override
  public String toString() {
    return "SecretKey(id = " + id + ", creation at: "
        + creationTime + ", expire at: " + expiryTime + ")";
  }

  public byte[] sign(byte[] data) {
    try {
      Mac mac = macInstances.get();
      mac.init(secretKey);
      return mac.doFinal(data);
    } catch (InvalidKeyException e) {
      throw new IllegalArgumentException("Invalid key to HMAC computation", e);
    }
  }

  public byte[] sign(TokenIdentifier tokenId) {
    return sign(tokenId.getBytes());
  }

  public boolean isValidSignature(byte[] data, byte[] signature) {
    byte[] expectedSignature = sign(data);
    return MessageDigest.isEqual(expectedSignature, signature);
  }

  public boolean isValidSignature(TokenIdentifier tokenId, byte[] signature) {
    return isValidSignature(tokenId.getBytes(), signature);
  }

  /**
   * @return the protobuf message to deserialize this object.
   */
  public SCMSecretKeyProtocolProtos.ManagedSecretKey toProtobuf() {
    return SCMSecretKeyProtocolProtos.ManagedSecretKey.newBuilder()
        .setId(ProtobufUtils.toProtobuf(id))
        .setCreationTime(this.creationTime.toEpochMilli())
        .setExpiryTime(this.expiryTime.toEpochMilli())
        .setAlgorithm(this.secretKey.getAlgorithm())
        .setEncoded(ByteString.copyFrom(this.secretKey.getEncoded()))
        .build();
  }

  /**
   * Create a {@link ManagedSecretKey} from a given protobuf message.
   */
  public static ManagedSecretKey fromProtobuf(
      SCMSecretKeyProtocolProtos.ManagedSecretKey message) {
    UUID id = ProtobufUtils.fromProtobuf(message.getId());
    Instant creationTime = Instant.ofEpochMilli(message.getCreationTime());
    Instant expiryTime = Instant.ofEpochMilli(message.getExpiryTime());
    SecretKey secretKey = new SecretKeySpec(message.getEncoded().toByteArray(),
        message.getAlgorithm());
    return new ManagedSecretKey(id, creationTime, expiryTime, secretKey);
  }

  public static ManagedSecretKey fromProtobuf(
      OzoneManagerProtocolProtos.GetCurrentSecretKeyResponse response) {
    UUID id = ProtobufUtils.fromProtobuf(response.getSecretKey().getId());
    Instant creationTime =
        Instant.ofEpochMilli(response.getSecretKey().getCreationTime());
    Instant expiryTime =
        Instant.ofEpochMilli(response.getSecretKey().getExpiryTime());
    SecretKey secretKey =
        new SecretKeySpec(response.getSecretKey().getEncoded().toByteArray(),
            response.getSecretKey().getAlgorithm());
    return new ManagedSecretKey(id, creationTime, expiryTime, secretKey);
  }

  public static OzoneManagerProtocolProtos.ManagedSecretKey toProto(
      ManagedSecretKey secretKey) {
    OzoneManagerProtocolProtos.ManagedSecretKey.Builder builder =
        OzoneManagerProtocolProtos.ManagedSecretKey.newBuilder();
    builder.setId(ProtobufUtils.toProtobuf(secretKey.getId()))
        .setCreationTime(secretKey.getCreationTime().toEpochMilli())
        .setExpiryTime(secretKey.getExpiryTime().toEpochMilli())
        .setAlgorithm(secretKey.getSecretKey().getAlgorithm())
        .setEncoded(ByteString.copyFrom(secretKey.getSecretKey().getEncoded()));
    return builder.build();
  }
}
