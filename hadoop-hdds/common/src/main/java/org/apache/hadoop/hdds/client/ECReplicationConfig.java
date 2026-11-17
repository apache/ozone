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

package org.apache.hadoop.hdds.client;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import java.util.EnumSet;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * Replication configuration for EC replication.
 */
@Immutable
public class ECReplicationConfig implements ReplicationConfig {

  public static final String EC_REPLICATION_PARAMS_DELIMITER = "-";

  // <codec>-<data>-<parity>-<chunk size>, usually as getReplication() prints it, such as rs-6-3-1024k. The codec and
  // the "k" suffix are case-insensitive, and without "k" the chunk size is in bytes, so RS-6-3-1024K and
  // rs-6-3-1048576 are accepted too.
  private static final Pattern STRING_FORMAT = Pattern.compile("([a-zA-Z]+)-(\\d+)-(\\d+)-(\\d+)([kK])?");

  /** The maximum number of configs, and of strings, cached by {@link #getInstance}. */
  @VisibleForTesting
  static final int MAX_CACHED = 64;
  /**
   * The shared instance of each config by value. Only a few configs are used in practice, so they are shared instead
   * of creating equal objects; the size is bounded since a client may send arbitrary configs.
   */
  private static final ConcurrentMap<ECReplicationConfig, ECReplicationConfig> INSTANCES = new ConcurrentHashMap<>();
  /** The shared instances by their string form, such as rs-6-3-1024k, to look them up without the regex. */
  private static final ConcurrentMap<String, ECReplicationConfig> BY_STRING = new ConcurrentHashMap<>();
  private static final EcCodec[] CODECS = EcCodec.values();

  private final int data;

  private final int parity;

  private final int ecChunkSize;

  private final EcCodec codec;

  /**
   * Enum defining the allowed list of ECCodecs.
   */
  public enum EcCodec {
    RS, XOR;

    @Override
    public String toString() {
      return name().toLowerCase();
    }

    public static String allValuesAsString() {
      return EnumSet.allOf(EcCodec.class)
          .stream()
          .map(Enum::toString)
          .collect(Collectors.joining(","));
    }
  }

  /**
   * Same as {@link #ECReplicationConfig(String)}, but returns a shared instance for a config seen before, up to
   * {@link #MAX_CACHED} configs.
   */
  public static ECReplicationConfig getInstance(String string) {
    final ECReplicationConfig cached = BY_STRING.get(string);
    if (cached != null) {
      return cached;
    }
    final ECReplicationConfig config = intern(parse(string));
    // The size check is not atomic with the put, so concurrent callers may exceed the bound by a few entries.
    if (BY_STRING.size() < MAX_CACHED) {
      BY_STRING.putIfAbsent(string, config);
    }
    return config;
  }

  /**
   * Same as {@link #ECReplicationConfig(HddsProtos.ECReplicationConfig)}, but returns a shared instance for a config
   * seen before, up to {@link #MAX_CACHED} configs.
   */
  public static ECReplicationConfig getInstance(HddsProtos.ECReplicationConfig proto) {
    return intern(new ECReplicationConfig(proto));
  }

  /** @return the shared instance equal to the given config, or the given config if the cache is full. */
  private static ECReplicationConfig intern(ECReplicationConfig config) {
    final ECReplicationConfig cached = INSTANCES.get(config);
    if (cached != null) {
      return cached;
    }
    // The size check is not atomic with the put, so concurrent callers may exceed the bound by a few entries.
    if (INSTANCES.size() >= MAX_CACHED) {
      return config;
    }
    final ECReplicationConfig previous = INSTANCES.putIfAbsent(config, config);
    return previous != null ? previous : config;
  }

  /** Like {@code EcCodec.valueOf(name.toUpperCase())}, but without creating a string for a valid codec. */
  private static EcCodec getCodec(String name) {
    for (EcCodec codec : CODECS) {
      if (codec.name().equalsIgnoreCase(name)) {
        return codec;
      }
    }
    return EcCodec.valueOf(name.toUpperCase()); // throws the same exception as before
  }

  @VisibleForTesting
  static void clearCache() {
    BY_STRING.clear();
    INSTANCES.clear();
  }

  public ECReplicationConfig(int data, int parity) {
    this(data, parity, EcCodec.RS, 1024 * 1024);
  }

  public ECReplicationConfig(int data, int parity, EcCodec codec,
      int ecChunkSize) {
    this.data = data;
    this.parity = parity;
    this.codec = codec;
    this.ecChunkSize = ecChunkSize;
  }

  /**
   * Creates an ECReplicationConfig from a string in the form {@code <codec>-<data>-<parity>-<chunk size>}, usually
   * as {@link #getReplication()} prints it, such as rs-6-3-1024k. The codec and the "k" suffix are case-insensitive,
   * and without "k" the chunk size is in bytes, so RS-6-3-1024K and rs-6-3-1048576 are the same config as
   * rs-6-3-1024k.
   * <p>
   * This always creates a new object; {@link #getInstance(String)} returns a shared one.
   *
   * @param string the EC replication config
   * @throws IllegalArgumentException if the string is not in this form, or the codec, data, parity or chunk size is
   *         invalid
   */
  public ECReplicationConfig(String string) {
    this(parse(string));
  }

  private ECReplicationConfig(ECReplicationConfig other) {
    this(other.data, other.parity, other.codec, other.ecChunkSize);
  }

  /**
   * Extracts the codec, data, parity and chunk size from a string such as rs-6-3-1024k.
   *
   * @return a new instance for these values
   * @throws IllegalArgumentException if the string is not a valid EC replication config
   */
  private static ECReplicationConfig parse(String string) {
    final Matcher matcher = STRING_FORMAT.matcher(string);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("EC replication config should be " +
          "defined in the form rs-3-2-1024k, rs-6-3-1024k; or rs-10-4-1024k." +
          " Provided configuration was: " + string);
    }

    final EcCodec codec;
    try {
      codec = EcCodec.valueOf(matcher.group(1).toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("The codec " + matcher.group(1) +
          " is invalid. It must be one of " + EcCodec.allValuesAsString() + ".",
          e);
    }

    final int data = Integer.parseInt(matcher.group(2));
    final int parity = Integer.parseInt(matcher.group(3));
    if (data <= 0 || parity <= 0) {
      throw new IllegalArgumentException("Data and parity part in EC " +
          "replication config supposed to be positive numbers");
    }

    int chunkSize = Integer.parseInt((matcher.group(4)));
    if (chunkSize <= 0) {
      throw new IllegalArgumentException("The ecChunkSize (" + chunkSize +
          ") be greater than zero");
    }
    if (matcher.group(5) != null) {
      // The "k" modifier is present, so multiply by 1024
      chunkSize = chunkSize * 1024;
    }
    return new ECReplicationConfig(data, parity, codec, chunkSize);
  }

  public ECReplicationConfig(
      HddsProtos.ECReplicationConfig ecReplicationConfig) {
    this.data = ecReplicationConfig.getData();
    this.parity = ecReplicationConfig.getParity();
    this.codec = getCodec(ecReplicationConfig.getCodec());
    this.ecChunkSize = ecReplicationConfig.getEcChunkSize();
  }

  @Override
  public HddsProtos.ReplicationType getReplicationType() {
    return HddsProtos.ReplicationType.EC;
  }

  @Override
  public int getRequiredNodes() {
    return data + parity;
  }

  @Override
  @JsonIgnore
  public String getReplication() {
    return getCodec() + EC_REPLICATION_PARAMS_DELIMITER
        + getData() + EC_REPLICATION_PARAMS_DELIMITER
        + getParity() + EC_REPLICATION_PARAMS_DELIMITER
        + chunkKB();
  }

  /** Similar to {@link #getReplication()}, but applies to proto structure, without any validation. */
  public static String toString(HddsProtos.ECReplicationConfig proto) {
    return proto.getCodec() + EC_REPLICATION_PARAMS_DELIMITER
        + proto.getData() + EC_REPLICATION_PARAMS_DELIMITER
        + proto.getParity() + EC_REPLICATION_PARAMS_DELIMITER
        + proto.getEcChunkSize();
  }

  public HddsProtos.ECReplicationConfig toProto() {
    return HddsProtos.ECReplicationConfig.newBuilder()
        .setData(data)
        .setParity(parity)
        .setCodec(codec.toString())
        .setEcChunkSize(ecChunkSize)
        .build();
  }

  public int getData() {
    return data;
  }

  public int getParity() {
    return parity;
  }

  public int getEcChunkSize() {
    return ecChunkSize;
  }

  public EcCodec getCodec() {
    return codec;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ECReplicationConfig that = (ECReplicationConfig) o;
    return data == that.data && parity == that.parity
        && codec == that.getCodec() && ecChunkSize == that.getEcChunkSize();
  }

  @Override
  public int hashCode() {
    // Same value as Objects.hash(data, parity, codec, ecChunkSize), without boxing: getInstance looks up every config
    return ((((31 + data) * 31 + parity) * 31 + Objects.hashCode(codec)) * 31) + ecChunkSize;
  }

  @Override
  public String toString() {
    return HddsProtos.ReplicationType.EC + "{"
        + codec + "-" + data + "-" + parity + "-" + chunkKB() + "}";
  }

  @Override
  public String configFormat() {
    return HddsProtos.ReplicationType.EC.name()
        + "/" + data + "-" + parity + "-" + chunkKB();
  }

  @Override
  public int getMinimumNodes() {
    return data;
  }

  private String chunkKB() {
    return ecChunkSize / 1024 + "k";
  }
}
