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
import com.google.common.collect.ImmutableMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
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

  /** The data-parity schemes allowed by default by ozone.replication.allowed-configs. */
  private static final int[][] PREDEFINED_SCHEMES = {{3, 2}, {6, 3}, {10, 4}};
  /** The chunk sizes allowed by default: 512k, 1024k, 2048k and 4096k. */
  private static final int PREDEFINED_MIN_CHUNK_SIZE = 512 * 1024;
  private static final int PREDEFINED_CHUNK_SIZES = 4;
  private static final EcCodec[] CODECS = EcCodec.values();

  /**
   * One shared instance for each config allowed by default by ozone.replication.allowed-configs, with each codec.
   * Only these are used in practice, so {@link #getInstance} returns them instead of a new object; any other config,
   * for example one a client sends, is created as before.
   */
  private static final ECReplicationConfig[] PREDEFINED =
      new ECReplicationConfig[CODECS.length * PREDEFINED_SCHEMES.length * PREDEFINED_CHUNK_SIZES];

  /** The predefined configs by their string form, such as rs-6-3-1024k, to look them up without the regex. */
  private static final Map<String, ECReplicationConfig> PREDEFINED_BY_STRING;

  static {
    for (EcCodec codec : CODECS) {
      for (int scheme = 0; scheme < PREDEFINED_SCHEMES.length; scheme++) {
        for (int chunk = 0; chunk < PREDEFINED_CHUNK_SIZES; chunk++) {
          PREDEFINED[predefinedIndex(codec, scheme, chunk)] = new ECReplicationConfig(PREDEFINED_SCHEMES[scheme][0],
              PREDEFINED_SCHEMES[scheme][1], codec, PREDEFINED_MIN_CHUNK_SIZE << chunk);
        }
      }
    }
    final ImmutableMap.Builder<String, ECReplicationConfig> byString = ImmutableMap.builder();
    for (ECReplicationConfig config : PREDEFINED) {
      byString.put(config.getReplication(), config);
    }
    PREDEFINED_BY_STRING = byString.build();
  }

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

  /** Same as {@link #ECReplicationConfig(String)}, but returns a shared instance for the predefined configs. */
  public static ECReplicationConfig getInstance(String string) {
    return parse(string);
  }

  /** Same as the proto constructor, but returns a shared instance for the predefined configs, without creating one. */
  public static ECReplicationConfig getInstance(HddsProtos.ECReplicationConfig proto) {
    final EcCodec codec = getCodec(proto.getCodec());
    if (codec == null) {
      return new ECReplicationConfig(proto); // throws the same exception as before
    }
    final ECReplicationConfig predefined = getPredefined(proto.getData(), proto.getParity(), codec,
        proto.getEcChunkSize());
    return predefined != null ? predefined
        : new ECReplicationConfig(proto.getData(), proto.getParity(), codec, proto.getEcChunkSize());
  }

  /** @return the predefined instance for the given values, or null if they are not a predefined config. */
  private static ECReplicationConfig getPredefined(int data, int parity, EcCodec codec, int ecChunkSize) {
    int scheme = PREDEFINED_SCHEMES.length - 1;
    while (scheme >= 0 && (PREDEFINED_SCHEMES[scheme][0] != data || PREDEFINED_SCHEMES[scheme][1] != parity)) {
      scheme--;
    }
    int chunk = PREDEFINED_CHUNK_SIZES - 1;
    while (chunk >= 0 && PREDEFINED_MIN_CHUNK_SIZE << chunk != ecChunkSize) {
      chunk--;
    }
    return scheme < 0 || chunk < 0 ? null : PREDEFINED[predefinedIndex(codec, scheme, chunk)];
  }

  private static int predefinedIndex(EcCodec codec, int scheme, int chunk) {
    return (codec.ordinal() * PREDEFINED_SCHEMES.length + scheme) * PREDEFINED_CHUNK_SIZES + chunk;
  }

  /** Like {@code EcCodec.valueOf(name.toUpperCase())}, without creating a string; null if there is no such codec. */
  private static EcCodec getCodec(String name) {
    for (EcCodec codec : CODECS) {
      if (codec.name().equalsIgnoreCase(name)) {
        return codec;
      }
    }
    return null;
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
   * This always creates a new object; {@link #getInstance(String)} returns a shared one for the predefined configs.
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
   * Extracts the codec, data, parity and chunk size from a string such as rs-6-3-1024k. The predefined configs in
   * their usual form, as {@link #getReplication()} prints them, are looked up without the regex.
   *
   * @return the predefined instance for these values, or a new instance for any other config
   * @throws IllegalArgumentException if the string is not a valid EC replication config
   */
  private static ECReplicationConfig parse(String string) {
    final ECReplicationConfig predefined = PREDEFINED_BY_STRING.get(string);
    if (predefined != null) {
      return predefined;
    }

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
    final ECReplicationConfig other = getPredefined(data, parity, codec, chunkSize);
    return other != null ? other : new ECReplicationConfig(data, parity, codec, chunkSize);
  }

  public ECReplicationConfig(
      HddsProtos.ECReplicationConfig ecReplicationConfig) {
    this.data = ecReplicationConfig.getData();
    this.parity = ecReplicationConfig.getParity();
    this.codec = EcCodec.valueOf(ecReplicationConfig.getCodec().toUpperCase());
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
    return Objects.hash(data, parity, codec, ecChunkSize);
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
