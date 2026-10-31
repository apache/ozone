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
import java.util.EnumSet;
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

  // Acceptable patterns are like:
  //   rs-3-2-1024k
  //   RS-3-2-2048
  //   XOR-10-4-4096K
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

  static {
    for (EcCodec codec : CODECS) {
      for (int scheme = 0; scheme < PREDEFINED_SCHEMES.length; scheme++) {
        for (int chunk = 0; chunk < PREDEFINED_CHUNK_SIZES; chunk++) {
          PREDEFINED[predefinedIndex(codec, scheme, chunk)] = new ECReplicationConfig(PREDEFINED_SCHEMES[scheme][0],
              PREDEFINED_SCHEMES[scheme][1], codec, PREDEFINED_MIN_CHUNK_SIZE << chunk);
        }
      }
    }
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
    final ECReplicationConfig parsed = new ECReplicationConfig(string);
    final ECReplicationConfig predefined = getPredefined(parsed.data, parsed.parity, parsed.codec, parsed.ecChunkSize);
    return predefined != null ? predefined : parsed;
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
   * Create an ECReplicationConfig object from a string representing the
   * various parameters. Acceptable patterns are like:
   *     rs-3-2-1024k
   *     RS-3-2-2048k
   *     XOR-10-4-4096K
   * IllegalArgumentException will be thrown if the passed string does not
   * match the defined pattern.
   * @param string Parameters used to create ECReplicationConfig instances.
   *       Format: &lt;codec&gt;-&lt;data&gt;-&lt;parity&gt;-&lt;chunksize&gt;
   */
  public ECReplicationConfig(String string) {
    final Matcher matcher = STRING_FORMAT.matcher(string);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("EC replication config should be " +
          "defined in the form rs-3-2-1024k, rs-6-3-1024k; or rs-10-4-1024k." +
          " Provided configuration was: " + string);
    }

    try {
      codec = EcCodec.valueOf(matcher.group(1).toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("The codec " + matcher.group(1) +
          " is invalid. It must be one of " + EcCodec.allValuesAsString() + ".",
          e);
    }

    data = Integer.parseInt(matcher.group(2));
    parity = Integer.parseInt(matcher.group(3));
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
    ecChunkSize = chunkSize;
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
