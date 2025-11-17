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
