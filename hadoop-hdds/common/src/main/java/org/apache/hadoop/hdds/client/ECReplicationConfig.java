/**
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

package org.apache.hadoop.hdds.client;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.logging.log4j.util.Strings;

import java.util.EnumSet;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Replication configuration for EC replication.
 */
public class ECReplicationConfig implements ReplicationConfig {

  // TODO - should this enum be defined in the protobuf rather than here? Right
  //        the proto will carry a string. It might be more flexible for the
  //        constants to be defined in code rather than proto?

  /**
   * Enum defining the allowed list of ECCodecs.
   */
  public enum EcCodec {
    RS, XOR;

    public static String allValuesAsString() {
      return EnumSet.allOf(EcCodec.class)
          .stream()
          .map(Enum::toString)
          .collect(Collectors.joining(","));
    }
  }

  // Acceptable patterns are like:
  //   rs-3-2-1024k
  //   RS-3-2-2048
  //   XOR-10-4-4096K
  private static final Pattern STRING_FORMAT
      = Pattern.compile("([a-zA-Z]+)-(\\d+)-(\\d+)-(\\d+)((?:k|K))?");

  private int data;

  private int parity;

  // TODO - the default chunk size is 4MB - is EC defaulting to 1MB or 4MB
  //        stripe width? Should we default this to the chunk size setting?
  private int stripeSize = 1024 * 1024;

  // TODO - should we have a config for the default, or does it matter if we
  //        always force the client to send rs-3-2-1024k for example?
  private EcCodec codec = EcCodec.RS;

  public ECReplicationConfig(int data, int parity) {
    this.data = data;
    this.parity = parity;
  }

  public ECReplicationConfig(int data, int parity, EcCodec codec,
      int stripeSize) {
    this.data = data;
    this.parity = parity;
    this.codec = codec;
    this.stripeSize = stripeSize;
  }

  /**
   * Create an ECReplicationConfig object from a string representing the
   * various parameters. Acceptable patterns are like:
   *     rs-3-2-1024k
   *     RS-3-2-2048
   *     XOR-10-4-4096K
   * IllegalArgumentException will be thrown if the passed string does not
   * match the defined pattern.
   * @param string
   */
  public ECReplicationConfig(String string) {
    final Matcher matcher = STRING_FORMAT.matcher(string);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("EC replication config should be " +
          "defined in the form rs-3-2-1024k, rs-6-3-1024; or rs-10-4-1024k");
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

    int stripe = Integer.parseInt((matcher.group(4)));
    if (stripe <= 0) {
      throw new IllegalArgumentException("The stripeSize (" + stripe + ") be " +
          "greater than zero");
    }
    if (matcher.group(5) != null) {
      // The "k" modifier is present, so multiple by 1024
      stripe = stripe * 1024;
    }
    stripeSize = stripe;
  }

  public ECReplicationConfig(
      HddsProtos.ECReplicationConfig ecReplicationConfig) {
    this.data = ecReplicationConfig.getData();
    this.parity = ecReplicationConfig.getParity();
    this.codec = EcCodec.valueOf(ecReplicationConfig.getCodec().toUpperCase());
    this.stripeSize = ecReplicationConfig.getStripeSize();
  }

  @Override
  public HddsProtos.ReplicationType getReplicationType() {
    return HddsProtos.ReplicationType.EC;
  }

  @Override
  public int getRequiredNodes() {
    return data + parity;
  }

  public HddsProtos.ECReplicationConfig toProto() {
    return HddsProtos.ECReplicationConfig.newBuilder()
        .setData(data)
        .setParity(parity)
        .setCodec(codec.toString())
        .setStripeSize(stripeSize)
        .build();
  }

  public int getData() {
    return data;
  }

  public int getParity() {
    return parity;
  }

  public int getStripeSize() {
    return stripeSize;
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
        && codec == that.getCodec() && stripeSize == that.getStripeSize();
  }

  @Override
  public int hashCode() {
    return Objects.hash(data, parity, codec, stripeSize);
  }

}
