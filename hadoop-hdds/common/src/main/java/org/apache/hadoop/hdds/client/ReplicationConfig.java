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

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import java.util.Objects;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE_DEFAULT;

/**
 * Replication configuration for any ReplicationType with all the required
 * parameters..
 */
public interface ReplicationConfig {

  /**
   * Helper method to create proper replication method from old-style
   * factor+type definition.
   * <p>
   * Note: it's never used for EC replication where config is created.
   */
  static ReplicationConfig fromProtoTypeAndFactor(
      HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor
  ) {
    switch (type) {
    case RATIS:
      return RatisReplicationConfig.getInstance(factor);
    case STAND_ALONE:
      return StandaloneReplicationConfig.getInstance(factor);
    default:
      throw new UnsupportedOperationException(
          "Not supported replication: " + type);
    }
  }

  /**
   * Helper method to create proper replication method from old-style
   * factor+type definition.
   * <p>
   * Note: it's never used for EC replication where config is created.
   */
  static ReplicationConfig fromTypeAndFactor(
      org.apache.hadoop.hdds.client.ReplicationType type,
      org.apache.hadoop.hdds.client.ReplicationFactor factor
  ) {
    return fromProtoTypeAndFactor(
        HddsProtos.ReplicationType.valueOf(type.name()),
        HddsProtos.ReplicationFactor.valueOf(factor.name()));
  }

  static ReplicationConfig getDefault(ConfigurationSource config) {
    String replication =
        config.get(OZONE_REPLICATION, OZONE_REPLICATION_DEFAULT);
    return parse(null, replication, config);
  }

  /**
   * Helper method to serialize from proto.
   * <p>
   * This uses either the old type/factor or the new ecConfig depends on the
   * type.
   */
  static ReplicationConfig fromProto(
      HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor,
      HddsProtos.ECReplicationConfig ecConfig) {
    switch (type) {
    case EC:
      return new ECReplicationConfig(ecConfig);
    case RATIS:
    case STAND_ALONE:
      return fromProtoTypeAndFactor(type, factor);
    default:
      throw new UnsupportedOperationException(
          "Not supported replication: " + type);
    }
  }

  static HddsProtos.ReplicationFactor getLegacyFactor(
      ReplicationConfig replicationConfig) {
    if (replicationConfig instanceof ReplicatedReplicationConfig) {
      return ((ReplicatedReplicationConfig) replicationConfig)
          .getReplicationFactor();
    }
    throw new UnsupportedOperationException(
        "Replication configuration of type "
            + replicationConfig.getReplicationType()
            + " does not have a replication factor property.");
  }

  /**
   * Create new replication config with adjusted replication factor.
   * <p>
   * Used by hadoop file system. Some replication schemes (like EC) may not
   * support changing the replication.
   * <p>
   * Based on the provided configuration the adjusted ReplicationConfig is
   * validated against the ozone.replication.allowed-configs property, and if
   * the new config is not allowed the method throws an
   * IllegalArgumentException.
   */
  static ReplicationConfig adjustReplication(
      ReplicationConfig config, short replication, ConfigurationSource conf) {
    ReplicationType replicationType =
        ReplicationType.valueOf(config.getReplicationType().toString());
    if (replicationType.equals(ReplicationType.EC)) {
      return config;
    }
    return parse(replicationType, Short.toString(replication), conf);
  }

  /**
   * Parses the string representation of the replication configuration that is
   * defined by the ReplicationType parameter.
   * The configuration object is necessary to check if the parsed
   * ReplicationConfig object is allowed based on the
   * ozone.replication.allowed-configs property.
   * @param type the ReplicationType to parse from the replication string
   * @param replication the replication String that for example contains the
   *                    replication factor for RATIS replication.
   * @param config the current Ozone configuration to apply validation on the
   *               parsed object.
   * @return a validated ReplicationConfig object that is allowed based on the
   *         system's configuration.
   * @throws IllegalArgumentException if the parsed ReplicationConfig is not
   *         allowed by the ozone.replication.allowed-configs property, or
   *         if the give replication type or replication can not be parsed.
   * @throws NullPointerException if the ReplicationConfig was not created
   *         for the type.
   */
  static ReplicationConfig parse(ReplicationType type, String replication,
      ConfigurationSource config) {
    if (type == null) {
      type = ReplicationType.valueOf(
          config.get(OZONE_REPLICATION_TYPE, OZONE_REPLICATION_TYPE_DEFAULT));
    }
    replication = Objects.toString(replication,
        config.get(OZONE_REPLICATION, OZONE_REPLICATION_DEFAULT));

    return parseWithoutFallback(type, replication, config);
  }

  static ReplicationConfig parseWithoutFallback(ReplicationType type,
      String replication, ConfigurationSource config) {

    if (replication == null) {
      throw new IllegalArgumentException(
          "Replication can't be null. Replication type passed was : " + type);
    }
    if (type == null) {
      throw new IllegalArgumentException(
          "Replication type must be specified for: " + replication);
    }

    ReplicationConfig replicationConfig;
    switch (type) {
    case RATIS:
    case STAND_ALONE:
      ReplicationFactor factor;
      try {
        factor = ReplicationFactor.valueOf(Integer.parseInt(replication));
      } catch (NumberFormatException ex) {
        try {
          factor = ReplicationFactor.valueOf(replication);
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException(replication +
              " is not supported for " + type + " replication type", e);
        }
      }
      replicationConfig = fromTypeAndFactor(type, factor);
      break;
    case EC:
      replicationConfig = new ECReplicationConfig(replication);
      break;
    default:
      throw new IllegalArgumentException("Replication type " + type +
              " can not be parsed.");
    }

    ReplicationConfigValidator validator =
        config.getObject(ReplicationConfigValidator.class);
    validator.validate(replicationConfig);

    return replicationConfig;
  }

  /**
   * Replication type supported by the replication config.
   */
  HddsProtos.ReplicationType getReplicationType();

  /**
   * Number of required nodes for this replication.
   */
  int getRequiredNodes();

  /**
   * Returns the replication option in string format.
   */
  String getReplication();

  String configFormat();

}
