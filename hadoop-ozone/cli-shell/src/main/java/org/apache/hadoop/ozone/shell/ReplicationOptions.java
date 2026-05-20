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

package org.apache.hadoop.ozone.shell;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;

import java.util.Optional;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.client.OzoneClientUtils;

/**
 * Common options for specifying replication config: specialized for
 * Ozone Shell and Freon commands.
 */
public abstract class ReplicationOptions {

  protected static final String REPLICATION_DESCRIPTION =
      "Replication definition. Valid values are replication"
          + " type-specific.  For RATIS: ONE or THREE."
          + " In case of EC, pass CODEC-DATA-PARITY-CHUNKSIZE, "
          + " e.g. rs-3-2-1024k, rs-6-3-1024k, rs-10-4-1024k";
  protected static final String TYPE_DESCRIPTION =
      "Replication type. Supported types are: RATIS, EC";

  private ReplicationType type;
  private String replication;

  public Optional<ReplicationConfig> fromParams(ConfigurationSource conf) {
    if (replication == null && type == null) {
      return Optional.empty();
    }

    // special case for backward compatibility: factor from config for RATIS
    if (ReplicationType.RATIS == type && replication == null) {
      replication = conf.get(OZONE_REPLICATION, OZONE_REPLICATION_DEFAULT);
    }

    return Optional.of(
        ReplicationConfig.parseWithoutFallback(type, replication, conf));
  }

  public Optional<ReplicationConfig> fromConfig(ConfigurationSource conf) {
    ReplicationType defaultType = Optional
        .ofNullable(conf.get(OZONE_REPLICATION_TYPE))
        .map(ReplicationType::valueOf)
        .orElse(null);
    String defaultReplication =
        conf.get(OZONE_REPLICATION, OZONE_REPLICATION_DEFAULT);

    return Optional.ofNullable(
        OzoneClientUtils.validateAndGetClientReplicationConfig(
            defaultType, defaultReplication, conf));
  }

  public ReplicationConfig fromParamsOrConfig(ConfigurationSource conf) {
    return fromParams(conf)
        .orElseGet(() -> fromConfig(conf)
            .orElse(null));
  }

  // Option is defined in subclasses
  protected void setReplication(String replication) {
    this.replication = replication;
  }

  // Option is defined in subclasses
  protected void setType(String type) {
    try {
      ReplicationType replicationType = ReplicationType.valueOf(type);
      if (replicationType == ReplicationType.CHAINED
              || replicationType == ReplicationType.STAND_ALONE) {
        throw new IllegalArgumentException(
                String.format("Unsupported replication type %s",
                        replicationType.name()));
      }
      this.type = replicationType;
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException(
              String.format("Invalid value '%s' for option '--type'. %s." +
                      " Supported values are: RATIS, EC.",
                      type, ex.getMessage()));
    }
  }
}
