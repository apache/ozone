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

package org.apache.hadoop.ozone.nfs.gateway;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE_DEFAULT;

import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientUtils;

/** Resolves the RATIS replication used for NFS-published and staged data. */
final class NfsReplicationConfig {
  private NfsReplicationConfig() {
  }

  static ReplicationConfig forBucket(OzoneBucket bucket, ConfigurationSource configuration) throws IOException {
    ReplicationConfig replication = bucket.getReplicationConfig();
    if (replication == null) {
      replication = OzoneClientUtils.getClientConfiguredReplicationConfig(configuration);
    }
    if (replication == null) {
      String type = configuration.get(OZONE_REPLICATION_TYPE, OZONE_REPLICATION_TYPE_DEFAULT);
      if (!ReplicationType.RATIS.toString().equalsIgnoreCase(type)) {
        throw new IOException("NFS writes require RATIS replication");
      }
      String factor = configuration.get(OZONE_REPLICATION, OZONE_REPLICATION_DEFAULT);
      replication = RatisReplicationConfig.getInstance(parseFactor(factor).toProto());
    }
    if (!(replication instanceof RatisReplicationConfig)) {
      throw new IOException("NFS writes require RATIS replication");
    }
    return replication;
  }

  private static ReplicationFactor parseFactor(String factor) throws IOException {
    try {
      return ReplicationFactor.valueOf(factor.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      try {
        return ReplicationFactor.valueOf(Integer.parseInt(factor));
      } catch (IllegalArgumentException ex) {
        throw new IOException("Invalid NFS RATIS replication factor: " + factor, ex);
      }
    }
  }
}
