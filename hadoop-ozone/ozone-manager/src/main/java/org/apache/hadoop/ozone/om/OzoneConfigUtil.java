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
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.Collection;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_GROUPS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ADMINISTRATORS_GROUPS;

/**
 * Utility class for ozone configurations.
 */
public final class OzoneConfigUtil {
  private OzoneConfigUtil() {
  }

  /**
   * Return list of OzoneAdministrators from config.
   */
  static Collection<String> getOzoneAdminsFromConfig(OzoneConfiguration conf)
          throws IOException {
    Collection<String> ozAdmins =
            conf.getTrimmedStringCollection(OZONE_ADMINISTRATORS);
    String omSPN = UserGroupInformation.getCurrentUser().getShortUserName();
    if (!ozAdmins.contains(omSPN)) {
      ozAdmins.add(omSPN);
    }
    return ozAdmins;
  }

  /**
   * Return list of s3 administrators prop from config.
   *
   * If ozone.s3.administrators value is empty string or unset,
   * defaults to ozone.administrators value.
   */
  static Collection<String> getS3AdminsFromConfig(OzoneConfiguration conf)
          throws IOException {
    Collection<String> ozAdmins =
            conf.getTrimmedStringCollection(OZONE_S3_ADMINISTRATORS);
    if (ozAdmins == null || ozAdmins.isEmpty()) {
      ozAdmins = conf.getTrimmedStringCollection(OZONE_ADMINISTRATORS);
    }
    String omSPN = UserGroupInformation.getCurrentUser().getShortUserName();
    if (!ozAdmins.contains(omSPN)) {
      ozAdmins.add(omSPN);
    }
    return ozAdmins;
  }

  static Collection<String> getOzoneAdminsGroupsFromConfig(
      OzoneConfiguration conf) {
    return conf.getTrimmedStringCollection(OZONE_ADMINISTRATORS_GROUPS);
  }

  static Collection<String> getS3AdminsGroupsFromConfig(
      OzoneConfiguration conf) {
    Collection<String> s3AdminsGroup =
            conf.getTrimmedStringCollection(OZONE_S3_ADMINISTRATORS_GROUPS);
    if (s3AdminsGroup.isEmpty() && conf
        .getTrimmedStringCollection(OZONE_S3_ADMINISTRATORS).isEmpty()) {
      s3AdminsGroup = conf
              .getTrimmedStringCollection(OZONE_ADMINISTRATORS_GROUPS);
    }
    return s3AdminsGroup;
  }

  public static ReplicationConfig resolveReplicationConfigPreference(
      HddsProtos.ReplicationType clientType,
      HddsProtos.ReplicationFactor clientFactor,
      HddsProtos.ECReplicationConfig clientECReplicationConfig,
      DefaultReplicationConfig bucketDefaultReplicationConfig,
      ReplicationConfig omDefaultReplicationConfig) {
    ReplicationConfig replicationConfig = null;
    if (clientType != HddsProtos.ReplicationType.NONE) {
      // Client passed the replication config, so let's use it.
      replicationConfig = ReplicationConfig
          .fromProto(clientType, clientFactor, clientECReplicationConfig);
    } else {
      // type is NONE, so, let's look for the bucket defaults.
      if (bucketDefaultReplicationConfig != null) {
        boolean hasECReplicationConfig = bucketDefaultReplicationConfig
            .getType() == ReplicationType.EC && bucketDefaultReplicationConfig
            .getEcReplicationConfig() != null;
        // Since Bucket defaults are available, let's inherit
        replicationConfig = ReplicationConfig.fromProto(
            ReplicationType.toProto(bucketDefaultReplicationConfig.getType()),
            ReplicationFactor
                .toProto(bucketDefaultReplicationConfig.getFactor()),
            hasECReplicationConfig ?
                bucketDefaultReplicationConfig.getEcReplicationConfig()
                    .toProto() :
                null);
      } else {
        // if bucket defaults also not available, then use server defaults.
        replicationConfig = omDefaultReplicationConfig;
      }
    }
    return replicationConfig;
  }
}
