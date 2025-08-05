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

package org.apache.hadoop.ozone.om.request;

import static org.apache.hadoop.ozone.OzoneConsts.ETAG;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.om.helpers.OMAuditLogger;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;

/**
 * Interface for OM Requests to convert to audit objects.
 */
public interface RequestAuditor {

  /**
   * Build AuditMessage.
   * @param op
   * @param auditMap
   * @param throwable
   * @param userInfo
   */
  OMAuditLogger.Builder buildAuditMessage(
      AuditAction op, Map<String, String> auditMap, Throwable throwable, UserInfo userInfo);

  /**
   * Build auditMap with specified volume.
   * @param volume
   * @return auditMap.
   */
  Map<String, String> buildVolumeAuditMap(String volume);

  /**
   * Build auditMap for KeyArgs.
   * @param keyArgs
   */
  default Map<String, String> buildLightKeyArgsAuditMap(KeyArgs keyArgs) {
    if (keyArgs == null) {
      return new HashMap<>(0);
    } else {
      Map<String, String> auditMap = new LinkedHashMap<>();
      auditMap.put(OzoneConsts.VOLUME, keyArgs.getVolumeName());
      auditMap.put(OzoneConsts.BUCKET, keyArgs.getBucketName());
      auditMap.put(OzoneConsts.KEY, keyArgs.getKeyName());
      return auditMap;
    }
  }

  /**
   * Build auditMap for KeyArgs.
   * @param keyArgs
   */
  default Map<String, String> buildKeyArgsAuditMap(KeyArgs keyArgs) {
    if (keyArgs == null) {
      return new HashMap<>(0);
    } else {
      Map< String, String > auditMap = buildLightKeyArgsAuditMap(keyArgs);
      auditMap.put(OzoneConsts.DATA_SIZE,
          String.valueOf(keyArgs.getDataSize()));
      if (keyArgs.hasType()) {
        auditMap.put(OzoneConsts.REPLICATION_TYPE, keyArgs.getType().name());
      }
      if (keyArgs.hasFactor() && keyArgs.getFactor() != HddsProtos.ReplicationFactor.ZERO) {
        auditMap.put(OzoneConsts.REPLICATION_FACTOR, keyArgs.getFactor().name());
      }
      if (keyArgs.hasEcReplicationConfig()) {
        auditMap.put(OzoneConsts.REPLICATION_CONFIG,
            ECReplicationConfig.toString(keyArgs.getEcReplicationConfig()));
      }
      if (keyArgs.hasExpectedDataGeneration()) {
        auditMap.put(OzoneConsts.REWRITE_GENERATION,
            String.valueOf(keyArgs.getExpectedDataGeneration()));
      }
      for (HddsProtos.KeyValue item : keyArgs.getMetadataList()) {
        if (ETAG.equals(item.getKey())) {
          auditMap.put(ETAG, item.getValue());
        }
      }
      return auditMap;
    }
  }
}
