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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKey;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKeyBucket;

/**
 * The expired open keys.
 *
 * @see OMConfigKeys#OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD_DEFAULT
 */
public class ExpiredOpenKeys {
  private final Map<String, OpenKeyBucket.Builder> openKeyBuckets
      = new HashMap<>();
  private final List<CommitKeyRequest.Builder> hsyncKeys
      = new ArrayList<>();

  /** @return non-hsync'ed open keys. */
  public Collection<OpenKeyBucket.Builder> getOpenKeyBuckets() {
    return openKeyBuckets.values();
  }

  /** @return hsync'ed open keys. */
  public List<CommitKeyRequest.Builder> getHsyncKeys() {
    return hsyncKeys;
  }

  void addOpenKey(OmKeyInfo info, String dbOpenKeyName) {
    final String mapKey = info.getVolumeName() + OM_KEY_PREFIX
        + info.getBucketName();
    openKeyBuckets.computeIfAbsent(mapKey, k -> OpenKeyBucket.newBuilder()
            .setVolumeName(info.getVolumeName())
            .setBucketName(info.getBucketName()))
        .addKeys(OpenKey.newBuilder().setName(dbOpenKeyName));
  }

  void addHsyncKey(KeyArgs.Builder keyArgs, long clientId) {
    hsyncKeys.add(CommitKeyRequest.newBuilder()
        .setKeyArgs(keyArgs)
        .setClientID(clientId));
  }
}
