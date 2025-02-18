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

package org.apache.hadoop.ozone.container.upgrade;

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;

/**
 * Util methods for upgrade.
 */
public final class UpgradeUtils {

  private UpgradeUtils() {
  }

  public static LayoutVersionProto defaultLayoutVersionProto() {
    return LayoutVersionProto.newBuilder()
        .setMetadataLayoutVersion(maxLayoutVersion())
        .setSoftwareLayoutVersion(maxLayoutVersion()).build();
  }

  public static LayoutVersionProto toLayoutVersionProto(int mLv, int sLv) {
    return LayoutVersionProto.newBuilder()
        .setMetadataLayoutVersion(mLv)
        .setSoftwareLayoutVersion(sLv).build();
  }
}
