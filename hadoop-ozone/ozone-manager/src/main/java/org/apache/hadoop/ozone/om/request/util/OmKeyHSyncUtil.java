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

package org.apache.hadoop.ozone.om.request.util;

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper methods related to OM key HSync.
 */
public final class OmKeyHSyncUtil {

  private static final Logger LOG = LoggerFactory.getLogger(OmKeyHSyncUtil.class);

  private OmKeyHSyncUtil() {
  }

  /**
   * Returns true if the key has been hsync'ed before (has metadata HSYNC_CLIENT_ID).
   * @param omKeyInfo OmKeyInfo
   * @param clientIdString Client ID String
   * @param dbOpenKey dbOpenKey
   */
  public static boolean isHSyncedPreviously(OmKeyInfo omKeyInfo, String clientIdString, String dbOpenKey) {
    // Check whether the key has been hsync'ed before
    final String previousHsyncClientId = omKeyInfo.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
    if (previousHsyncClientId != null) {
      if (clientIdString.equals(previousHsyncClientId)) {
        // Same client ID, no need to update OpenKeyTable. One less DB write
        return true;
      } else {
        // Sanity check. Should never enter
        LOG.warn("Client ID '{}' currently hsync'ing key does not match previous hsync client ID '{}'. dbOpenKey='{}'",
            clientIdString, previousHsyncClientId, dbOpenKey);
      }
    }
    return false;
  }
}
