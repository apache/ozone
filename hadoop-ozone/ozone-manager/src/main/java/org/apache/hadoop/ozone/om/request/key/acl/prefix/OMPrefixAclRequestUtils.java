/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.request.key.acl.prefix;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.ResolvedBucket;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

/**
 * Utilities related to OM Acl.
 */
public final class OMPrefixAclRequestUtils {

  private OMPrefixAclRequestUtils() {

  }

  /**
   * Get resolved prefix path from origin prefix and resolvedBucket.
   * @param originPrefixPath originPrefixPath.
   * @param resolvedBucket resolvedBucket corresponding to originPrefixPath.
   * @return resolvedPrefixPath.
   */
  public static String resolvedPrefixPath(String originPrefixPath,
      ResolvedBucket resolvedBucket) {
    String[] tokens =
        StringUtils.split(originPrefixPath, OZONE_URI_DELIMITER, 3);
    if (tokens.length == 2) {
      return OZONE_URI_DELIMITER + resolvedBucket.realVolume()
          + OZONE_URI_DELIMITER + resolvedBucket.realBucket();
    } else if (tokens.length > 2) {
      return OZONE_URI_DELIMITER + resolvedBucket.realVolume()
          + OZONE_URI_DELIMITER + resolvedBucket.realBucket()
          + tokens[2];
    }
    return originPrefixPath;
  }

}
