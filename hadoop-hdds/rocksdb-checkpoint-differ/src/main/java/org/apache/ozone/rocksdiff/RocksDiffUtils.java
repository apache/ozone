/*
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
package org.apache.ozone.rocksdiff;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Helper methods for snap-diff operations.
 */
public final class RocksDiffUtils {

  private RocksDiffUtils() {
  }

  public static boolean isKeyWithPrefixPresent(String prefixForColumnFamily,
      String firstDbKey, String lastDbKey) {
    return firstDbKey.compareTo(prefixForColumnFamily) <= 0
        && prefixForColumnFamily.compareTo(lastDbKey) <= 0;
  }

  public static String constructBucketKey(String keyName) {
    if (!keyName.startsWith(OzoneConsts.OM_KEY_PREFIX)) {
      keyName = OzoneConsts.OM_KEY_PREFIX.concat(keyName);
    }
    String[] elements = keyName.split(OzoneConsts.OM_KEY_PREFIX);
    String volume = elements[1];
    String bucket = elements[2];
    StringBuilder builder =
        new StringBuilder().append(OzoneConsts.OM_KEY_PREFIX).append(volume);

    if (StringUtils.isNotBlank(bucket)) {
      builder.append(OzoneConsts.OM_KEY_PREFIX).append(bucket);
    }
    return builder.toString();
  }


}
