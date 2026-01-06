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

package org.apache.hadoop.ozone.om.helpers;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.isValidKeyPath;
import static org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.normalizePrefix;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.ozone.om.exceptions.OMException;

/**
 * Utility class for Ozone Lifecycle.
 */
public final class OmLifecycleUtils {

  // Ref: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
  public static final int MAX_PREFIX_LENGTH = 1024;

  // Ref: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-tagging.html
  public static final int MAX_TAG_KEY_LENGTH = 128;
  public static final int MAX_TAG_VALUE_LENGTH = 256;

  private OmLifecycleUtils() {
  }

  /**
   * Check if the prefix is a Trash path.
   *
   * @param prefix the prefix to check
   * @throws OMException if the prefix is a trash path
   */
  public static void validateTrashPrefix(String prefix) throws OMException {
    if (org.apache.commons.lang3.StringUtils.isEmpty(prefix)) {
      return;
    }
    // Remove leading slash if present for validation
    String p = prefix.startsWith(OZONE_URI_DELIMITER) ? prefix.substring(1) : prefix;

    if (p.startsWith(FileSystem.TRASH_PREFIX + OZONE_URI_DELIMITER) ||
        p.equals(FileSystem.TRASH_PREFIX)) {
      throw new OMException("Lifecycle rule prefix cannot be trash root " +
          FileSystem.TRASH_PREFIX + OZONE_URI_DELIMITER, OMException.ResultCodes.INVALID_REQUEST);
    }
  }

  /**
   * Normalize and validate the prefix for FILE_SYSTEM_OPTIMIZED layout.
   *
   * @param prefix the prefix to validate
   * @throws OMException if the prefix is invalid
   */
  public static void validateAndNormalizePrefix(String prefix) throws OMException {
    String normalizedPrefix = normalizePrefix(prefix);
    if (!normalizedPrefix.equals(prefix)) {
      throw new OMException("Prefix format is not supported. Please use " + normalizedPrefix +
          " instead of " + prefix + ".", OMException.ResultCodes.INVALID_REQUEST);
    }
    try {
      isValidKeyPath(normalizedPrefix);
    } catch (OMException e) {
      throw new OMException("Prefix is not a valid key path: " + prefix, OMException.ResultCodes.INVALID_REQUEST);
    }
  }

  /**
   * Validate prefix length.
   *
   * @param prefix the prefix to validate
   * @throws OMException if the prefix length exceeds 1024
   */
  public static void validatePrefixLength(String prefix) throws OMException {
    if (prefix != null && prefix.getBytes(StandardCharsets.UTF_8).length > MAX_PREFIX_LENGTH) {
      throw new OMException("The maximum size of a prefix is " + MAX_PREFIX_LENGTH,
          OMException.ResultCodes.INVALID_REQUEST);
    }
  }

  /**
   * Validate tag key, value length and the uniqueness of the key.
   *
   * @param tags the tags to validate
   * @throws OMException if the tag key or value is invalid
   */
  public static void validateTagUniqAndLength(Map<String, String> tags) throws OMException {
    if (tags == null) {
      return;
    }
    Set<String> keys = new HashSet<>();
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();

      if (key == null || key.isEmpty() ||
          key.getBytes(StandardCharsets.UTF_8).length > MAX_TAG_KEY_LENGTH) {
        throw new OMException("A Tag's Key must be a length between 1 and " +
            MAX_TAG_KEY_LENGTH, OMException.ResultCodes.INVALID_REQUEST);
      }

      if (value != null && value.getBytes(StandardCharsets.UTF_8).length > MAX_TAG_VALUE_LENGTH) {
        throw new OMException("A Tag's Value must be a length between 0 and " +
            MAX_TAG_VALUE_LENGTH, OMException.ResultCodes.INVALID_REQUEST);
      }

      if (!keys.add(key)) {
        throw new OMException("Duplicate Tag Keys are not allowed",
            OMException.ResultCodes.INVALID_REQUEST);
      }
    }
  }
}

