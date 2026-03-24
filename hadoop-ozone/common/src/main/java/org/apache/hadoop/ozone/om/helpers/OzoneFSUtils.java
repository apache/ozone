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

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_KEY_NAME;

import jakarta.annotation.Nonnull;
import java.nio.file.Paths;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for OzoneFileSystem.
 */
public final class OzoneFSUtils {
  static final Logger LOG = LoggerFactory.getLogger(OzoneFSUtils.class);

  private OzoneFSUtils() { }

  /**
   * Returns string representation of path after removing the leading slash.
   */
  public static String pathToKey(Path path) {
    return path.toString().substring(1);
  }

  /**
   * Returns string representation of the input path parent. The function adds
   * a trailing slash if it does not exist and returns an empty string if the
   * parent is root.
   */
  public static String getParent(String keyName) {
    java.nio.file.Path parentDir = Paths.get(keyName).getParent();
    if (parentDir == null) {
      return "";
    }
    return addTrailingSlashIfNeeded(parentDir.toString());
  }

  /**
   * The function returns immediate child of given ancestor in a particular
   * descendant. For example if ancestor is /a/b and descendant is /a/b/c/d/e
   * the function should return /a/b/c/. If the descendant itself is the
   * immediate child then it is returned as is without adding a trailing slash.
   * This is done to distinguish files from a directory as in ozone files do
   * not carry a trailing slash.
   */
  public static String getImmediateChild(String descendant, String ancestor) {
    ancestor =
        !ancestor.isEmpty() ? addTrailingSlashIfNeeded(ancestor) : ancestor;
    if (!descendant.startsWith(ancestor)) {
      return null;
    }
    java.nio.file.Path descendantPath = Paths.get(descendant);
    java.nio.file.Path ancestorPath = Paths.get(ancestor);
    int ancestorPathNameCount =
        ancestor.isEmpty() ? 0 : ancestorPath.getNameCount();
    if (descendantPath.getNameCount() - ancestorPathNameCount > 1) {
      return addTrailingSlashIfNeeded(
          ancestor + descendantPath.getName(ancestorPathNameCount));
    }
    return descendant;
  }

  public static String addTrailingSlashIfNeeded(String key) {
    if (!key.endsWith(OZONE_URI_DELIMITER)) {
      return key + OZONE_URI_DELIMITER;
    } else {
      return key;
    }
  }

  public static boolean isFile(String keyName) {
    return !keyName.endsWith(OZONE_URI_DELIMITER);
  }

  /**
   * Core validation logic for key path components.
   * Checks for invalid characters: ".", "..", ":", "/", and "//" in the middle.
   *
   * @param path the path to validate
   * @param allowLeadingSlash whether leading slash is allowed (true) or invalid (false)
   * @return true if path components are valid, false otherwise
   */
  private static boolean validateKeyPathComponents(String path, boolean allowLeadingSlash) {
    if (allowLeadingSlash) {
      if (!path.startsWith(Path.SEPARATOR)) {
        return false;
      }
    } else {
      if (path.startsWith(Path.SEPARATOR)) {
        return false;
      }
    }

    String[] components = StringUtils.split(path, '/');
    for (int i = 0; i < components.length; i++) {
      String element = components[i];
      if (element.equals(".") ||
          element.contains(":") ||
          element.contains("/") ||
          element.equals("..")) {
        return false;
      }

      // The string may end with a /, but not have "//" in the middle.
      if (element.isEmpty() && i != components.length - 1) {
        // Allow empty at start for absolute paths (e.g., "/a/b" → ["", "a", "b"])
        // This is needed for isValidName but not for isValidKeyPath (which rejects leading slashes)
        if (allowLeadingSlash && i == 0) {
          continue;
        }
        return false;
      }
    }
    return true;
  }

  /**
   * Whether the pathname is valid.  Currently prohibits relative paths,
   * names which contain a ":" or "//", or other non-canonical paths.
   */
  public static boolean isValidName(String path) {
    return validateKeyPathComponents(path, true);
  }

  /**
   * Whether the pathname is valid.  Check key names which contain a
   * ":", ".", "..", "//", "". If it has any of these characters throws
   * OMException, else return the path.
   *
   * @param path the path to validate
   * @param throwOnEmpty whether to throw exception if path is empty
   * @return the path if valid
   * @throws OMException if path is invalid
   */
  public static String isValidKeyPath(String path, boolean throwOnEmpty) throws OMException {
    if (path.isEmpty()) {
      if (throwOnEmpty) {
        throw new OMException("Invalid KeyPath, empty keyName",
            INVALID_KEY_NAME);
      }
      return path;
    }

    if (!validateKeyPathComponents(path, false)) {
      throw new OMException("Invalid KeyPath " + path, INVALID_KEY_NAME);
    }

    return path;
  }

  /**
   * Checks whether the bucket layout is valid for File System operations
   * otherwise throws IllegalArgumentException.
   * Allowed bucket layouts are FILE_SYSTEM_OPTIMIZED and LEGACY.
   */
  public static void validateBucketLayout(String bucketName,
                                          BucketLayout bucketLayout) {
    if (bucketLayout.equals(BucketLayout.OBJECT_STORE)) {
      throw new IllegalArgumentException(
          "Bucket: " + bucketName + " has layout: " + bucketLayout +
              ", which does not support" +
              " file system semantics. Bucket Layout must be " +
              BucketLayout.FILE_SYSTEM_OPTIMIZED + " or "
              + BucketLayout.LEGACY + ".");
    }
  }

  /**
   * The function returns leaf node name from the given absolute path. For
   * example, the given key path '/a/b/c/d/e/file1' then it returns leaf node
   * name 'file1'.
   */
  public static String getFileName(@Nonnull String keyName) {
    java.nio.file.Path fileName = Paths.get(keyName).getFileName();
    if (fileName != null) {
      return fileName.toString();
    }
    // failed to converts a path key
    return keyName;
  }

  /**
   * Verifies whether the childKey is a sibling of a given
   * parentKey.
   *
   * @param parentKey parent key name
   * @param childKey  child key name
   * @return true if childKey is a sibling of parentKey
   */
  public static boolean isSibling(String parentKey, String childKey) {
    // Empty childKey has no parent, so just returning false.
    if (org.apache.commons.lang3.StringUtils.isBlank(childKey)) {
      return false;
    }
    java.nio.file.Path parentPath = Paths.get(parentKey);
    java.nio.file.Path childPath = Paths.get(childKey);

    java.nio.file.Path childParent = childPath.getParent();
    java.nio.file.Path parentParent = parentPath.getParent();

    if (childParent != null && parentParent != null) {
      return childParent.equals(parentParent);
    }

    return childParent == parentParent;
  }

  public static boolean isAncestorPath(String parentKey, String childKey) {
    // Empty childKey has no parent, so just returning false.
    if (org.apache.commons.lang3.StringUtils.isBlank(childKey)) {
      return false;
    }
    java.nio.file.Path parentPath = Paths.get(parentKey);
    java.nio.file.Path childPath = Paths.get(childKey);

    java.nio.file.Path childParent = childPath.getParent();
    java.nio.file.Path parentParent = parentPath.getParent();

    if (childParent != null && parentParent != null) {
      return childParent.startsWith(parentParent) ||
          childParent.equals(parentParent);
    }

    return childParent == parentParent;
  }

  /**
   * Verifies whether the childKey is an immediate path under the given
   * parentKey.
   *
   * @param parentKey parent key name
   * @param childKey  child key name
   * @return true if childKey is an immediate path under the given parentKey
   */
  public static boolean isImmediateChild(String parentKey, String childKey) {

    // Empty childKey has no parent, so just returning false.
    if (org.apache.commons.lang3.StringUtils.isBlank(childKey)) {
      return false;
    }
    java.nio.file.Path parentPath = Paths.get(parentKey);
    java.nio.file.Path childPath = Paths.get(childKey);

    java.nio.file.Path childParent = childPath.getParent();

    // Following are the valid parentKey formats:
    // parentKey="" or parentKey="/" or parentKey="/a" or parentKey="a"
    // Following are the valid childKey formats:
    // childKey="/" or childKey="/a/b" or childKey="a/b"
    if (org.apache.commons.lang3.StringUtils.isBlank(parentKey)) {
      return childParent == null ||
              OM_KEY_PREFIX.equals(childParent.toString());
    }

    return parentPath.equals(childParent);
  }

  /**
   * The function returns parent directory from the given absolute path. For
   * example, the given key path '/a/b/c/d/e/file1' then it returns parent
   * directory name as 'e'.
   *
   * @param keyName key name
   * @return parent directory. If not found then return keyName itself.
   */
  public static String getParentDir(@Nonnull String keyName) {
    java.nio.file.Path fileName = Paths.get(keyName).getParent();
    if (fileName != null) {
      return fileName.toString();
    }
    // no parent directory.
    return "";
  }

  /**
   * This function appends the given file name to the given key name path.
   *
   * @param keyName key name
   * @param fileName  file name
   * @return full path
   */
  public static String appendFileNameToKeyPath(String keyName,
                                               String fileName) {
    StringBuilder newToKeyName = new StringBuilder(keyName);
    newToKeyName.append(OZONE_URI_DELIMITER);
    newToKeyName.append(fileName);
    return newToKeyName.toString();
  }

  /**
   * Returns the number of path components in the given keyName.
   *
   * @param keyName keyname
   * @return path components count
   */
  public static int getFileCount(String keyName) {
    java.nio.file.Path keyPath = Paths.get(keyName);
    return keyPath.getNameCount();
  }

  public static String removeTrailingSlashIfNeeded(String key) {
    if (key.endsWith(OZONE_URI_DELIMITER)) {
      java.nio.file.Path keyPath = Paths.get(key);
      return keyPath.toString();
    } else {
      return key;
    }
  }

  public static String generateUniqueTempSnapshotName() {
    return "temp" + UUID.randomUUID() + SnapshotInfo.generateName(Time.now());
  }

  public static Path trimPathToDepth(Path path, int maxDepth) {
    Path res = path;
    while (res.depth() > maxDepth) {
      res = res.getParent();
    }
    return res;
  }

  /**
   * Helper method to return whether Hsync can be enabled.
   * And print warning when the config is ignored.
   */
  public static boolean canEnableHsync(ConfigurationSource conf, boolean isClient) {
    final String confKey = isClient ?
        "ozone.client.hbase.enhancements.allowed" :
        OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED;

    boolean confHBaseEnhancementsAllowed = conf.getBoolean(
        confKey, OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED_DEFAULT);

    boolean confHsyncEnabled = conf.getBoolean(
        OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED_DEFAULT);

    if (confHBaseEnhancementsAllowed) {
      return confHsyncEnabled;
    } else {
      if (confHsyncEnabled) {
        LOG.debug("Ignoring {} = {} because HBase enhancements are disallowed. To enable it, set {} = true as well.",
            OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true,
            confKey);
      }
      return false;
    }
  }
}
