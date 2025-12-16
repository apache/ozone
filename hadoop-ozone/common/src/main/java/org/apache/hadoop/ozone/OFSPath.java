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

package org.apache.hadoop.ozone;

import static org.apache.hadoop.fs.FileSystem.TRASH_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_INDICATOR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_OFS_SHARED_TMP_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_OFS_SHARED_TMP_DIR_DEFAULT;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.StringTokenizer;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.ParseException;

/**
 * Utility class for Rooted Ozone Filesystem (OFS) path processing.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class OFSPath {
  private String authority = "";
  /**
   * Here is a table illustrating what each name variable is given an input path
   * Assuming /tmp is mounted to /tempVol/tempBucket
   * (empty) = empty string "".
   *
   * Path                  volumeName   bucketName       mountName    keyName
   * --------------------------------------------------------------------------
   * /vol1/buc2/dir3/key4  vol1         buc2             (empty)      dir3/key4
   * /vol1/buc2            vol1         buc2             (empty)      (empty)
   * /vol1                 vol1         (empty)          (empty)      (empty)
   * /tmp/dir3/key4        tmp          md5(<username>)  tmp          dir3/key4
   *
   * Note the leading '/' doesn't matter.
   */
  private String volumeName = "";
  private String bucketName = "";
  private String mountName = "";
  private String keyName = "";
  private OzoneConfiguration conf;
  private static final String OFS_MOUNT_NAME_TMP = "tmp";
  // Hard-code the volume name to tmp for the first implementation
  @VisibleForTesting
  public static final String OFS_MOUNT_TMP_VOLUMENAME = "tmp";
  private static final String OFS_SHARED_TMP_BUCKETNAME = "tmp";
  // Hard-coded bucket name to use when OZONE_OM_ENABLE_OFS_SHARED_TMP_DIR
  // enabled;  HDDS-7746 to make this name configurable.

  public OFSPath(Path path, OzoneConfiguration conf) {
    this.conf = conf;
    initOFSPath(path.toUri(), false);
  }

  public OFSPath(String pathStr, OzoneConfiguration conf) {
    this.conf = conf;
    if (StringUtils.isEmpty(pathStr)) {
      return;
    }
    final Path fsPath = new Path(pathStr);
    // Preserve '/' at the end of a key if any, as fs.Path(String) discards it
    final boolean endsWithSlash = pathStr.endsWith(OZONE_URI_DELIMITER);
    initOFSPath(fsPath.toUri(), endsWithSlash);
  }

  public static boolean isSharedTmpBucket(OzoneObjInfo objInfo) {
    return OFS_MOUNT_TMP_VOLUMENAME.equals(objInfo.getVolumeName()) &&
        OFS_SHARED_TMP_BUCKETNAME.equals(objInfo.getBucketName());
  }

  private void initOFSPath(URI uri, boolean endsWithSlash) {
    // Scheme is case-insensitive
    String scheme = uri.getScheme();
    if (scheme != null && !scheme.equalsIgnoreCase(OZONE_OFS_URI_SCHEME)) {
      throw new ParseException("Can't parse schemes other than ofs.");
    }
    // authority could be empty
    authority = uri.getAuthority() == null ? "" : uri.getAuthority();
    final String pathStr = uri.getPath();
    StringTokenizer token = new StringTokenizer(pathStr, OZONE_URI_DELIMITER);
    int numToken = token.countTokens();

    if (numToken > 0) {
      String firstToken = token.nextToken();
      // TODO: Compare a list of mounts in the future.
      if (firstToken.equals(OFS_MOUNT_NAME_TMP)) {
        mountName = firstToken;
        // TODO: Make this configurable in the future.
        volumeName = OFS_MOUNT_TMP_VOLUMENAME;
        try {
          if (conf.getBoolean(OZONE_OM_ENABLE_OFS_SHARED_TMP_DIR,
              OZONE_OM_ENABLE_OFS_SHARED_TMP_DIR_DEFAULT)) {
            bucketName = OFS_SHARED_TMP_BUCKETNAME;
          } else {
            bucketName = getTempMountBucketNameOfCurrentUser();
          }
        } catch (IOException ex) {
          throw new ParseException(
              "Failed to get temp bucket name for current user.");
        }
      } else if (numToken >= 2) {
        // Regular volume and bucket path
        volumeName = firstToken;
        bucketName = token.nextToken();
      } else {
        // Volume only
        volumeName = firstToken;
      }
    }

    // Compose key name
    if (token.hasMoreTokens()) {
      keyName = token.nextToken("").substring(1);
      // Restore the '/' at the end
      if (endsWithSlash) {
        keyName += OZONE_URI_DELIMITER;
      }
    }
  }

  public String getAuthority() {
    return authority;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getMountName() {
    return mountName;
  }

  // Shouldn't have a delimiter at beginning e.g. dir1/dir12
  public String getKeyName() {
    return keyName;
  }

  private boolean isEmpty() {
    return getAuthority().isEmpty()
        && getMountName().isEmpty()
        && getVolumeName().isEmpty()
        && getBucketName().isEmpty()
        && getKeyName().isEmpty();
  }

  /**
   * Return the reconstructed path string.
   * Directories including volumes and buckets will have a trailing '/'.
   */
  @Override
  public String toString() {
    if (isEmpty()) {
      return "";
    }
    Objects.requireNonNull(authority, "authority == null");
    StringBuilder sb = new StringBuilder();
    if (!isMount()) {
      sb.append(volumeName);
      sb.append(OZONE_URI_DELIMITER);
      if (!bucketName.isEmpty()) {
        sb.append(bucketName);
        sb.append(OZONE_URI_DELIMITER);
      }
    } else {
      sb.append(mountName);
      sb.append(OZONE_URI_DELIMITER);
    }
    if (!keyName.isEmpty()) {
      sb.append(keyName);
    }
    if (authority.isEmpty()) {
      sb.insert(0, OZONE_URI_DELIMITER);
      return sb.toString();
    } else {
      final Path pathWithSchemeAuthority = new Path(
          OZONE_OFS_URI_SCHEME, authority, OZONE_URI_DELIMITER);
      sb.insert(0, pathWithSchemeAuthority.toString());
      return sb.toString();
    }
  }

  /**
   * Get the volume and bucket or mount name (non-key path).
   * @return String of path excluding key in bucket.
   */
  // Prepend a delimiter at beginning. e.g. /vol1/buc1
  public String getNonKeyPath() {
    if (isEmpty()) {
      return "";
    }
    return OZONE_URI_DELIMITER + getNonKeyPathNoPrefixDelim();
  }

  // Don't prepend the delimiter. e.g. vol1/buc1
  public String getNonKeyPathNoPrefixDelim() {
    if (isEmpty()) {
      return "";
    }
    if (isMount()) {
      return mountName;
    } else {
      return volumeName + OZONE_URI_DELIMITER + bucketName;
    }
  }

  public boolean isMount() {
    return !mountName.isEmpty();
  }

  private static boolean isInSameBucketAsInternal(
      OFSPath p1, OFSPath p2) {

    return p1.getVolumeName().equals(p2.getVolumeName()) &&
        p1.getBucketName().equals(p2.getBucketName());
  }

  /**
   * Check if this OFSPath is in the same bucket as another given OFSPath.
   * Note that mount name is resolved into volume and bucket names.
   * @return true if in the same bucket, false otherwise.
   */
  public boolean isInSameBucketAs(OFSPath p2) {
    return isInSameBucketAsInternal(this, p2);
  }

  /**
   * If both volume and bucket names are empty, the given path is root.
   * i.e. / is root.
   */
  public boolean isRoot() {
    return this.getVolumeName().isEmpty() && this.getBucketName().isEmpty();
  }

  /**
   * If bucket name is empty but volume name is not, the given path is a volume.
   * e.g. /volume1 is a volume.
   */
  public boolean isVolume() {
    return this.getBucketName().isEmpty() && !this.getVolumeName().isEmpty();
  }

  /**
   * If key name is empty but volume and bucket names are not, the given path
   * is a bucket.
   * e.g. /volume1/bucket2 is a bucket.
   */
  public boolean isBucket() {
    return this.getKeyName().isEmpty() &&
        !this.getBucketName().isEmpty() &&
        !this.getVolumeName().isEmpty();
  }

  /**
   * If volume and bucket names are not empty and the key name
   * only contains the snapshot indicator, then return true.
   * e.g. /vol/bucket/.snapshot is a snapshot path.
   */
  public boolean isSnapshotPath() {
    if (keyName.startsWith(OM_SNAPSHOT_INDICATOR)) {
      String[] keyNames = keyName.split(OZONE_URI_DELIMITER);

      if (keyNames.length == 1) {
        return  !bucketName.isEmpty() &&
                !volumeName.isEmpty();
      }
    }
    return false;
  }

  /**
   * If the path is a snapshot path get the snapshot name from the key name.
   */
  public String getSnapshotName() {
    if (keyName.startsWith(OM_SNAPSHOT_INDICATOR)) {
      if (!bucketName.isEmpty() && !volumeName.isEmpty()) {
        String[] keyNames = keyName.split(OZONE_URI_DELIMITER);
        return keyNames.length > 1 ? keyNames[1] : null;
      }
    }
    return null;
  }

  /**
   * If key name is not empty, the given path is a key.
   * e.g. /volume1/bucket2/key3 is a key.
   */
  public boolean isKey() {
    return !this.getKeyName().isEmpty();
  }

  private static String md5Hex(String input) {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
      BigInteger bigInt = new BigInteger(1, digest);
      StringBuilder sb = new StringBuilder(bigInt.toString(16));
      while (sb.length() < 32) {
        sb.insert(0, "0");
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Get the bucket name of temp for given username.
   * @param username Input user name String. Mustn't be null.
   * @return Username MD5 hash in hex digits.
   */
  @VisibleForTesting
  static String getTempMountBucketName(String username) {
    Objects.requireNonNull(username, "username == null");
    // TODO: Improve this to "slugify(username)-md5(username)" for better
    //  readability?
    return md5Hex(username);
  }

  /**
   * Get the bucket name of temp for the current user from UserGroupInformation.
   * @return Username MD5 hash in hex digits.
   * @throws IOException When UserGroupInformation.getCurrentUser() fails.
   */
  public static String getTempMountBucketNameOfCurrentUser()
      throws IOException {
    String username = UserGroupInformation.getCurrentUser().getShortUserName();
    return getTempMountBucketName(username);
  }

  /**
   * Return trash root for the given path.
   * @return trash root for the given path.
   */
  public Path getTrashRoot() {
    return getTrashRoot(null);
  }

  /**
   * Return trash root for the given path and username.
   * The username can be specified to use the proxy user instead of {@link UserGroupInformation#getCurrentUser()}.
   * @param username the username used to get the trash root path. If it is not specified,
   *                 will fall back to {@link UserGroupInformation#getCurrentUser()}.
   * @return trash root for the given path and username.
   */
  public Path getTrashRoot(String username) {
    if (!this.isKey()) {
      throw new RuntimeException("Recursive rm of volume or bucket with trash" +
          " enabled is not permitted. Consider using the -skipTrash option.");
    }
    try {
      if (StringUtils.isEmpty(username)) {
        username = UserGroupInformation.getCurrentUser().getShortUserName();
      }
      final Path pathRoot = new Path(
          OZONE_OFS_URI_SCHEME, authority, OZONE_URI_DELIMITER);
      final Path pathToVolume = new Path(pathRoot, volumeName);
      final Path pathToBucket = new Path(pathToVolume, bucketName);
      final Path pathToTrash = new Path(pathToBucket, TRASH_PREFIX);
      return new Path(pathToTrash, username);
    } catch (IOException ex) {
      throw new RuntimeException("getTrashRoot failed.", ex);
    }
  }
}
