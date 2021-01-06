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
package org.apache.hadoop.ozone;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.ParseException;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;

import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.StringTokenizer;

import static org.apache.hadoop.fs.FileSystem.TRASH_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

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
  private static final String OFS_MOUNT_NAME_TMP = "tmp";
  // Hard-code the volume name to tmp for the first implementation
  @VisibleForTesting
  public static final String OFS_MOUNT_TMP_VOLUMENAME = "tmp";

  public OFSPath(Path path) {
    initOFSPath(path.toUri());
  }

  public OFSPath(String pathStr) {
    try {
      initOFSPath(new URI(pathStr));
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void initOFSPath(URI uri) {
    // Scheme is case-insensitive
    String scheme = uri.getScheme();
    if (scheme != null) {
      if (!scheme.toLowerCase().equals(OZONE_OFS_URI_SCHEME)) {
        throw new ParseException("Can't parse schemes other than ofs://.");
      }
    }
    // authority could be empty
    authority = uri.getAuthority() == null ? "" : uri.getAuthority();
    String pathStr = uri.getPath();
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
          bucketName = getTempMountBucketNameOfCurrentUser();
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

  /**
   * Return the reconstructed path string.
   * Directories including volumes and buckets will have a trailing '/'.
   */
  @Override
  public String toString() {
    Preconditions.checkNotNull(authority);
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
   * Get the volume & bucket or mount name (non-key path).
   * @return String of path excluding key in bucket.
   */
  // Prepend a delimiter at beginning. e.g. /vol1/buc1
  public String getNonKeyPath() {
    return OZONE_URI_DELIMITER + getNonKeyPathNoPrefixDelim();
  }

  // Don't prepend the delimiter. e.g. vol1/buc1
  public String getNonKeyPathNoPrefixDelim() {
    if (isMount()) {
      return mountName;
    } else {
      return volumeName + OZONE_URI_DELIMITER + bucketName;
    }
  }

  public boolean isMount() {
    return mountName.length() > 0;
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
    Preconditions.checkNotNull(username);
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
    String username = UserGroupInformation.getCurrentUser().getUserName();
    return getTempMountBucketName(username);
  }

  /**
   * Return trash root for the given path.
   * @return trash root for the given path.
   */
  public Path getTrashRoot() {
    if (!this.isKey()) {
      throw new RuntimeException("Volume or bucket doesn't have trash root.");
    }
    try {
      String username = UserGroupInformation.getCurrentUser().getUserName();
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
