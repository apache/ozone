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
package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import java.util.StringTokenizer;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

/**
 * Utility class for Rooted Ozone Filesystem (OFS) path processing.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class OFSPath {
  private String volumeName;
  private String bucketName;
  private String mountName;
  private String keyName;

  private static final String OFS_MOUNT_NAME_TMP = "tmp";

  OFSPath(Path path) {
    String pathStr = path.toUri().getPath();
    initOFSPath(pathStr);
  }

  OFSPath(String pathStr) {
    initOFSPath(pathStr);
  }

  private void initOFSPath(String pathStr) {
    StringTokenizer token = new StringTokenizer(pathStr, OZONE_URI_DELIMITER);
    int numToken = token.countTokens();
    if (numToken > 0) {
      String firstToken = token.nextToken();
      // TODO: Compare a keyword list instead for future expansion.
      if (firstToken.equals(OFS_MOUNT_NAME_TMP)) {
        // TODO: Retrieve volume and bucket name with the mount name.
        //  Set to null just for now.
        volumeName = null;
        bucketName = null;
        mountName = firstToken;
      } else if (numToken >= 2) {
        // Regular volume and bucket path
        volumeName = firstToken;
        bucketName = token.nextToken();
        mountName = null;
      } else {
        // Volume only.
        volumeName = firstToken;
        bucketName = null;
        mountName = null;
      }
//    } else {  // TODO: Implement '/' case for ls.
    }

    // Compose key name.
    if (token.hasMoreTokens()) {
      keyName = token.nextToken("").substring(1);
    } else {
      keyName = "";  // Assign empty String, shouldn't be null.
    }
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
    return mountName != null;
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
}
