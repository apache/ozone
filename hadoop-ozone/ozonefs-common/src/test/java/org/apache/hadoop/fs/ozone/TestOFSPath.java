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

package org.apache.hadoop.fs.ozone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OFSPath;
import org.junit.jupiter.api.Test;

/**
 * Testing basic functions of utility class OFSPath.
 */
public class TestOFSPath {

  private OzoneConfiguration conf = new OzoneConfiguration();

  @Test
  public void testParsingPathWithSpace() {
    // Two most common cases: file key and dir key inside a bucket
    OFSPath ofsPath = new OFSPath("/volume1/bucket2/dir3/key4 space", conf);
    assertEquals("", ofsPath.getAuthority());
    assertEquals("volume1", ofsPath.getVolumeName());
    assertEquals("bucket2", ofsPath.getBucketName());
    assertEquals("dir3/key4 space", ofsPath.getKeyName());
    assertEquals("/volume1/bucket2", ofsPath.getNonKeyPath());
    assertFalse(ofsPath.isMount());
    assertEquals("/volume1/bucket2/dir3/key4 space",
        ofsPath.toString());
  }

  @Test
  public void testParsingVolumeBucketWithKey() {
    // Two most common cases: file key and dir key inside a bucket
    OFSPath ofsPath = new OFSPath("/volume1/bucket2/dir3/key4", conf);
    assertEquals("", ofsPath.getAuthority());
    assertEquals("volume1", ofsPath.getVolumeName());
    assertEquals("bucket2", ofsPath.getBucketName());
    assertEquals("dir3/key4", ofsPath.getKeyName());
    assertEquals("/volume1/bucket2", ofsPath.getNonKeyPath());
    assertFalse(ofsPath.isMount());
    assertEquals("/volume1/bucket2/dir3/key4", ofsPath.toString());

    // The ending '/' matters for key inside a bucket, indicating directory
    ofsPath = new OFSPath("/volume1/bucket2/dir3/dir5/", conf);
    assertEquals("", ofsPath.getAuthority());
    assertEquals("volume1", ofsPath.getVolumeName());
    assertEquals("bucket2", ofsPath.getBucketName());
    // Check the key must end with '/' (dir5 is a directory)
    assertEquals("dir3/dir5/", ofsPath.getKeyName());
    assertEquals("/volume1/bucket2", ofsPath.getNonKeyPath());
    assertFalse(ofsPath.isMount());
    assertEquals("/volume1/bucket2/dir3/dir5/", ofsPath.toString());
  }

  @Test
  public void testParsingVolumeBucketOnly() {
    // Volume and bucket only
    OFSPath ofsPath = new OFSPath("/volume1/bucket2/", conf);
    assertEquals("", ofsPath.getAuthority());
    assertEquals("volume1", ofsPath.getVolumeName());
    assertEquals("bucket2", ofsPath.getBucketName());
    assertEquals("", ofsPath.getMountName());
    assertEquals("", ofsPath.getKeyName());
    assertEquals("/volume1/bucket2", ofsPath.getNonKeyPath());
    assertFalse(ofsPath.isMount());
    assertEquals("/volume1/bucket2/", ofsPath.toString());

    // The trailing '/' doesn't matter when parsing a bucket path
    ofsPath = new OFSPath("/volume1/bucket2", conf);
    assertEquals("", ofsPath.getAuthority());
    assertEquals("volume1", ofsPath.getVolumeName());
    assertEquals("bucket2", ofsPath.getBucketName());
    assertEquals("", ofsPath.getMountName());
    assertEquals("", ofsPath.getKeyName());
    assertEquals("/volume1/bucket2", ofsPath.getNonKeyPath());
    assertFalse(ofsPath.isMount());
    assertEquals("/volume1/bucket2/", ofsPath.toString());
  }

  @Test
  public void testParsingVolumeOnly() {
    // Volume only
    OFSPath ofsPath = new OFSPath("/volume1/", conf);
    assertEquals("", ofsPath.getAuthority());
    assertEquals("volume1", ofsPath.getVolumeName());
    assertEquals("", ofsPath.getBucketName());
    assertEquals("", ofsPath.getMountName());
    assertEquals("", ofsPath.getKeyName());
    assertEquals("/volume1/", ofsPath.getNonKeyPath());
    assertFalse(ofsPath.isMount());
    assertEquals("/volume1/", ofsPath.toString());

    // The trailing '/' doesn't matter when parsing a volume path
    ofsPath = new OFSPath("/volume1", conf);
    assertEquals("", ofsPath.getAuthority());
    assertEquals("volume1", ofsPath.getVolumeName());
    assertEquals("", ofsPath.getBucketName());
    assertEquals("", ofsPath.getMountName());
    assertEquals("", ofsPath.getKeyName());
    // Note: currently getNonKeyPath() returns with '/' if input is volume only.
    //  There is no use case for this for now.
    //  The behavior might change in the future.
    assertEquals("/volume1/", ofsPath.getNonKeyPath());
    assertFalse(ofsPath.isMount());
    assertEquals("/volume1/", ofsPath.toString());
  }

  @Test
  public void testParsingEmptyInput() {
    OFSPath ofsPath = new OFSPath("", conf);
    assertEquals("", ofsPath.getAuthority());
    assertEquals("", ofsPath.getVolumeName());
    assertEquals("", ofsPath.getBucketName());
    assertEquals("", ofsPath.getKeyName());
    assertEquals("", ofsPath.getNonKeyPath());
    assertEquals("", ofsPath.getNonKeyPathNoPrefixDelim());
    assertFalse(ofsPath.isMount());
    assertEquals("", ofsPath.toString());
  }

  @Test
  public void testParsingWithAuthority() {
    OFSPath ofsPath = new OFSPath("ofs://svc1:9876/volume1/bucket2/dir3/",
        conf);
    assertEquals("svc1:9876", ofsPath.getAuthority());
    assertEquals("volume1", ofsPath.getVolumeName());
    assertEquals("bucket2", ofsPath.getBucketName());
    assertEquals("dir3/", ofsPath.getKeyName());
    assertEquals("/volume1/bucket2", ofsPath.getNonKeyPath());
    assertFalse(ofsPath.isMount());
    assertEquals("ofs://svc1:9876/volume1/bucket2/dir3/",
        ofsPath.toString());
  }

  @Test
  public void testParsingMount() {
    String bucketName;
    try {
      bucketName = OFSPath.getTempMountBucketNameOfCurrentUser();
    } catch (IOException ex) {
      fail("Failed to get the current user name, "
          + "thus failed to get temp bucket name.");
      bucketName = "";  // Make javac happy
    }
    // Mount only
    OFSPath ofsPath = new OFSPath("/tmp/", conf);
    assertEquals("", ofsPath.getAuthority());
    assertEquals(
        OFSPath.OFS_MOUNT_TMP_VOLUMENAME, ofsPath.getVolumeName());
    assertEquals(bucketName, ofsPath.getBucketName());
    assertEquals("tmp", ofsPath.getMountName());
    assertEquals("", ofsPath.getKeyName());
    assertEquals("/tmp", ofsPath.getNonKeyPath());
    assertTrue(ofsPath.isMount());
    assertEquals("/tmp/", ofsPath.toString());

    // Mount with key
    ofsPath = new OFSPath("/tmp/key1", conf);
    assertEquals("", ofsPath.getAuthority());
    assertEquals(
        OFSPath.OFS_MOUNT_TMP_VOLUMENAME, ofsPath.getVolumeName());
    assertEquals(bucketName, ofsPath.getBucketName());
    assertEquals("tmp", ofsPath.getMountName());
    assertEquals("key1", ofsPath.getKeyName());
    assertEquals("/tmp", ofsPath.getNonKeyPath());
    assertTrue(ofsPath.isMount());
    assertEquals("/tmp/key1", ofsPath.toString());
  }
}
