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

import org.apache.hadoop.ozone.OFSPath;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Testing basic functions of utility class OFSPath.
 */
public class TestOFSPath {

  @Test
  public void testParsingVolumeBucketWithKey() {
    // Two most common cases: file key and dir key inside a bucket
    OFSPath ofsPath = new OFSPath("/volume1/bucket2/dir3/key4");
    Assert.assertEquals("", ofsPath.getAuthority());
    Assert.assertEquals("volume1", ofsPath.getVolumeName());
    Assert.assertEquals("bucket2", ofsPath.getBucketName());
    Assert.assertEquals("dir3/key4", ofsPath.getKeyName());
    Assert.assertEquals("/volume1/bucket2", ofsPath.getNonKeyPath());
    Assert.assertFalse(ofsPath.isMount());
    Assert.assertEquals("/volume1/bucket2/dir3/key4", ofsPath.toString());

    // The ending '/' matters for key inside a bucket, indicating directory
    ofsPath = new OFSPath("/volume1/bucket2/dir3/dir5/");
    Assert.assertEquals("", ofsPath.getAuthority());
    Assert.assertEquals("volume1", ofsPath.getVolumeName());
    Assert.assertEquals("bucket2", ofsPath.getBucketName());
    // Check the key must end with '/' (dir5 is a directory)
    Assert.assertEquals("dir3/dir5/", ofsPath.getKeyName());
    Assert.assertEquals("/volume1/bucket2", ofsPath.getNonKeyPath());
    Assert.assertFalse(ofsPath.isMount());
    Assert.assertEquals("/volume1/bucket2/dir3/dir5/", ofsPath.toString());
  }

  @Test
  public void testParsingVolumeBucketOnly() {
    // Volume and bucket only
    OFSPath ofsPath = new OFSPath("/volume1/bucket2/");
    Assert.assertEquals("", ofsPath.getAuthority());
    Assert.assertEquals("volume1", ofsPath.getVolumeName());
    Assert.assertEquals("bucket2", ofsPath.getBucketName());
    Assert.assertEquals("", ofsPath.getMountName());
    Assert.assertEquals("", ofsPath.getKeyName());
    Assert.assertEquals("/volume1/bucket2", ofsPath.getNonKeyPath());
    Assert.assertFalse(ofsPath.isMount());
    Assert.assertEquals("/volume1/bucket2/", ofsPath.toString());

    // The trailing '/' doesn't matter when parsing a bucket path
    ofsPath = new OFSPath("/volume1/bucket2");
    Assert.assertEquals("", ofsPath.getAuthority());
    Assert.assertEquals("volume1", ofsPath.getVolumeName());
    Assert.assertEquals("bucket2", ofsPath.getBucketName());
    Assert.assertEquals("", ofsPath.getMountName());
    Assert.assertEquals("", ofsPath.getKeyName());
    Assert.assertEquals("/volume1/bucket2", ofsPath.getNonKeyPath());
    Assert.assertFalse(ofsPath.isMount());
    Assert.assertEquals("/volume1/bucket2/", ofsPath.toString());
  }

  @Test
  public void testParsingVolumeOnly() {
    // Volume only
    OFSPath ofsPath = new OFSPath("/volume1/");
    Assert.assertEquals("", ofsPath.getAuthority());
    Assert.assertEquals("volume1", ofsPath.getVolumeName());
    Assert.assertEquals("", ofsPath.getBucketName());
    Assert.assertEquals("", ofsPath.getMountName());
    Assert.assertEquals("", ofsPath.getKeyName());
    Assert.assertEquals("/volume1/", ofsPath.getNonKeyPath());
    Assert.assertFalse(ofsPath.isMount());
    Assert.assertEquals("/volume1/", ofsPath.toString());

    // The trailing '/' doesn't matter when parsing a volume path
    ofsPath = new OFSPath("/volume1");
    Assert.assertEquals("", ofsPath.getAuthority());
    Assert.assertEquals("volume1", ofsPath.getVolumeName());
    Assert.assertEquals("", ofsPath.getBucketName());
    Assert.assertEquals("", ofsPath.getMountName());
    Assert.assertEquals("", ofsPath.getKeyName());
    // Note: currently getNonKeyPath() returns with '/' if input is volume only.
    //  There is no use case for this for now.
    //  The behavior might change in the future.
    Assert.assertEquals("/volume1/", ofsPath.getNonKeyPath());
    Assert.assertFalse(ofsPath.isMount());
    Assert.assertEquals("/volume1/", ofsPath.toString());
  }

  @Test
  public void testParsingWithAuthority() {
    OFSPath ofsPath = new OFSPath("ofs://svc1:9876/volume1/bucket2/dir3/");
    Assert.assertEquals("svc1:9876", ofsPath.getAuthority());
    Assert.assertEquals("volume1", ofsPath.getVolumeName());
    Assert.assertEquals("bucket2", ofsPath.getBucketName());
    Assert.assertEquals("dir3/", ofsPath.getKeyName());
    Assert.assertEquals("/volume1/bucket2", ofsPath.getNonKeyPath());
    Assert.assertFalse(ofsPath.isMount());
    Assert.assertEquals("ofs://svc1:9876/volume1/bucket2/dir3/",
        ofsPath.toString());
  }

  @Test
  public void testParsingMount() {
    String bucketName;
    try {
      bucketName = OFSPath.getTempMountBucketNameOfCurrentUser();
    } catch (IOException ex) {
      Assert.fail("Failed to get the current user name, "
          + "thus failed to get temp bucket name.");
      bucketName = "";  // Make javac happy
    }
    // Mount only
    OFSPath ofsPath = new OFSPath("/tmp/");
    Assert.assertEquals("", ofsPath.getAuthority());
    Assert.assertEquals(
        OFSPath.OFS_MOUNT_TMP_VOLUMENAME, ofsPath.getVolumeName());
    Assert.assertEquals(bucketName, ofsPath.getBucketName());
    Assert.assertEquals("tmp", ofsPath.getMountName());
    Assert.assertEquals("", ofsPath.getKeyName());
    Assert.assertEquals("/tmp", ofsPath.getNonKeyPath());
    Assert.assertTrue(ofsPath.isMount());
    Assert.assertEquals("/tmp/", ofsPath.toString());

    // Mount with key
    ofsPath = new OFSPath("/tmp/key1");
    Assert.assertEquals("", ofsPath.getAuthority());
    Assert.assertEquals(
        OFSPath.OFS_MOUNT_TMP_VOLUMENAME, ofsPath.getVolumeName());
    Assert.assertEquals(bucketName, ofsPath.getBucketName());
    Assert.assertEquals("tmp", ofsPath.getMountName());
    Assert.assertEquals("key1", ofsPath.getKeyName());
    Assert.assertEquals("/tmp", ofsPath.getNonKeyPath());
    Assert.assertTrue(ofsPath.isMount());
    Assert.assertEquals("/tmp/key1", ofsPath.toString());
  }
}
