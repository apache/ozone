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

import org.junit.Assert;
import org.junit.Test;

/**
 * Testing basic functions of utility class OFSPath.
 */
public class TestOFSPath {

  @Test
  public void testParsingVolumeBucketWithKey() {
    // Two most common cases: file key and dir key inside a bucket
    OFSPath ofsPath = new OFSPath("/volume1/bucket2/dir3/key4");
    Assert.assertEquals("volume1", ofsPath.getVolumeName());
    Assert.assertEquals("bucket2", ofsPath.getBucketName());
    Assert.assertEquals("dir3/key4", ofsPath.getKeyName());
    Assert.assertEquals("/volume1/bucket2", ofsPath.getNonKeyPath());
    Assert.assertFalse(ofsPath.isMount());

    // The ending '/' matters for key inside a bucket, indicating directory
    ofsPath = new OFSPath("/volume1/bucket2/dir3/dir5/");
    Assert.assertEquals("volume1", ofsPath.getVolumeName());
    Assert.assertEquals("bucket2", ofsPath.getBucketName());
    // Check the key must end with '/' (dir5 is a directory)
    Assert.assertEquals("dir3/dir5/", ofsPath.getKeyName());
    Assert.assertEquals("/volume1/bucket2", ofsPath.getNonKeyPath());
    Assert.assertFalse(ofsPath.isMount());
  }

  @Test
  public void testParsingVolumeBucketOnly() {
    // Volume and bucket only
    OFSPath ofsPath = new OFSPath("/volume1/bucket2/");
    Assert.assertEquals("volume1", ofsPath.getVolumeName());
    Assert.assertEquals("bucket2", ofsPath.getBucketName());
    Assert.assertEquals("", ofsPath.getMountName());
    Assert.assertEquals("", ofsPath.getKeyName());
    Assert.assertEquals("/volume1/bucket2", ofsPath.getNonKeyPath());
    Assert.assertFalse(ofsPath.isMount());

    // The ending '/' shouldn't for buckets
    ofsPath = new OFSPath("/volume1/bucket2");
    Assert.assertEquals("volume1", ofsPath.getVolumeName());
    Assert.assertEquals("bucket2", ofsPath.getBucketName());
    Assert.assertEquals("", ofsPath.getMountName());
    Assert.assertEquals("", ofsPath.getKeyName());
    Assert.assertEquals("/volume1/bucket2", ofsPath.getNonKeyPath());
    Assert.assertFalse(ofsPath.isMount());
  }

  @Test
  public void testParsingVolumeOnly() {
    // Volume only
    OFSPath ofsPath = new OFSPath("/volume1/");
    Assert.assertEquals("volume1", ofsPath.getVolumeName());
    Assert.assertEquals("", ofsPath.getBucketName());
    Assert.assertEquals("", ofsPath.getMountName());
    Assert.assertEquals("", ofsPath.getKeyName());
    Assert.assertEquals("/volume1/", ofsPath.getNonKeyPath());
    Assert.assertFalse(ofsPath.isMount());

    // Ending '/' shouldn't matter
    ofsPath = new OFSPath("/volume1");
    Assert.assertEquals("volume1", ofsPath.getVolumeName());
    Assert.assertEquals("", ofsPath.getBucketName());
    Assert.assertEquals("", ofsPath.getMountName());
    Assert.assertEquals("", ofsPath.getKeyName());
    // Note: currently getNonKeyPath() returns with '/' if input is volume only.
    //  There is no use case for this for now.
    //  The behavior might change in the future.
    Assert.assertEquals("/volume1/", ofsPath.getNonKeyPath());
    Assert.assertFalse(ofsPath.isMount());
  }

  @Test
  public void testParsingWithAuthority() {
    try {
      new OFSPath("ofs://svc1/volume1/bucket1/dir1/");
      Assert.fail(
          "Should have thrown exception when parsing path with authority.");
    } catch (Exception ignored) {
      // Test pass
    }
  }

  @Test
  public void testParsingMount() {
    // Mount only
    OFSPath ofsPath = new OFSPath("/tmp/");
    // TODO: Subject to change in HDDS-2929.
    Assert.assertEquals("tempVolume", ofsPath.getVolumeName());
    Assert.assertEquals("tempBucket", ofsPath.getBucketName());
    Assert.assertEquals("tmp", ofsPath.getMountName());
    Assert.assertEquals("", ofsPath.getKeyName());
    Assert.assertEquals("/tmp", ofsPath.getNonKeyPath());
    Assert.assertTrue(ofsPath.isMount());

    // Mount with key
    ofsPath = new OFSPath("/tmp/key1");
    // TODO: Subject to change in HDDS-2929.
    Assert.assertEquals("tempVolume", ofsPath.getVolumeName());
    Assert.assertEquals("tempBucket", ofsPath.getBucketName());
    Assert.assertEquals("tmp", ofsPath.getMountName());
    Assert.assertEquals("key1", ofsPath.getKeyName());
    Assert.assertEquals("/tmp", ofsPath.getNonKeyPath());
    Assert.assertTrue(ofsPath.isMount());
  }
}
