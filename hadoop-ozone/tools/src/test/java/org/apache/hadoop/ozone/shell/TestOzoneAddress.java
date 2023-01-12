/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.shell;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.ozone.client.OzoneClientException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test ozone URL parsing.
 */
@RunWith(Parameterized.class)
public class TestOzoneAddress {

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {"o3://localhost:9878/"},
        {"o3://localhost/"},
        {"o3:///"},
        {"/"},
        {""}
    });
  }

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private final String prefix;
  private OzoneAddress address;

  public TestOzoneAddress(String prefix) {
    this.prefix = prefix;
  }

  @Test
  public void checkRootUrlType() throws OzoneClientException {
    address = new OzoneAddress("");
    address.ensureRootAddress();

    address = new OzoneAddress(prefix + "");
    address.ensureRootAddress();
  }

  @Test
  public void checkVolumeUrlType() throws OzoneClientException {
    address = new OzoneAddress(prefix + "vol1");
    address.ensureVolumeAddress();
    Assert.assertEquals("vol1", address.getVolumeName());
  }

  @Test
  public void checkBucketUrlType() throws OzoneClientException {
    address = new OzoneAddress(prefix + "vol1/bucket");
    address.ensureBucketAddress();
    Assert.assertEquals("vol1", address.getVolumeName());
    Assert.assertEquals("bucket", address.getBucketName());

    address = new OzoneAddress(prefix + "vol1/bucket/");
    address.ensureBucketAddress();
    Assert.assertEquals("vol1", address.getVolumeName());
    Assert.assertEquals("bucket", address.getBucketName());
  }

  @Test
  public void checkKeyUrlType() throws OzoneClientException {
    address = new OzoneAddress(prefix + "vol1/bucket/key");
    address.ensureKeyAddress();
    Assert.assertEquals("vol1", address.getVolumeName());
    Assert.assertEquals("bucket", address.getBucketName());
    Assert.assertEquals("key", address.getKeyName());

    address = new OzoneAddress(prefix + "vol1/bucket/key/");
    address.ensureKeyAddress();
    Assert.assertEquals("vol1", address.getVolumeName());
    Assert.assertEquals("bucket", address.getBucketName());
    Assert.assertEquals("key/", address.getKeyName());

    address = new OzoneAddress(prefix + "vol1/bucket/key1/key3/key");
    address.ensureKeyAddress();
    Assert.assertEquals("vol1", address.getVolumeName());
    Assert.assertEquals("bucket", address.getBucketName());
    Assert.assertEquals("key1/key3/key", address.getKeyName());
    Assert.assertFalse("this should not be a prefix",
        address.isPrefix());
  }

  @Test
  public void checkPrefixUrlType() throws OzoneClientException {
    address = new OzoneAddress(prefix + "vol1/bucket/prefix");
    address.ensurePrefixAddress();
    Assert.assertEquals("vol1", address.getVolumeName());
    Assert.assertEquals("bucket", address.getBucketName());
    Assert.assertEquals("prefix", address.getKeyName());
    Assert.assertTrue("this should be a prefix",
        address.isPrefix());
  }

  @Test
  public void checkSnapshotUrlType() throws OzoneClientException {
    address = new OzoneAddress(prefix + "vol1/bucket/.snapshot/snap1");
    address.ensureSnapshotAddress();
    Assert.assertEquals("vol1", address.getVolumeName());
    Assert.assertEquals("bucket", address.getBucketName());
    Assert.assertEquals(".snapshot/snap1", address.getSnapshotName());
    Assert.assertEquals(".snapshot/snap1", address.getKeyName());


    String message = "Delimiters (/) not allowed following " +
        "a bucket name. Only a snapshot name with " +
        "a snapshot indicator are accepted";

    address = new OzoneAddress(prefix + "vol1/bucket/.snapshot");

    exception.expect(OzoneClientException.class);
    exception.expectMessage(message);

    address.ensureSnapshotAddress();
  }
}