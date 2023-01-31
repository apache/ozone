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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;


/**
 * Test ozone URL parsing.
 */
public class TestOzoneAddress {

  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {"o3://localhost:9878/"},
        {"o3://localhost/"},
        {"o3:///"},
        {"/"},
        {""}
    });
  }

  private OzoneAddress address;

  @ParameterizedTest
  @MethodSource("data")
  public void checkRootUrlType(String prefix) throws OzoneClientException {
    address = new OzoneAddress("");
    address.ensureRootAddress();

    address = new OzoneAddress(prefix + "");
    address.ensureRootAddress();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void checkVolumeUrlType(String prefix) throws OzoneClientException {
    address = new OzoneAddress(prefix + "vol1");
    address.ensureVolumeAddress();
    Assertions.assertEquals("vol1", address.getVolumeName());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void checkBucketUrlType(String prefix) throws OzoneClientException {
    address = new OzoneAddress(prefix + "vol1/bucket");
    address.ensureBucketAddress();
    Assertions.assertEquals("vol1", address.getVolumeName());
    Assertions.assertEquals("bucket", address.getBucketName());

    address = new OzoneAddress(prefix + "vol1/bucket/");
    address.ensureBucketAddress();
    Assertions.assertEquals("vol1", address.getVolumeName());
    Assertions.assertEquals("bucket", address.getBucketName());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void checkKeyUrlType(String prefix) throws OzoneClientException {
    address = new OzoneAddress(prefix + "vol1/bucket/key");
    address.ensureKeyAddress();
    Assertions.assertEquals("vol1", address.getVolumeName());
    Assertions.assertEquals("bucket", address.getBucketName());
    Assertions.assertEquals("key", address.getKeyName());

    address = new OzoneAddress(prefix + "vol1/bucket/key/");
    address.ensureKeyAddress();
    Assertions.assertEquals("vol1", address.getVolumeName());
    Assertions.assertEquals("bucket", address.getBucketName());
    Assertions.assertEquals("key/", address.getKeyName());

    address = new OzoneAddress(prefix + "vol1/bucket/key1/key3/key");
    address.ensureKeyAddress();
    Assertions.assertEquals("vol1", address.getVolumeName());
    Assertions.assertEquals("bucket", address.getBucketName());
    Assertions.assertEquals("key1/key3/key", address.getKeyName());
    Assertions.assertFalse(address.isPrefix(), "this should not be a prefix");
  }

  @ParameterizedTest
  @MethodSource("data")
  public void checkPrefixUrlType(String prefix) throws OzoneClientException {
    address = new OzoneAddress(prefix + "vol1/bucket/prefix");
    address.ensurePrefixAddress();
    Assertions.assertEquals("vol1", address.getVolumeName());
    Assertions.assertEquals("bucket", address.getBucketName());
    Assertions.assertEquals("prefix", address.getKeyName());
    Assertions.assertTrue(address.isPrefix(), "this should be a prefix");
  }

  @ParameterizedTest
  @MethodSource("data")
  public void checkSnapshotUrlType(String prefix) throws OzoneClientException {
    address = new OzoneAddress(prefix + "vol1/bucket/.snapshot/snap1");
    address.ensureSnapshotAddress();
    Assertions.assertEquals("vol1", address.getVolumeName());
    Assertions.assertEquals("bucket", address.getBucketName());
    Assertions.assertEquals(".snapshot/snap1",
        address.getSnapshotNameWithIndicator());
    Assertions.assertEquals(".snapshot/snap1", address.getKeyName());


    String message = "Only a snapshot name with " +
        "a snapshot indicator is accepted";

    address = new OzoneAddress(prefix + "vol1/bucket/.snapshot");

    OzoneClientException exception = Assertions
        .assertThrows(OzoneClientException.class,
            () -> address.ensureSnapshotAddress());
    Assertions.assertTrue(exception.getMessage().contains(message));
  }
}
