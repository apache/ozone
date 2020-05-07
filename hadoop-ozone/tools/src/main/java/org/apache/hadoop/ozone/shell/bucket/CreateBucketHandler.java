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
package org.apache.hadoop.ozone.shell.bucket;

import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.OzoneAddress;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;

/**
 * create bucket handler.
 */
@Command(name = "create",
    description = "creates a bucket in a given volume")
public class CreateBucketHandler extends BucketHandler {

  @Option(names = {"--bucketkey", "-k"},
      description = "bucket encryption key name")
  private String bekName;

  @Option(names = {"--enforcegdpr", "-g"},
      description = "if true, indicates GDPR enforced bucket, " +
          "false/unspecified indicates otherwise")
  private Boolean isGdprEnforced;

  @Option(names = {"--enableTrash", "-t"},
      description = "if true, indicates bucket with trash-enabled, " +
          "false/unspecified indicates otherwise",
      defaultValue = "false")
  private boolean isTrashEnabled;

  /**
   * Executes create bucket.
   */
  @Override
  public void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    BucketArgs.Builder bb = new BucketArgs.Builder()
        .setStorageType(StorageType.DEFAULT)
        .setVersioning(false);

    if (isGdprEnforced != null) {
      bb.addMetadata(OzoneConsts.GDPR_FLAG, String.valueOf(isGdprEnforced));
    }

    if ((isGdprEnforced != null) &&
        (isGdprEnforced) &&
        (isTrashEnabled)) {
      isTrashEnabled = false;
      System.out.println("GDPR enabled buckets cannot have trash-enabled. " +
          "Set trash-disabled.");
    }
    bb.setTrashEnabled(isTrashEnabled);

    if (bekName != null) {
      if (!bekName.isEmpty()) {
        bb.setBucketEncryptionKey(bekName);
      } else {
        throw new IllegalArgumentException("Bucket encryption key name must" +
            " " + "be specified to enable bucket encryption!");
      }
      if (isVerbose()) {
        out().printf("Bucket Encryption enabled with Key Name: %s%n",
            bekName);
      }
    }

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    vol.createBucket(bucketName, bb.build());

    if (isVerbose()) {
      OzoneBucket bucket = vol.getBucket(bucketName);
      printObjectAsJson(bucket);
    }
  }
}
