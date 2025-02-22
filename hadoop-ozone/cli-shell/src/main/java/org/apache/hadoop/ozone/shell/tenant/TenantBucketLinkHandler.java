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

package org.apache.hadoop.ozone.shell.tenant;

import java.io.IOException;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.bucket.BucketUri;
import picocli.CommandLine;
import picocli.CommandLine.Parameters;

/**
 * ozone tenant linkbucket.
 *
 * Note: Currently this command is exactly the same as `ozone sh bucket link`.
 * We might expand this to add more functionality in the future, and different
 * ObjectStore API(s) would be used by then.
 */
@CommandLine.Command(name = "linkbucket",
    description = "Create a symlink to another bucket")
public class TenantBucketLinkHandler extends TenantHandler {

  @Parameters(index = "0", arity = "1..1",
      description = "The bucket which the link should point to.",
      converter = BucketUri.class)
  private OzoneAddress source;

  @Parameters(index = "1", arity = "1..1",
      description = "Address of the link bucket",
      converter = BucketUri.class)
  private OzoneAddress target;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    BucketArgs.Builder bb = new BucketArgs.Builder()
        .setStorageType(StorageType.DEFAULT)
        .setVersioning(false)
        .setSourceVolume(source.getVolumeName())
        .setSourceBucket(source.getBucketName());

    String volumeName = target.getVolumeName();
    String bucketName = target.getBucketName();

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    vol.createBucket(bucketName, bb.build());

    if (isVerbose()) {
      OzoneBucket bucket = vol.getBucket(bucketName);
      printObjectAsJson(bucket);
    }
  }
}
