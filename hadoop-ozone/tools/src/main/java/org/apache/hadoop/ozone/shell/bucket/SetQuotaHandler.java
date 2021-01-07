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

import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.SetSpaceQuotaOptions;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.IOException;

/**
 * set quota of the bucket.
 */
@Command(name = "setquota",
    description = "Set quota of the buckets")
public class SetQuotaHandler extends BucketHandler {

  @CommandLine.Mixin
  private SetSpaceQuotaOptions quotaOptions;

  @Override
  public void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    OzoneBucket bucket = client.getObjectStore().getVolume(volumeName)
        .getBucket(bucketName);
    long spaceQuota = bucket.getQuotaInBytes();
    long namespaceQuota = bucket.getQuotaInNamespace();

    if (quotaOptions.getQuotaInBytes() != null
        && !quotaOptions.getQuotaInBytes().isEmpty()) {
      spaceQuota = OzoneQuota.parseQuota(quotaOptions.getQuotaInBytes(),
          quotaOptions.getQuotaInNamespace()).getQuotaInBytes();
    }
    if (quotaOptions.getQuotaInNamespace() >= 0) {
      namespaceQuota = quotaOptions.getQuotaInNamespace();
    }

    bucket.setQuota(OzoneQuota.getOzoneQuota(spaceQuota, namespaceQuota));
  }
}
