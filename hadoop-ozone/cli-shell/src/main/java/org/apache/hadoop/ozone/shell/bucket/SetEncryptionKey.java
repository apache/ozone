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

package org.apache.hadoop.ozone.shell.bucket;

import java.io.IOException;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

/**
 * Command-line tool to set the encryption key of a bucket.
 *
 * There are known bugs, HDDS-7449 and HDDS-7526, which could potentially result
 * in the loss of bucket encryption properties when either quota or bucket
 * replication properties are (re)set on an existing bucket, posing a critical
 * issue. This may affect consumers using previous versions of Ozone.
 *
 * To address this bug, this CLI tool provides the ability to (re)set the
 * Bucket Encryption Key (BEK) for HDDS-7449/HDDS-7526 affected buckets using
 * the Ozone shell.
 *
 * Users can execute the following command for setting BEK for a given bucket:
 * "ozone sh bucket set-encryption-key -k <enckey> <vol>/<buck>"
 *
 * Please note that this operation only resets the BEK and does not modify any
 * other properties of the bucket or the existing keys within it.
 *
 * Existing keys in the bucket will retain their current properties, and any
 * keys added before the BEK reset will remain unencrypted. Keys added after the
 * BEK reset will be encrypted using the new BEK details provided.
 *
 * @deprecated This functionality is deprecated as it is not intended for users
 * to reset bucket encryption post-bucket creation under normal circumstances
 * and may be removed in the future. Users are advised to exercise caution and
 * consider alternative approaches for managing bucket encryption unless
 * HDDS-7449 or HDDS-7526 is encountered. As a result, the setter methods and
 * this CLI functionality have been marked as deprecated, and the command has
 * been hidden.
 */
@Deprecated
@CommandLine.Command(name = "set-encryption-key",
    description = "Set Bucket Encryption Key (BEK) for a given bucket. Users " +
        "are advised to exercise caution and consider alternative approaches " +
        "for managing bucket encryption unless HDDS-7449 or HDDS-7526 is " +
        "encountered.",
    hidden = true)
public class SetEncryptionKey extends BucketHandler {

  @CommandLine.Option(names = {"--key", "-k"},
      description = "bucket encryption key name")
  private String bekName;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    OzoneBucket bucket =
        client.getObjectStore().getVolume(volumeName).getBucket(bucketName);
    bucket.setEncryptionKey(bekName);
  }
}
