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

package org.apache.hadoop.ozone.shell.keys;

import static org.apache.hadoop.ozone.OzoneConsts.MB;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.MandatoryReplicationOptions;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

/**
 * Rewrite a key with different replication.
 */
@CommandLine.Command(name = "rewrite",
    description = "Rewrites the key with different replication")
public class RewriteKeyHandler extends KeyHandler {

  @CommandLine.Mixin
  private MandatoryReplicationOptions replication;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) throws IOException {
    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String keyName = address.getKeyName();

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    OzoneKeyDetails key = bucket.getKey(keyName);

    ReplicationConfig newReplication = replication.fromParamsOrConfig(getConf());
    if (newReplication == null ||
        newReplication.equals(key.getReplicationConfig())) {
      System.err.println("Replication unchanged: " + key.getReplicationConfig());
      return;
    }

    try (
        InputStream input = bucket.readKey(keyName);
        OutputStream output = bucket.rewriteKey(keyName, key.getDataSize(), key.getGeneration(),
            newReplication, key.getMetadata())) {
      IOUtils.copyBytes(input, output, (int) Math.min(MB, key.getDataSize()));
    }
  }
}
