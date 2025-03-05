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
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.OzoneIllegalArgumentException;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.ShellReplicationOptions;
import picocli.CommandLine;

/**
 * set replication configuration of the bucket.
 */
@CommandLine.Command(name = "set-replication-config",
    description = "Set replication config on bucket")
public class SetReplicationConfigHandler extends BucketHandler {

  @CommandLine.Mixin
  private ShellReplicationOptions replication;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {
    ReplicationConfig replicationConfig = replication.fromParams(getConf())
        .orElseThrow(() -> new OzoneIllegalArgumentException(
            "Replication type and config must be specified."));

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    OzoneBucket bucket =
        client.getObjectStore().getVolume(volumeName).getBucket(bucketName);
    bucket.setReplicationConfig(replicationConfig);
  }
}
