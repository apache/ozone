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

import java.io.IOException;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.bucket.BucketHandler;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * Renames an existing key.
 */
@Command(name = "rename",
    description = "renames an existing key")
public class RenameKeyHandler extends BucketHandler {

  @Parameters(index = "1", arity = "1..1",
      description = "The existing key to be renamed")
  private String fromKey;

  @Parameters(index = "2", arity = "1..1",
      description = "The new desired name of the key")
  private String toKey;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    bucket.renameKey(fromKey, toKey);

    if (isVerbose()) {
      out().printf("Renamed Key : %s to %s%n", fromKey, toKey);
    }
  }
}
