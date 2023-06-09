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

package org.apache.hadoop.ozone.shell.volume;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.ListOptions;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.IOException;
import java.util.Iterator;

/**
 * Executes Volume listKeys call.
 */
@Command(name = "listkeys",
    description = "List keys inside volume")
public class VolumeListKeysHandler extends VolumeHandler {

  @CommandLine.Mixin
  private ListOptions listOptions;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {
    String volumeName = address.getVolumeName();
    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);

    Iterator<? extends OzoneBucket> bucketIterator =
        vol.listBuckets(null);
    int maxKeyLimit = listOptions.getLimit();
    int totalKeys = 0;
    while (bucketIterator.hasNext()) {
      OzoneBucket bucket = bucketIterator.next();
      Iterator<? extends OzoneKey> keyIterator = bucket.listKeys(null);

      int counter = printAsJsonArray(keyIterator, maxKeyLimit);
      totalKeys += counter;
      maxKeyLimit -= counter;

      // More keys were returned notify about max length
      if (keyIterator.hasNext() || (bucketIterator.hasNext()
          && maxKeyLimit <= 0)) {
        out().println("Listing first " + totalKeys + " entries of the " +
            "result. Use --length (-l) to override max returned keys.");
        return;
      }
    }
    if (isVerbose()) {
      out().printf("Found : %d keys in volume : %s %n",
          totalKeys, volumeName);
    }
  }
}
