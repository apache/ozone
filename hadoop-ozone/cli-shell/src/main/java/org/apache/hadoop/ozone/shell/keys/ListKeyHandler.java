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

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.ListPaginationOptions;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.PrefixFilterOption;
import org.apache.hadoop.ozone.shell.common.VolumeBucketHandler;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Executes List Keys for a bucket or snapshot.
 */
@Command(name = "list",
    aliases = "ls",
    description = "list all keys in a given volume or bucket or snapshot")
public class ListKeyHandler extends VolumeBucketHandler {

  @CommandLine.Mixin
  private ListPaginationOptions listOptions;

  @CommandLine.Mixin
  private PrefixFilterOption prefixFilter;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    if (!Strings.isNullOrEmpty(address.getBucketName())) {
      listKeysInsideBucket(client, address);
      return;
    }
    listKeysInsideVolume(client, address);
  }

  private void listKeysInsideBucket(OzoneClient client, OzoneAddress address)
      throws IOException {
    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String snapshotNameWithIndicator = address.getSnapshotNameWithIndicator();
    String keyPrefix = "";

    if (!Strings.isNullOrEmpty(snapshotNameWithIndicator)) {
      keyPrefix += snapshotNameWithIndicator;

      if (!Strings.isNullOrEmpty(prefixFilter.getPrefix())) {
        keyPrefix += "/";
      }
    }

    if (!Strings.isNullOrEmpty(prefixFilter.getPrefix())) {
      keyPrefix += prefixFilter.getPrefix();
    }

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    int maxKeyLimit = listOptions.getLimit();
    if (maxKeyLimit < bucket.getListCacheSize()) {
      bucket.setListCacheSize(maxKeyLimit);
    }
    Iterator<? extends OzoneKey> keyIterator = bucket.listKeys(
        keyPrefix, listOptions.getStartItem());

    int counter = printAsJsonArray(keyIterator, maxKeyLimit);

    // More keys were returned notify about max length
    if (keyIterator.hasNext()) {
      err().println("Listing first " + maxKeyLimit + " entries of the " +
          "result. Use --length (-l) to override max returned keys.");
    } else if (isVerbose()) {
      if (!Strings.isNullOrEmpty(snapshotNameWithIndicator)) {
        // snapshotValues[0] = ".snapshot"
        // snapshotValues[1] = snapshot name
        String[] snapshotValues = snapshotNameWithIndicator.split("/");

        err().printf("Found : %d keys for snapshot %s " +
                "under bucket %s in volume : %s ",
            counter, snapshotValues[1], bucketName, volumeName);
      } else {
        err().printf("Found : %d keys for bucket %s in volume : %s ",
            counter, bucketName, volumeName);
      }
    }
  }

  private void listKeysInsideVolume(OzoneClient client, OzoneAddress address)
      throws IOException {
    String volumeName = address.getVolumeName();
    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);

    Iterator<? extends OzoneBucket> bucketIterator =
        vol.listBuckets(null);
    int maxKeyLimit = listOptions.getLimit();
    String keyPrefix = "";
    if (!Strings.isNullOrEmpty(prefixFilter.getPrefix())) {
      keyPrefix += prefixFilter.getPrefix();
    }

    int totalKeys = 0;
    while (bucketIterator.hasNext()) {
      OzoneBucket bucket = bucketIterator.next();
      Iterator<? extends OzoneKey> keyIterator = bucket.listKeys(keyPrefix,
          listOptions.getStartItem());

      int counter = printAsJsonArray(keyIterator, maxKeyLimit);
      totalKeys += counter;
      maxKeyLimit -= counter;

      // More keys were returned notify about max length
      if (keyIterator.hasNext() || (bucketIterator.hasNext()
          && maxKeyLimit <= 0)) {
        err().println("Listing first " + totalKeys + " entries of the " +
            "result. Use --length (-l) to override max returned keys.");
        return;
      }
    }
    if (isVerbose()) {
      err().printf("Found : %d keys in volume : %s %n",
          totalKeys, volumeName);
    }
  }
}
