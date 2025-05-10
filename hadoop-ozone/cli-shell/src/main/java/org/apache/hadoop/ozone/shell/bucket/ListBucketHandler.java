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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.ListPaginationOptions;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.PrefixFilterOption;
import org.apache.hadoop.ozone.shell.volume.VolumeHandler;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Executes List Bucket.
 */
@Command(name = "list",
    aliases = "ls",
    description = "lists the buckets in a volume.")
public class ListBucketHandler extends VolumeHandler {

  @CommandLine.Mixin
  private ListPaginationOptions listOptions;

  @CommandLine.Mixin
  private PrefixFilterOption prefixFilter;

  @CommandLine.Option(names = {"--has-snapshot"},
      description = "Only show buckets that have at least one active snapshot.")
  private boolean filterByHasSnapshot;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    String volumeName = address.getVolumeName();
    ObjectStore objectStore = client.getObjectStore();
    OzoneVolume vol = objectStore.getVolume(volumeName);
    Iterator<? extends OzoneBucket> bucketIterator =
        vol.listBuckets(prefixFilter.getPrefix(),
            listOptions.getStartItem(), filterByHasSnapshot);
    List<Object> bucketList = new ArrayList<>();
    int counter = 0;
    while (bucketIterator.hasNext() && counter < listOptions.getLimit()) {
      OzoneBucket bucket = bucketIterator.next();
      if (bucket.isLink()) {
        bucketList.add(new InfoBucketHandler.LinkBucket(bucket));
      } else {
        bucketList.add(bucket);
      }
      counter++;
    }
    printAsJsonArray(bucketList.iterator(), listOptions.getLimit());
    if (isVerbose()) {
      err().printf("Found : %d buckets for volume : %s ", counter, volumeName);
    }
  }

}

