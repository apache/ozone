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
package org.apache.hadoop.ozone.shell.snapshot;

import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneSnapshot;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.ListOptions;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.volume.VolumeHandler;
import picocli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE;

/**
 * ozone sh snapshot list-bucket.
 * a handler for Ozone shell CLI command 'list buckets which have snapshots'.
 */
@CommandLine.Command(name = "list-bucket",
    description = "List bucket which have snapshots.")
public class ListBucketWithSnapshotHandler extends VolumeHandler {

  @CommandLine.Mixin
  private ListOptions listOptions;
  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException, OzoneClientException {

    String volumeName = address.getVolumeName();
    ObjectStore objectStore = client.getObjectStore();
    OzoneVolume vol = objectStore.getVolume(volumeName);

    List<OzoneBucket> bucketList = new ArrayList<>();
    int counter = 0;
    String startItem = listOptions.getStartItem();
    while (counter < listOptions.getLimit()) {
      Iterator<? extends OzoneBucket> bucketIterator =
          vol.listBuckets(listOptions.getPrefix(), startItem);
      if (!bucketIterator.hasNext()) {
        break;
      }
      OzoneBucket bucket = null;
      while (counter < listOptions.getLimit() && bucketIterator.hasNext()) {
        bucket = bucketIterator.next();
        if (!bucket.isLink()) {
          List<OzoneSnapshot> snapshotList =
              objectStore.listSnapshot(volumeName, bucket.getName());
          for (OzoneSnapshot snapshot : snapshotList) {
            if (Objects.equals(snapshot.getSnapshotStatus(),
                SNAPSHOT_ACTIVE.name())) {
              bucketList.add(bucket);
              counter++;
            }
          }
        }
      }
      if (bucket != null) {
        startItem = bucket.getName();
      }
    }
    printAsJsonArray(bucketList.iterator(), listOptions.getLimit());
    if (isVerbose()) {
      out().printf("Found : %d buckets which have snapshots for volume : %s ",
          counter, volumeName);
    }
  }
}
