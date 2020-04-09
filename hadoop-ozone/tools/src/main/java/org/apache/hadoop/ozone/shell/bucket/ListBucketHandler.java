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

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.ListOptions;
import org.apache.hadoop.ozone.shell.OzoneAddress;
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
  private ListOptions listOptions;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    String volumeName = address.getVolumeName();
    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    Iterator<? extends OzoneBucket> bucketIterator =
        vol.listBuckets(listOptions.getPrefix(), listOptions.getStartItem());

    int counter = 0;
    while (listOptions.getLimit() > counter && bucketIterator.hasNext()) {
      printObjectAsJson(bucketIterator.next());

      counter++;
    }

    if (isVerbose()) {
      out().printf("Found : %d buckets for volume : %s ",
          counter, volumeName);
    }
  }

}

