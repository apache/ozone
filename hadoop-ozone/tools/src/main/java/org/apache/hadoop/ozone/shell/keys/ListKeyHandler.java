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

package org.apache.hadoop.ozone.shell.keys;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.ListOptions;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.bucket.BucketHandler;

import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Executes List Keys.
 */
@Command(name = "list",
    aliases = "ls",
    description = "list all keys in a given bucket")
public class ListKeyHandler extends BucketHandler {

  @CommandLine.Mixin
  private ListOptions listOptions;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException, OzoneClientException {

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    Iterator<? extends OzoneKey> keyIterator = bucket.listKeys(
        listOptions.getPrefix(), listOptions.getStartItem());

    int maxKeyLimit = listOptions.getLimit();

    int counter = 0;
    while (maxKeyLimit > counter && keyIterator.hasNext()) {
      OzoneKey ozoneKey = keyIterator.next();
      printObjectAsJson(ozoneKey);
      counter++;
    }

    // More keys were returned notify about max length
    if (keyIterator.hasNext()) {
      out().println("Listing first " + maxKeyLimit + " entries of the " +
          "result. Use --length (-l) to override max returned keys.");
    } else if (isVerbose()) {
      out().printf("Found : %d keys for bucket %s in volume : %s ",
          counter, bucketName, volumeName);
    }
  }

}
