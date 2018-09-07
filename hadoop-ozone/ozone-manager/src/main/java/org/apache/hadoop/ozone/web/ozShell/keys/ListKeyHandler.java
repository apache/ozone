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

package org.apache.hadoop.ozone.web.ozShell.keys;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.rest.response.KeyInfo;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.apache.hadoop.ozone.web.utils.JsonUtils;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Executes List Keys.
 */
@Command(name = "-listKey",
    description = "list all keys in a given bucket")
public class ListKeyHandler extends Handler {

  @Parameters(arity = "1..1", description = Shell.OZONE_BUCKET_URI_DESCRIPTION)
  private String uri;

  @Option(names = {"--length", "-length", "-l"},
      description = "Limit of the max results",
      defaultValue = "100")
  private int maxKeys;

  @Option(names = {"--start", "-start", "-s"},
      description = "The first key to start the listing")
  private String startKey;

  @Option(names = {"--prefix", "-prefix", "-p"},
      description = "Prefix to filter the key")
  private String prefix;

  /**
   * Executes the Client Calls.
   */
  @Override
  public Void call() throws Exception {

    URI ozoneURI = verifyURI(uri);
    Path path = Paths.get(ozoneURI.getPath());
    if (path.getNameCount() < 2) {
      throw new OzoneClientException(
          "volume/bucket is required in listKey");
    }

    if (maxKeys < 1) {
      throw new IllegalArgumentException(
          "the length should be a positive number");
    }

    String volumeName = path.getName(0).toString();
    String bucketName = path.getName(1).toString();

    if (isVerbose()) {
      System.out.printf("Volume Name : %s%n", volumeName);
      System.out.printf("bucket Name : %s%n", bucketName);
    }

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    Iterator<OzoneKey> keyIterator = bucket.listKeys(prefix, startKey);
    List<KeyInfo> keyInfos = new ArrayList<>();

    while (maxKeys > 0 && keyIterator.hasNext()) {
      KeyInfo key = OzoneClientUtils.asKeyInfo(keyIterator.next());
      keyInfos.add(key);
      maxKeys -= 1;
    }

    if (isVerbose()) {
      System.out.printf("Found : %d keys for bucket %s in volume : %s ",
          keyInfos.size(), bucketName, volumeName);
    }
    System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(
        JsonUtils.toJsonString(keyInfos)));
    return null;
  }

}
