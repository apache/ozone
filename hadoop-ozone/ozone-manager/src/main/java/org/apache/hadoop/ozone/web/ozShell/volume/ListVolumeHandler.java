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

package org.apache.hadoop.ozone.web.ozShell.volume;

import com.google.common.base.Strings;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.rest.response.VolumeInfo;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.apache.hadoop.ozone.web.utils.JsonUtils;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Executes List Volume call.
 */
@Command(name = "-listVolume",
    description = "List the volumes of a given user")
public class ListVolumeHandler extends Handler {

  @Parameters(arity = "1..1",
      description = Shell.OZONE_VOLUME_URI_DESCRIPTION,
      defaultValue = "/")
  private String uri;

  @Option(names = {"--length", "-length", "-l"},
      description = "Limit of the max results",
      defaultValue = "100")
  private int maxVolumes;

  @Option(names = {"--start", "-start", "-s"},
      description = "The first volume to start the listing")
  private String startVolume;

  @Option(names = {"--prefix", "-prefix", "-p"},
      description = "Prefix to filter the volumes")
  private String prefix;

  @Option(names = {"--user", "-user", "-u"},
      description = "Owner of the volumes to list.")
  private String userName;

  /**
   * Executes the Client Calls.
   */
  @Override
  public Void call() throws Exception {

    URI ozoneURI = verifyURI(uri);
    if (!Strings.isNullOrEmpty(ozoneURI.getPath()) && !ozoneURI.getPath()
        .equals("/")) {
      throw new OzoneClientException(
          "Invalid URI: " + ozoneURI + " . Specified path not used." + ozoneURI
              .getPath());
    }

    if (userName == null) {
      userName = System.getProperty("user.name");
    }

    if (maxVolumes < 1) {
      throw new IllegalArgumentException(
          "the length should be a positive number");
    }

    Iterator<OzoneVolume> volumeIterator;
    if(userName != null) {
      volumeIterator = client.getObjectStore()
          .listVolumesByUser(userName, prefix, startVolume);
    } else {
      volumeIterator = client.getObjectStore().listVolumes(prefix);
    }

    List<VolumeInfo> volumeInfos = new ArrayList<>();

    while (maxVolumes > 0 && volumeIterator.hasNext()) {
      VolumeInfo volume = OzoneClientUtils.asVolumeInfo(volumeIterator.next());
      volumeInfos.add(volume);
      maxVolumes -= 1;
    }

    if (isVerbose()) {
      System.out.printf("Found : %d volumes for user : %s ", volumeInfos.size(),
          userName);
    }
    System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(
        JsonUtils.toJsonString(volumeInfos)));
    return null;
  }
}

