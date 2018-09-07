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

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.apache.hadoop.ozone.web.utils.JsonUtils;

import java.net.URI;

/**
 * Executes the create volume call for the shell.
 */
@Command(name = "-createVolume",
    description = "Creates a volume for the specified user")
public class CreateVolumeHandler extends Handler {

  @Parameters(arity = "1..1", description = Shell.OZONE_VOLUME_URI_DESCRIPTION)
  private String uri;

  @Option(names = {"--user", "-user"},
      description = "Owner of of the volume", required =
      true)
  private String userName;

  @Option(names = {"--quota", "-quota"},
      description =
          "Quota of the newly created volume (eg. 1G)")
  private String quota;

  @Option(names = {"--root", "-root"},
      description = "Development flag to execute the "
          + "command as the admin (hdfs) user.")
  private boolean root;

  /**
   * Executes the Create Volume.
   */
  @Override
  public Void call() throws Exception {

    URI ozoneURI = verifyURI(uri);

    // we need to skip the slash in the URI path
    // getPath returns /volumeName needs to remove the initial slash.
    String volumeName = ozoneURI.getPath().replaceAll("^/+", "");
    if (volumeName.isEmpty()) {
      throw new OzoneClientException(
          "Volume name is required to create a volume");
    }

    if (isVerbose()) {
      System.out.printf("Volume name : %s%n", volumeName);
    }

    String rootName;
    if (root) {
      rootName = "hdfs";
    } else {
      rootName = System.getProperty("user.name");
    }

    VolumeArgs.Builder volumeArgsBuilder = VolumeArgs.newBuilder()
        .setAdmin(rootName)
        .setOwner(userName);
    if (quota != null) {
      volumeArgsBuilder.setQuota(quota);
    }
    client.getObjectStore().createVolume(volumeName, volumeArgsBuilder.build());

    if (isVerbose()) {
      OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
      System.out.printf("%s%n", JsonUtils.toJsonStringWithDefaultPrettyPrinter(
          JsonUtils.toJsonString(OzoneClientUtils.asVolumeInfo(vol))));
    }
    return null;
  }

}

