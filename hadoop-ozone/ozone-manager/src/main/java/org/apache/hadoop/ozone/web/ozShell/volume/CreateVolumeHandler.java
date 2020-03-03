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

import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.web.ozShell.OzoneAddress;
import org.apache.hadoop.security.UserGroupInformation;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;

/**
 * Executes the create volume call for the shell.
 */
@Command(name = "create",
    description = "Creates a volume for the specified user")
public class CreateVolumeHandler extends VolumeHandler {

  @Option(names = {"--user", "-u"},
      description = "Owner of of the volume")
  private String userName;

  @Option(names = {"--quota", "-q"},
      description =
          "Quota of the newly created volume (eg. 1G)")
  private String quota;

  @Option(names = {"--root"},
      description = "Development flag to execute the "
          + "command as the admin (hdfs) user.")
  private boolean root;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    if (userName == null) {
      userName = UserGroupInformation.getCurrentUser().getUserName();
    }

    String volumeName = address.getVolumeName();

    String rootName;
    if (root) {
      rootName = "hdfs";
    } else {
      rootName = UserGroupInformation.getCurrentUser().getShortUserName();
    }

    VolumeArgs.Builder volumeArgsBuilder = VolumeArgs.newBuilder()
        .setAdmin(rootName)
        .setOwner(userName);
    if (quota != null) {
      volumeArgsBuilder.setQuota(quota);
    }
    client.getObjectStore().createVolume(volumeName,
        volumeArgsBuilder.build());

    if (isVerbose()) {
      OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
      printObjectAsJson(vol);
    }
  }

}

