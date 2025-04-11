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

package org.apache.hadoop.ozone.shell.volume;

import com.google.common.base.Strings;
import java.io.IOException;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.SetSpaceQuotaOptions;
import org.apache.hadoop.security.UserGroupInformation;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Executes the create volume call for the shell.
 */
@Command(name = "create",
    description = "Creates a volume for the specified user")
public class CreateVolumeHandler extends VolumeHandler {

  @Option(names = {"--user", "-u"},
      description = "Owner of the volume")
  private String ownerName;

  @CommandLine.Mixin
  private SetSpaceQuotaOptions quotaOptions;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {
    if (ownerName == null) {
      ownerName = UserGroupInformation.getCurrentUser().getShortUserName();
    }

    String volumeName = address.getVolumeName();

    String adminName = UserGroupInformation.getCurrentUser().getShortUserName();
    VolumeArgs.Builder volumeArgsBuilder = VolumeArgs.newBuilder()
        .setAdmin(adminName)
        .setOwner(ownerName);
    if (!Strings.isNullOrEmpty(quotaOptions.getQuotaInBytes())) {
      volumeArgsBuilder.setQuotaInBytes(OzoneQuota.parseSpaceQuota(
          quotaOptions.getQuotaInBytes()).getQuotaInBytes());
    }

    if (!Strings.isNullOrEmpty(quotaOptions.getQuotaInNamespace())) {
      volumeArgsBuilder.setQuotaInNamespace(OzoneQuota.parseNameSpaceQuota(
          quotaOptions.getQuotaInNamespace()).getQuotaInNamespace());
    }

    client.getObjectStore().createVolume(volumeName,
        volumeArgsBuilder.build());

    if (isVerbose()) {
      OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
      printObjectAsJson(vol);
    }
  }

}

