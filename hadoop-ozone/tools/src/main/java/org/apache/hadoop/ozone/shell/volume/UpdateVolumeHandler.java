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

package org.apache.hadoop.ozone.shell.volume;

import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.OzoneAddress;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;

/**
 * Executes update volume calls.
 */
@Command(name = "update",
    description = "Updates parameter of the volumes")
public class UpdateVolumeHandler extends VolumeHandler {

  @Option(names = {"--user"},
      description = "Owner of the volume to set")
  private String ownerName;

  @Option(names = {"--spaceQuota", "-s"},
      description = "Quota in bytes of the volume to set (eg. 1GB)")
  private String storagespaceQuota;

  @Option(names = {"--quota", "-q"},
      description = "Bucket counts of the volume to set (eg. 5)")
  private long namespaceQuota = OzoneConsts.QUOTA_COUNT_RESET;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {
    String volumeName = address.getVolumeName();
    OzoneVolume volume = client.getObjectStore().getVolume(volumeName);

    long spaceQuota = volume.getStoragespaceQuota();
    long countQuota = volume.getNamespaceQuota();

    if (storagespaceQuota != null && !storagespaceQuota.isEmpty()) {
      spaceQuota = OzoneQuota.parseQuota(storagespaceQuota,
          namespaceQuota).getStoragespaceQuota();
    }
    if (namespaceQuota >= 0) {
      countQuota = namespaceQuota;
    }

    volume.setQuota(
        OzoneQuota.getOzoneQuota(spaceQuota, countQuota));

    if (ownerName != null && !ownerName.isEmpty()) {
      boolean result = volume.setOwner(ownerName);
      if (LOG.isDebugEnabled() && !result) {
        out().format("Volume '%s' owner is already '%s'. Unchanged.%n",
            volumeName, ownerName);
      }
    }

    // For printing newer modificationTime.
    OzoneVolume updatedVolume = client.getObjectStore().getVolume(volumeName);
    printObjectAsJson(updatedVolume);
  }
}
