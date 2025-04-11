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

import static org.apache.hadoop.ozone.OzoneConsts.OLD_QUOTA_DEFAULT;

import com.google.common.base.Strings;
import java.io.IOException;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.SetSpaceQuotaOptions;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Executes set volume quota calls.
 */
@Command(name = "setquota",
    description = "Set quota of the volumes. At least one of the " +
        "quota set flag is mandatory.")
public class SetQuotaHandler extends VolumeHandler {

  @CommandLine.Mixin
  private SetSpaceQuotaOptions quotaOptions;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {
    String volumeName = address.getVolumeName();
    OzoneVolume volume = client.getObjectStore().getVolume(volumeName);

    long spaceQuota = volume.getQuotaInBytes();
    long namespaceQuota = volume.getQuotaInNamespace();
    boolean isOptionPresent = false;
    if (!Strings.isNullOrEmpty(quotaOptions.getQuotaInBytes())) {
      spaceQuota = OzoneQuota.parseSpaceQuota(
          quotaOptions.getQuotaInBytes()).getQuotaInBytes();
      isOptionPresent = true;
    }

    if (!Strings.isNullOrEmpty(quotaOptions.getQuotaInNamespace())) {
      namespaceQuota = OzoneQuota.parseNameSpaceQuota(
          quotaOptions.getQuotaInNamespace()).getQuotaInNamespace();
      isOptionPresent = true;
    }
    
    if (!isOptionPresent) {
      throw new IOException(
          "At least one of the quota set flag is required.");
    }

    if (volume.getQuotaInNamespace() == OLD_QUOTA_DEFAULT) {
      String msg = "Volume " + volumeName + " is created before version " +
          "1.1.0, usedNamespace may be inaccurate and it is not recommended" +
          " to enable quota.";
      printMsg(msg);
    }

    volume.setQuota(OzoneQuota.getOzoneQuota(spaceQuota, namespaceQuota));
  }
}
