/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.security.acl.OzonePrefixPath;

import java.io.IOException;
import java.util.List;

public class OzonePrefixPathImpl implements OzonePrefixPath {

  private KeyManager keyManager;

  public OzonePrefixPathImpl(KeyManager keyManagerImpl){
    this.keyManager = keyManagerImpl;
  }

  @Override
  public List<OzoneFileStatus> getChildren(String volumeName,
      String bucketName, String keyPrefix) throws IOException {

    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyPrefix)
        .setRefreshPipeline(false)
        .build();

    List<OzoneFileStatus> statuses = keyManager.listStatus(omKeyArgs, false,
        null, Integer.MAX_VALUE);

    if (statuses.size() == 1) {
      OzoneFileStatus keyStatus = statuses.get(0);
      if (keyStatus.isFile() && StringUtils.equals(keyPrefix,
          keyStatus.getTrimmedName())) {
        throw new OMException("Invalid KeyPath, file name: " + keyPrefix +
            " is not allowed.", OMException.ResultCodes.INVALID_KEY_NAME);
      }
    }

    return statuses;
  }
}
