/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package org.apache.hadoop.ozone.om.upgrade;

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.upgrade.UpgradeException;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_METADATA_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_METADATA_LAYOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.FSO;
import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.VALIDATE_IN_PREFINALIZE;

/**
 * Checks that File System Optimization cannot be used when the Ozone Manager
 * is pre-finalized for file system optimization feature.
 */
@UpgradeActionOm(feature = FSO, type = VALIDATE_IN_PREFINALIZE)
public class FSOUnfinalizedStateValidationAction
    implements OmUpgradeAction {

  @Override
  public void execute(OzoneManager om) throws UpgradeException {
    if (om.getEnableFileSystemPaths()) {
      throwExceptionForConfig(OZONE_OM_ENABLE_FILESYSTEM_PATHS);
    }

    if (!om.getOMMetadataLayout()
        .equalsIgnoreCase(OZONE_OM_METADATA_LAYOUT_DEFAULT)) {
      throwExceptionForConfig(OZONE_OM_METADATA_LAYOUT);
    }
  }

  public void throwExceptionForConfig(String config) throws UpgradeException {
    throw new UpgradeException(String.format("Configuration %s cannot be " +
            "used until OM upgrade has been finalized", config),
        UpgradeException.ResultCodes.PREFINALIZE_ACTION_VALIDATION_FAILED);
  }
}

