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

package org.apache.hadoop.hdds.scm.server.upgrade;

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.SCM_HA;
import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.VALIDATE_IN_PREFINALIZE;
import static org.apache.hadoop.ozone.upgrade.UpgradeActionHdds.Component.SCM;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.upgrade.HDDSUpgradeAction;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.UpgradeActionHdds;
import org.apache.hadoop.ozone.upgrade.UpgradeException;

import java.io.IOException;

/**
 * Checks that SCM HA cannot be used in a pre-finalized cluster, unless it
 * was already being used before this action was run.
 */
@UpgradeActionHdds(feature = SCM_HA, component = SCM,
    type = VALIDATE_IN_PREFINALIZE)
public class ScmHAUnfinalizedStateValidationAction
    implements HDDSUpgradeAction<SCMUpgradeFinalizationContext> {

  @Override
  public void execute(SCMUpgradeFinalizationContext context)
      throws IOException {
    checkScmHA(context.getConfiguration(), context.getStorage(),
        context.getLayoutVersionManager());
  }

  /**
   * Allows checking that SCM HA is not enabled while pre-finalized in both
   * scm init and the upgrade action run on start.
   */
  public static void checkScmHA(OzoneConfiguration conf,
      SCMStorageConfig storageConf, LayoutVersionManager versionManager)
      throws IOException {

    // Since this action may need to be called outside the upgrade framework
    // during init, it needs to check for pre-finalized state.
    if (!versionManager.isAllowed(SCM_HA) &&
        SCMHAUtils.isSCMHAEnabled(conf) &&
        !storageConf.isSCMHAEnabled()) {
      throw new UpgradeException(String.format("Configuration %s cannot be " +
              "used until SCM upgrade has been finalized",
          ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY),
          UpgradeException.ResultCodes.PREFINALIZE_ACTION_VALIDATION_FAILED);
    }
  }
}
