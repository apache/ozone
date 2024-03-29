/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.ozone.om.upgrade;

import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.service.QuotaRepairTask;

import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.QUOTA;
import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.ON_FIRST_UPGRADE_START;

/**
 * Quota repair for usages action to be triggered during first upgrade.
 */
@UpgradeActionOm(type = ON_FIRST_UPGRADE_START, feature =
    QUOTA)
public class QuotaRepairUpgradeAction implements OmUpgradeAction {
  @Override
  public void execute(OzoneManager arg) throws Exception {
    boolean enabled = arg.getConfiguration().getBoolean(
        OMConfigKeys.OZONE_OM_UPGRADE_QUOTA_RECALCULATE_ENABLE,
        OMConfigKeys.OZONE_OM_UPGRADE_QUOTA_RECALCULATE_ENABLE_DEFAULT);
    if (enabled) {
      QuotaRepairTask quotaRepairTask = new QuotaRepairTask(
          arg.getMetadataManager());
      quotaRepairTask.repair();
    }
  }
}
