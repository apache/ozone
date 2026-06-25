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

package org.apache.hadoop.ozone.om.upgrade;

import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.QUOTA;

import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.service.QuotaRepairTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Quota repair for usages action to be triggered after upgrade.
 */
@UpgradeActionOm(feature = QUOTA)
public class QuotaRepairUpgradeAction implements OmUpgradeAction {
  private static final Logger LOG = LoggerFactory.getLogger(QuotaRepairUpgradeAction.class);

  @Override
  public void execute(OzoneManager arg) throws Exception {
    boolean enabled = arg.getConfiguration().getBoolean(
        OMConfigKeys.OZONE_OM_UPGRADE_QUOTA_RECALCULATE_ENABLE,
        OMConfigKeys.OZONE_OM_UPGRADE_QUOTA_RECALCULATE_ENABLE_DEFAULT);
    if (enabled) {
      // just trigger quota repair and status can be checked via CLI
      try {
        arg.checkLeaderStatus();
        QuotaRepairTask quotaRepairTask = new QuotaRepairTask(arg);
        quotaRepairTask.repair();
      } catch (OMNotLeaderException | OMLeaderNotReadyException ex) {
        // on leader node, repair will be triggered where finalize is called. For other nodes, it will be ignored.
        // This can be triggered on need basis via CLI tool.
        LOG.warn("Skip quota repair operation during upgrade on the node as this is not a leader node.");
      }
    }
  }
}
