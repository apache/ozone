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

package org.apache.hadoop.ozone.recon.upgrade;

import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrade action that triggers a rebuild of the NSSummary tree to
 * populate materialized totals upon upgrade to the feature version.
 *
 * This runs at FINALIZE and schedules the rebuild asynchronously so
 * Recon startup is not blocked. During rebuild, APIs that depend on
 * the tree may return initializing responses as designed.
 */
@UpgradeActionRecon(feature = ReconVersion.NSSUMMARY_AGGREGATED_TOTALS)
public class NSSummaryAggregatedTotalsUpgrade implements ReconUpgradeAction {

  private static final Logger LOG = LoggerFactory.getLogger(NSSummaryAggregatedTotalsUpgrade.class);

  @Override
  public void execute(DataSource source) throws Exception {
    ReconUpgradeAction.queueNSSummaryRebuildIfNeeded(LOG);
  }
}
