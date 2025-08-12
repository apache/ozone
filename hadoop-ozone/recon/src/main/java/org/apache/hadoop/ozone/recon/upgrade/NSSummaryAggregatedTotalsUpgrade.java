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

import static org.apache.hadoop.ozone.recon.upgrade.ReconUpgradeAction.UpgradeActionType.FINALIZE;

import javax.sql.DataSource;
import com.google.inject.Injector;
import org.apache.hadoop.ozone.recon.ReconGuiceServletContextListener;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
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
@UpgradeActionRecon(feature = ReconLayoutFeature.NSSUMMARY_AGGREGATED_TOTALS, type = FINALIZE)
public class NSSummaryAggregatedTotalsUpgrade implements ReconUpgradeAction {

  private static final Logger LOG = LoggerFactory.getLogger(NSSummaryAggregatedTotalsUpgrade.class);

  @Override
  public void execute(DataSource source) throws Exception {
    // Resolve required services from Guice
    Injector injector = ReconGuiceServletContextListener.getGlobalInjector();
    if (injector == null) {
      // Should not happen since ReconServer sets the injector before finalize call
      LOG.warn("Guice injector not initialized yet; skipping NSSummary rebuild at upgrade.");
      return;
    }

    ReconNamespaceSummaryManager nsMgr = injector.getInstance(ReconNamespaceSummaryManager.class);
    ReconOMMetadataManager omMgr = injector.getInstance(ReconOMMetadataManager.class);

    // Fire and forget: unified control using ReconUtils -> NSSummaryTask
    LOG.info("Triggering asynchronous NSSummary tree rebuild for materialized totals (upgrade action).");
    ReconUtils.triggerAsyncNSSummaryRebuild(nsMgr, omMgr);
  }

  @Override
  public UpgradeActionType getType() {
    return FINALIZE;
  }
}
