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

import com.google.inject.Injector;
import javax.sql.DataSource;
import org.apache.hadoop.ozone.recon.ReconGuiceServletContextListener;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskController;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskReInitializationEvent;
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
      throw new IllegalStateException(
          "Guice injector not initialized. NSSummary rebuild cannot proceed during upgrade.");
    }

    ReconTaskController reconTaskController = injector.getInstance(ReconTaskController.class);
    LOG.info("Triggering asynchronous NSSummary tree rebuild for materialized totals (upgrade action).");
    ReconTaskController.ReInitializationResult result = reconTaskController.queueReInitializationEvent(
        ReconTaskReInitializationEvent.ReInitializationReason.MANUAL_TRIGGER);
    if (result != ReconTaskController.ReInitializationResult.SUCCESS) {
      LOG.error(
          "Failed to queue reinitialization event for manual trigger (result: {}), failing the reinitialization " +
              "during NSSummaryAggregatedTotalsUpgrade action, will be retried as part of syncDataFromOM " +
              "scheduler task.", result);
    }
  }

  @Override
  public UpgradeActionType getType() {
    return FINALIZE;
  }
}
