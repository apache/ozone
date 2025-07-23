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

import com.google.inject.Injector;
import javax.sql.DataSource;
import org.apache.hadoop.ozone.recon.ReconGuiceServletContextListener;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrade action for the REPLICATED_SIZE_OF_FILES layout feature.
 * The action triggers a full rebuild of the NSSummary ensuring that the new field: replicatedSizeOfFiles is correctly
 * populated for all objects.
 */
@UpgradeActionRecon(feature = ReconLayoutFeature.REPLICATED_SIZE_OF_FILES,
    type = ReconUpgradeAction.UpgradeActionType.FINALIZE)
public class ReplicatedSizeOfFilesUpgradeAction implements ReconUpgradeAction {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicatedSizeOfFilesUpgradeAction.class);

  @Override
  public void execute(DataSource dataSource) {
    try {
      Injector injector = ReconGuiceServletContextListener.getStaticInjector();
      if (injector == null) {
        throw new IllegalStateException("Guice injector is not initialized. Cannot perform NSSummary rebuild.");
      }
      ReconNamespaceSummaryManager nsSummaryManager = injector.getInstance(ReconNamespaceSummaryManager.class);
      ReconOMMetadataManager omMetadataManager = injector.getInstance(ReconOMMetadataManager.class);
      LOG.info("Starting full rebuild of NSSummary for REPLICATED_SIZE_OF_FILES upgrade...");
      nsSummaryManager.rebuildNSSummaryTree(omMetadataManager);
      LOG.info("Completed full rebuild of NSSummary for REPLICATED_SIZE_OF_FILES upgrade.");
    } catch (Exception e) {
      LOG.error("Error during NSSummary rebuild for REPLICATED_SIZE_OF_FILES upgrade.", e);
      throw new RuntimeException("Failed to rebuild NSSummary during upgrade", e);
    }
  }

  @Override
  public UpgradeActionType getType() {
    return UpgradeActionType.FINALIZE;
  }
} 
