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

package org.apache.hadoop.hdds.scm.server.upgrade;

import static org.apache.hadoop.hdds.HDDSVersion.ZDU;

import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.upgrade.ScmUpgradeAction;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.upgrade.ScmUpgradeActionForVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Removes the orphan "finalizing in progress" mark written into the SCM meta table by pre-ZDU
 * code. Deleting an absent key is a no-op, making the action idempotent.
 */
@ScmUpgradeActionForVersion(version = ZDU)
public class ClearFinalizingStateScmUpgradeAction implements ScmUpgradeAction {
  private static final Logger LOG = LoggerFactory.getLogger(ClearFinalizingStateScmUpgradeAction.class);

  // Orphan "finalizing in progress" mark written by pre-ZDU SCM code; removed on ZDU finalization.
  private static final String LEGACY_FINALIZING_KEY = "#FINALIZING";

  @Override
  public void execute(OzoneStorageContainerManager context) throws Exception {
    StorageContainerManager scm = (StorageContainerManager) context;
    Table<String, String> metaTable = scm.getScmMetadataStore().getMetaTable();
    scm.getScmHAManager().getDBTransactionBuffer().removeFromBuffer(metaTable, LEGACY_FINALIZING_KEY);
    LOG.info("Removed leftover SCM finalizing mark {} during ZDU finalization.", LEGACY_FINALIZING_KEY);
  }
}
