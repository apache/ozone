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

import static org.apache.hadoop.ozone.OzoneManagerVersion.ZDU;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Removes leftover OM "prepare for upgrade" state written by pre-ZDU code.
 * It is idempotent (delete-if-exists), so it is a no-op on clusters that were never prepared and
 * on fresh ZDU clusters that never finalize.
 */
@OmUpgradeActionForVersion(version = ZDU)
public class ClearPreparedStateOmUpgradeAction implements OmUpgradeAction {
  private static final Logger LOG = LoggerFactory.getLogger(ClearPreparedStateOmUpgradeAction.class);

  // On-disk marker written by pre-ZDU OM prepare code; removed on ZDU finalization.
  private static final String LEGACY_PREPARE_MARKER = "prepareMarker";
  // transactionInfoTable key written by pre-ZDU OM prepare code.
  private static final String LEGACY_PREPARE_MARKER_KEY = "#PREPAREDINFO";

  @Override
  public void execute(OzoneManager om) throws Exception {
    // Reproduces the removed getPrepareMarkerFile() logic: <metadata dir>/current/prepareMarker.
    File markerDir = new File(ServerUtils.getOzoneMetaDirPath(om.getConfiguration()),
        Storage.STORAGE_DIR_CURRENT);
    File marker = new File(markerDir, LEGACY_PREPARE_MARKER);
    if (marker.exists()) {
      if (!marker.delete()) {
        throw new IOException("Failed to delete leftover OM prepare marker file " + marker);
      }
      LOG.info("Deleted leftover OM prepare marker file {}", marker);
    }

    // Direct RocksDB delete of the orphan prepare key, mirroring the removed startup cleanup.
    om.getMetadataManager().getTransactionInfoTable().delete(LEGACY_PREPARE_MARKER_KEY);
  }
}
