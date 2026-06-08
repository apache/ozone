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

package org.apache.hadoop.ozone.upgrade;

import static org.apache.hadoop.ozone.OzoneConsts.APPARENT_VERSION_KEY;

import java.io.IOException;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.common.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base version manager implementation for ratis-backed component versions.
 */
public abstract class RatisBasedVersionManager extends ComponentVersionManager {

  private static final Logger LOG = LoggerFactory.getLogger(RatisBasedVersionManager.class);

  protected RatisBasedVersionManager(Storage storage, ComponentVersion apparentVersion,
      ComponentVersion softwareVersion) {
    super(storage, apparentVersion, softwareVersion);
  }

  public void validateDBVersion(Table<String, String> finalizationStore) throws IOException {
    ComponentVersion dbVersion = getApparentVersionInDB(finalizationStore);
    ComponentVersion apparentVersion = getApparentVersion();

    if (!apparentVersion.equals(dbVersion)) {
      LOG.info("Version file has different apparent version ({}) than DB ({}). That is expected if this "
          + "component has never been finalized to a newer version.", apparentVersion, dbVersion);
    }
  }

  public void finalizeFromSnapshotIfRequired(Table<String, String> finalizationStore) throws IOException {
    ComponentVersion apparentVersionInNewDB = getApparentVersionInDB(finalizationStore);
    if (apparentVersionInNewDB != null && !isAllowed(apparentVersionInNewDB)) {
      LOG.info("New snapshot received with higher apparent version {}. Attempting to finalize to that version.",
          apparentVersionInNewDB);
      finalizeUpgrade();
      // Update the apparent version in the DB to match the VERSION file.
      // When finalization is not done with a snapshot, this DB value is updated by OMFinalizeUpgradeRequest.
      finalizationStore.put(APPARENT_VERSION_KEY, String.valueOf(getApparentVersion().serialize()));
    }
  }

  protected abstract ComponentVersion computeApparentVersion(int serializedVersion) throws IOException;

  private ComponentVersion getApparentVersionInDB(Table<String, String> finalizationStore) throws IOException {
    String apparentVersion = finalizationStore.get(APPARENT_VERSION_KEY);
    return (apparentVersion == null) ? null : computeApparentVersion(Integer.parseInt(apparentVersion));
  }
}
