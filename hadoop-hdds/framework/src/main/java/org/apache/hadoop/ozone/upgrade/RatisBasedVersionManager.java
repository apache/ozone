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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base version manager implementation for ratis-backed component versions.
 */
public abstract class RatisBasedVersionManager extends ComponentVersionManager {

  private static final Logger LOG = LoggerFactory.getLogger(RatisBasedVersionManager.class);

  protected RatisBasedVersionManager(ComponentVersion apparentVersion, ComponentVersion softwareVersion) {
    super(apparentVersion, softwareVersion);
  }

  public void validateDBVersion(Table<String, String> finalizationStore) throws IOException {
    ComponentVersion dbVersion = getApparentVersionInDB(finalizationStore);
    ComponentVersion apparentVersion = getApparentVersion();

    if (!apparentVersion.equals(dbVersion)) {
      LOG.info("Version file has different apparent version ({}) than DB ({}). That is expected if this "
          + "component has never been finalized to a newer version.", apparentVersion, dbVersion);
    }
  }

  public void finalizeFromDBIfRequired(Table<String, String> finalizationStore)
      throws IOException {
    ComponentVersion apparentVersionInNewDB = getApparentVersionInDB(finalizationStore);
    if (apparentVersionInNewDB == null) {
      LOG.info("Component has apparent version {} but no apparent version was found in the DB. That is expected if " +
          "this component has never been finalized to a newer version.", getApparentVersion());
    } else if (!isAllowed(apparentVersionInNewDB)) {
      // The DB is ahead of the VERSION file. This can happen if the leader finalized and this follower received the
      // update through a snapshot. It can also happen if the component was finalizing from a snapshot, failed
      // partway through, and restarted.
      if (apparentVersionInNewDB != getSoftwareVersion()) {
        LOG.warn("Received apparent version {} in DB which does not match the current apparent version {} or " +
                "software version {}. User may have incorrectly run finalization before all components were " +
                "upgraded. Finalizing this component to its software version.",
            apparentVersionInNewDB, getApparentVersion(), getSoftwareVersion());
      } else if (getApparentVersion().serialize() < apparentVersionInNewDB.serialize()) {
        LOG.info("Version file has lower apparent version ({}) than DB ({}). Finalizing to the DB version.",
            getApparentVersion(), apparentVersionInNewDB);
      }
      finalizeUpgrade();
    } else if (!apparentVersionInNewDB.equals(getApparentVersion())) {
      // The DB is behind the VERSION file. This should not happen: finalization writes the VERSION file inside
      // applyTransaction, so the Ratis log entry was committed and should be replayed on restart, which would
      // update the DB. This is a defensive branch to catch any potential bugs.
      LOG.warn("DB has lower apparent version ({}) than VERSION file ({}). Updating DB to match VERSION file.",
          apparentVersionInNewDB, getApparentVersion());
      finalizationStore.put(APPARENT_VERSION_KEY, String.valueOf(getApparentVersion().serialize()));
    }
  }

  protected abstract ComponentVersion computeApparentVersion(int serializedVersion) throws IOException;

  private ComponentVersion getApparentVersionInDB(Table<String, String> finalizationStore) throws IOException {
    String apparentVersion = finalizationStore.get(APPARENT_VERSION_KEY);
    return (apparentVersion == null) ? null : computeApparentVersion(Integer.parseInt(apparentVersion));
  }
}
