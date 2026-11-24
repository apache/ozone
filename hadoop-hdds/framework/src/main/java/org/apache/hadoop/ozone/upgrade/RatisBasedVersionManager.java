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

  public void finalizeFromDBIfRequired(Table<String, String> finalizationStore) throws IOException {
    // This call is expected to throw and crash the process if the apparent version in the DB is larger than our
    // software version, which we cannot support.
    ComponentVersion apparentVersionInDB = getApparentVersionInDB(finalizationStore);
    if (apparentVersionInDB == null) {
      LOG.debug("Component has apparent version {} but no apparent version was found in the DB. That is expected if " +
          "this component has never been finalized to a newer version.", getApparentVersion());
    } else if (!apparentVersionInDB.equals(getApparentVersion())) {
      // We only need to take potential action if DB version != apparent version.
      if (isAllowed(apparentVersionInDB)) {
        // DB version < apparent version.
        LOG.debug("Received apparent version {} in DB which is less than the current apparent version {}. " +
            "Component likely previously crashed during finalization. Ratis log replay should correct this issue " +
            "after startup.",
            apparentVersionInDB, getApparentVersion());
      } else {
        // DB version > apparent version. We must finalize to this version.
        if (apparentVersionInDB.isSupportedBy(getSoftwareVersion()) &&
            !apparentVersionInDB.equals(getSoftwareVersion())) {
          // DB version < software version.
          // This should never happen. When restarting after failed finalization, the DB should not have been updated
          // until all versions up to the software version were finalized. On the snapshot installation path all
          // components should be running the same software version when finalization is run.
          LOG.warn("Received apparent version {} in DB which is between the current apparent version {} " +
              "and the software version {}. User may have incorrectly run finalization in a mixed version " +
              "cluster.", apparentVersionInDB, getApparentVersion(), getSoftwareVersion());
        } else {
          // DB version == software version. This is expected when we receive finalization from a Ratis snapshot.
          // The previous attempt to finalize from a snapshot may have failed, so we will resume it here.
          LOG.info("Version file has lower apparent version ({}) than DB ({}). Finalizing to the DB version.",
              getApparentVersion(), apparentVersionInDB);
        }
        finalizeUpgrade(apparentVersionInDB);
      }
    }
  }

  protected abstract ComponentVersion computeApparentVersion(int serializedVersion) throws IOException;

  private ComponentVersion getApparentVersionInDB(Table<String, String> finalizationStore) throws IOException {
    String apparentVersion = finalizationStore.get(APPARENT_VERSION_KEY);
    return (apparentVersion == null) ? null : computeApparentVersion(Integer.parseInt(apparentVersion));
  }
}
