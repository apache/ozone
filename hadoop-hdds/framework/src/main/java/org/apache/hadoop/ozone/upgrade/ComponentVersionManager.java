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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.hadoop.hdds.ComponentVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks information about the apparent version, software version, and finalization status of a component.
 *
 * Software version: The {@link ComponentVersion} of the code that is currently running.
 *    This is always the highest component version within the code and does not change while the process is running.
 *
 * Apparent version: The {@link ComponentVersion} the software is acting as, which is persisted to the disk.
 *    The apparent version determines the API that is exposed by the component and the format it uses to persist data.
 *    Using an apparent version less than software version allows us to support rolling upgrades and downgrades.
 *
 * Pre-finalized: State a component enters when the apparent version on disk is less than the software version.
 *    At this time all other machines may or may not be running the new bits, new features are blocked, and downgrade
 *    is allowed.
 *
 * Finalized: State a component enters when the apparent version is equal to the software version.
 *    A component transitions from pre-finalized to finalized when it receives a finalize command from the
 *    admin. At this time all machines are running the new bits, and even though this component is finalized,
 *    different types of components may not be. Downgrade is not allowed after this point.
 *
 */
public abstract class ComponentVersionManager implements Closeable {
  // Apparent version may be updated during the finalization process.
  private volatile ComponentVersion apparentVersion;
  // Software version will never change.
  private final ComponentVersion softwareVersion;
  private final ComponentVersionManagerMetrics metrics;

  private static final Logger LOG =
      LoggerFactory.getLogger(ComponentVersionManager.class);

  protected ComponentVersionManager(ComponentVersion apparentVersion, ComponentVersion softwareVersion)
      throws IOException {
    this.apparentVersion = apparentVersion;
    this.softwareVersion = softwareVersion;

    if (!apparentVersion.isSupportedBy(softwareVersion)) {
      throw new IOException(
          "Cannot initialize ComponentVersionManager. Apparent version "
              + apparentVersion + " is larger than software version "
              + softwareVersion);
    }

    LOG.info("Initializing version manager with apparent version {} and software version {}",
        apparentVersion, softwareVersion);
    this.metrics = ComponentVersionManagerMetrics.create(this);
  }

  public ComponentVersion getApparentVersion() {
    return apparentVersion;
  }

  public ComponentVersion getSoftwareVersion() {
    return softwareVersion;
  }

  public boolean isAllowed(ComponentVersion version) {
    return version.isSupportedBy(apparentVersion);
  }

  public boolean needsFinalization() {
    return !apparentVersion.equals(softwareVersion);
  }

  /**
   * @return An Iterable of all versions after the current apparent version which still need to be finalized. If this
   *    component is already finalized, the Iterable will be empty.
   */
  public Iterable<ComponentVersion> getUnfinalizedVersions() {
    return () -> new Iterator<ComponentVersion>() {
      private ComponentVersion currentVersion = apparentVersion;

      @Override
      public boolean hasNext() {
        return currentVersion.nextVersion() != null;
      }

      @Override
      public ComponentVersion next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        currentVersion = currentVersion.nextVersion();
        return currentVersion;
      }
    };
  }

  /**
   * Validates that the provided version is valid to finalize to, and if so, updates the in-memory apparent version to
   * this version. Also logs corresponding messages about finalization status.
   *
   * @param newApparentVersion The version to mark as finalized.
   */
  public void markFinalized(ComponentVersion newApparentVersion) {
    String versionMsg = "Software version: " + softwareVersion
        + ", apparent version: " + apparentVersion
        + ", provided version: " + newApparentVersion
        + ".";

    if (newApparentVersion.isSupportedBy(apparentVersion)) {
      LOG.info("Finalize attempt on a version which has already been finalized. {} This can happen when " +
          "Raft Log is replayed during service restart.", versionMsg);
    } else {
      ComponentVersion nextVersion = apparentVersion.nextVersion();
      if (nextVersion == null) {
        throw new IllegalArgumentException("Attempt to finalize when no future versions exist." + versionMsg);
      } else if (nextVersion.equals(newApparentVersion)) {
        apparentVersion = newApparentVersion;
        LOG.info("Version {} has been finalized.", apparentVersion);
        if (!needsFinalization()) {
          LOG.info("Finalization is complete.");
        }
      } else {
        throw new IllegalArgumentException(
            "Finalize attempt on a version that is newer than the next feature to be finalized. " + versionMsg);
      }
    }
  }

  @Override
  public void close() {
    metrics.unRegister();
  }
}
