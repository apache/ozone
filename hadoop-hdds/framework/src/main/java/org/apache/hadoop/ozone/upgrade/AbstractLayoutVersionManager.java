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

import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.ALREADY_FINALIZED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.FINALIZATION_REQUIRED;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.management.ObjectName;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Layout Version Manager containing generic method implementations.
 */
@SuppressWarnings("visibilitymodifier")
public abstract class AbstractLayoutVersionManager<T extends LayoutFeature>
    implements LayoutVersionManager, LayoutVersionManagerMXBean {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractLayoutVersionManager.class);

  private volatile int metadataLayoutVersion; // MLV.
  private volatile int softwareLayoutVersion; // SLV.
  @VisibleForTesting
  protected final TreeMap<Integer, LayoutFeature> features = new TreeMap<>();
  private volatile Status currentUpgradeState;
  // Allows querying upgrade state while an upgrade is in progress.
  // Note that MLV may have been incremented during the upgrade
  // by the time the value is read/used.
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private ObjectName mBean;

  protected void init(int version, T[] lfs) throws IOException {
    lock.writeLock().lock();
    try {
      metadataLayoutVersion = version;
      initializeFeatures(lfs);
      softwareLayoutVersion = features.lastKey();
      if (softwareIsBehindMetaData()) {
        throw new IOException(
            String.format("Cannot initialize VersionManager. Metadata " +
                    "layout version (%d) > software layout version (%d)",
                metadataLayoutVersion, softwareLayoutVersion));
      } else if (metadataLayoutVersion == softwareLayoutVersion) {
        currentUpgradeState = ALREADY_FINALIZED;
      } else {
        currentUpgradeState = FINALIZATION_REQUIRED;
      }

      LayoutFeature mlvFeature = features.get(metadataLayoutVersion);
      LayoutFeature slvFeature = features.get(softwareLayoutVersion);
      LOG.info("Initializing Layout version manager with metadata layout" +
              " = {} (version = {}), software layout = {} (version = {})",
          mlvFeature, mlvFeature.layoutVersion(),
          slvFeature, slvFeature.layoutVersion());

      mBean = MBeans.register("LayoutVersionManager",
          getClass().getSimpleName(), this);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Status getUpgradeState() {
    lock.readLock().lock();
    try {
      return currentUpgradeState;
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setUpgradeState(Status status) {
    lock.writeLock().lock();
    try {
      currentUpgradeState = status;
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void initializeFeatures(T[] lfs) {
    Arrays.stream(lfs).forEach(f -> {
      Preconditions.checkArgument(!features.containsKey(f.layoutVersion()));
      features.put(f.layoutVersion(), f);
    });
  }

  public void finalized(T layoutFeature) {
    lock.writeLock().lock();
    try {
      if (layoutFeature.layoutVersion() == metadataLayoutVersion + 1) {
        metadataLayoutVersion = layoutFeature.layoutVersion();
        LOG.info("Layout feature {} has been finalized.", layoutFeature);
        if (!needsFinalization()) {
          LOG.info("Finalization is complete.");
        }
      } else {
        String versionMsg = "Software layout version: " + softwareLayoutVersion
            + ", Metadata layout version: " + metadataLayoutVersion
            + ", Feature Layout version: " + layoutFeature.layoutVersion()
            + ".";

        if (layoutFeature.layoutVersion() <= metadataLayoutVersion) {
          LOG.info("Finalize attempt on a layoutFeature which has already "
              + "been finalized. " + versionMsg + " This can happen when " +
              "Raft Log is replayed during service restart.");
        } else {
          throw new IllegalArgumentException(
              "Finalize attempt on a layoutFeature that is newer than the " +
                  "next feature to be finalized. " + versionMsg);
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private boolean softwareIsBehindMetaData() {
    lock.readLock().lock();
    try {
      return metadataLayoutVersion > softwareLayoutVersion;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public int getMetadataLayoutVersion() {
    lock.readLock().lock();
    try {
      return metadataLayoutVersion;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public int getSoftwareLayoutVersion() {
    return softwareLayoutVersion;
  }

  @Override
  public boolean needsFinalization() {
    lock.readLock().lock();
    try {
      return metadataLayoutVersion < softwareLayoutVersion;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean isAllowed(LayoutFeature layoutFeature) {
    lock.readLock().lock();
    try {
      return layoutFeature.layoutVersion() <= metadataLayoutVersion;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public LayoutFeature getFeature(int layoutVersion) {
    return features.get(layoutVersion);
  }

  @Override
  public Iterable<LayoutFeature> unfinalizedFeatures() {
    lock.readLock().lock();
    try {
      return new ArrayList<>(features
          .tailMap(metadataLayoutVersion + 1)
          .values());
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void close() {
    if (mBean != null) {
      MBeans.unregister(mBean);
      mBean = null;
    }
  }
}
