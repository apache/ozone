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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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

  private static final class State {
    final int metadataLayoutVersion; // MLV
    final int softwareLayoutVersion; // SLV
    final Status currentUpgradeState;

    private State(int mlv, int slv, Status status) {
      this.metadataLayoutVersion = mlv;
      this.softwareLayoutVersion = slv;
      this.currentUpgradeState = status;
    }

    private static Status computeStatus(int mlv, int slv) {
      return (mlv >= slv) ? ALREADY_FINALIZED : FINALIZATION_REQUIRED;
    }

    private State withStatus(Status status) {
      return new State(metadataLayoutVersion, softwareLayoutVersion, status);
    }

    private State withMlv(int newMlv) {
      return new State(newMlv, softwareLayoutVersion, computeStatus(newMlv, softwareLayoutVersion));
    }
  }

  private final AtomicReference<State> state = new AtomicReference<>();

  private final AtomicBoolean initialized = new AtomicBoolean(false);

  @VisibleForTesting
  protected volatile NavigableMap<Integer, LayoutFeature> features =
      Collections.unmodifiableNavigableMap(new TreeMap<>());
  @VisibleForTesting
  protected volatile Map<String, LayoutFeature> featureMap =
      Collections.unmodifiableMap(new HashMap<>());
  // Allows querying upgrade state while an upgrade is in progress.
  // Note that MLV may have been incremented during the upgrade
  // by the time the value is read/used.
  private ObjectName mBean;

  private State requireState() {
    State s = state.get();
    Preconditions.checkState(s != null, "LayoutVersionManager is not initialized.");
    return s;
  }

  protected void init(int version, T[] lfs) throws IOException {
    Preconditions.checkArgument(initialized.compareAndSet(false, true),
        "LayoutVersionManager is already initialized.");
    final TreeMap<Integer, LayoutFeature> localFeatures = new TreeMap<>();
    final Map<String, LayoutFeature> localFeatureMap = new HashMap<>();
    Arrays.stream(lfs).forEach(f -> {
      Preconditions.checkArgument(!localFeatureMap.containsKey(f.name()));
      Preconditions.checkArgument(!localFeatures.containsKey(f.layoutVersion()));
      localFeatures.put(f.layoutVersion(), f);
      localFeatureMap.put(f.name(), f);
    });

    if (localFeatures.isEmpty()) {
      throw new IOException("Cannot initialize VersionManager with no layout features.");
    }

    final int mlv = version;
    final int slv = localFeatures.lastKey();

    if (mlv > slv) {
      throw new IOException(String.format(
          "Cannot initialize VersionManager. Metadata layout version (%d) > software layout version (%d)",
          mlv, slv));
    }

    // publish immutable maps (safe publication via volatile write)
    this.features = Collections.unmodifiableNavigableMap(localFeatures);
    this.featureMap = Collections.unmodifiableMap(localFeatureMap);

    // set atomic state snapshot once
    state.set(new State(mlv, slv, State.computeStatus(mlv, slv)));

    LayoutFeature mlvFeature = features.get(mlv);
    LayoutFeature slvFeature = features.get(slv);
    LOG.info("Initializing Layout version manager with metadata layout" +
            " = {} (version = {}), software layout = {} (version = {})",
        mlvFeature, mlvFeature.layoutVersion(),
        slvFeature, slvFeature.layoutVersion());

    mBean = MBeans.register("LayoutVersionManager",
        getClass().getSimpleName(), this);
  }

  public Status getUpgradeState() {
    return requireState().currentUpgradeState;
  }

  public void setUpgradeState(Status status) {
    Preconditions.checkNotNull(status, "status");
    while (true) {
      State cur = requireState();
      State next = cur.withStatus(status);
      if (state.compareAndSet(cur, next)) {
        return;
      }
    }
  }

  public void finalized(T layoutFeature) {
    Preconditions.checkNotNull(layoutFeature, "layoutFeature");

    while (true) {
      State cur = requireState();
      int mlv = cur.metadataLayoutVersion;
      int slv = cur.softwareLayoutVersion;
      int lv = layoutFeature.layoutVersion();

      if (lv > slv) {
        String versionMsg = "Software layout version: " + slv
            + ", Metadata layout version: " + mlv
            + ", Feature Layout version: " + lv + ".";
        throw new IllegalArgumentException(
            "Finalize attempt on a layoutFeature that is newer than the software layout version. "
                + versionMsg);
      }

      if (lv == mlv + 1) {
        State next = cur.withMlv(lv);
        if (state.compareAndSet(cur, next)) {
          LOG.info("Layout feature {} has been finalized.", layoutFeature);
          if (!needsFinalization()) {
            LOG.info("Finalization is complete.");
          }
          return;
        }
        // CAS failed due to concurrent update; retry
        continue;
      }

      String versionMsg = "Software layout version: " + slv
          + ", Metadata layout version: " + mlv
          + ", Feature Layout version: " + lv + ".";

      if (lv <= mlv) {
        LOG.info("Finalize attempt on a layoutFeature which has already "
            + "been finalized. " + versionMsg + " This can happen when " +
            "Raft Log is replayed during service restart.");
        return;
      }

      throw new IllegalArgumentException(
          "Finalize attempt on a layoutFeature that is newer than the " +
              "next feature to be finalized. " + versionMsg);
    }
  }

  private boolean softwareIsBehindMetaData() {
    State s = requireState();
    return s.metadataLayoutVersion > s.softwareLayoutVersion;
  }

  @Override
  public int getMetadataLayoutVersion() {
    return requireState().metadataLayoutVersion;
  }

  @Override
  public int getSoftwareLayoutVersion() {
    return requireState().softwareLayoutVersion;
  }

  @Override
  public boolean needsFinalization() {
    State s = requireState();
    return s.metadataLayoutVersion < s.softwareLayoutVersion;
  }

  @Override
  public boolean isAllowed(LayoutFeature layoutFeature) {
    Preconditions.checkNotNull(layoutFeature, "layoutFeature");
    return layoutFeature.layoutVersion() <= requireState().metadataLayoutVersion;
  }

  @Override
  public boolean isAllowed(String featureName) {
    LayoutFeature f = featureMap.get(featureName);
    return f != null && isAllowed(f);
  }

  @Override
  public LayoutFeature getFeature(String name) {
    return featureMap.get(name);
  }

  @Override
  public LayoutFeature getFeature(int layoutVersion) {
    return features.get(layoutVersion);
  }

  @Override
  public Iterable<LayoutFeature> unfinalizedFeatures() {
    int mlv = requireState().metadataLayoutVersion;
    return new ArrayList<>(features
        .tailMap(mlv + 1)
        .values());
  }

  @Override
  public void close() {
    ObjectName bean = mBean;
    if (bean != null) {
      MBeans.unregister(bean);
      mBean = null;
    }
  }
}
