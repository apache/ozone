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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.utils.db.InMemoryTestTable;
import org.apache.hadoop.hdds.utils.db.Table;
import org.junit.jupiter.api.Test;

/**
 * Shared tests for {@link RatisBasedVersionManager} implementations, covering how the apparent version in the VERSION
 * file is reconciled with the apparent version recorded in the DB on startup.
 *
 * <p>A component that stops during a Ratis snapshot install after its DB was replaced, but before its VERSION file was
 * updated, cannot replay the finalization from the Raft log, so it must finalize from the DB on restart. Component
 * tests only need to verify that their real startup path calls
 * {@link RatisBasedVersionManager#finalizeFromDBIfRequired}.
 */
public abstract class AbstractRatisBasedVersionManagerTest extends AbstractComponentVersionManagerTest {

  @Override
  protected abstract RatisBasedVersionManager createManager(int serializedApparentVersion) throws IOException;

  /**
   * @return The last legacy layout feature of this component. Serialized values between it and ZDU are not valid.
   */
  protected abstract ComponentVersion lastLayoutFeature();

  @Test
  public void testApparentVersionBetweenLastLayoutFeatureAndZduRejected() {
    int serializedVersion = lastLayoutFeature().serialize() + 1;
    IOException e = assertThrows(IOException.class, () -> createManager(serializedVersion));
    assertTrue(e.getMessage().contains("unknown apparent version " + serializedVersion), e.getMessage());
  }

  /**
   * The DB is ahead of the VERSION file, as an interrupted snapshot install leaves it. The component finalizes to the
   * DB version.
   */
  @Test
  public void testFinalizeFromDBFinalizesToSoftwareVersion() throws Exception {
    Table<String, String> finalizationStore = storeWithVersion(expectedSoftwareVersion());
    try (RatisBasedVersionManager versionManager = createManager(initialVersion().serialize())) {
      versionManager.finalizeFromDBIfRequired(finalizationStore);
      assertVersions(versionManager, finalizationStore, expectedSoftwareVersion(), expectedSoftwareVersion());
      assertFalse(versionManager.needsFinalization());
    }
  }

  /**
   * The DB version is between the apparent version and the software version. This should not happen in practice,
   * but if it does the safest recovery is to partially finalize up to the DB version to avoid mixed apparent versions
   * in the quorum.
   */
  @Test
  public void testFinalizeFromDBFinalizesToIntermediateDBVersion() throws Exception {
    Table<String, String> finalizationStore = storeWithVersion(intermediateVersion());
    try (RatisBasedVersionManager versionManager = createManager(initialVersion().serialize())) {
      versionManager.finalizeFromDBIfRequired(finalizationStore);
      assertVersions(versionManager, finalizationStore, intermediateVersion(), intermediateVersion());
      assertTrue(versionManager.needsFinalization());
    }
  }

  /**
   * The VERSION file is ahead of the DB. This can happen if finalization wrote the VERSION file (inside Ratis
   * applyTransaction) but the process crashed before the Ratis transaction buffer was flushed to RocksDB.
   * Ratis log replay is async and may not have finished when this method runs, so this is treated as an expected
   * case and no action is taken — log replay will correct the DB after startup.
   */
  @Test
  public void testFinalizeFromDBNoOpWhenVersionFileAheadOfDB() throws Exception {
    Table<String, String> finalizationStore = storeWithVersion(intermediateVersion());
    try (RatisBasedVersionManager versionManager = createManager(expectedSoftwareVersion().serialize())) {
      versionManager.finalizeFromDBIfRequired(finalizationStore);
      assertVersions(versionManager, finalizationStore, expectedSoftwareVersion(), intermediateVersion());
    }
  }

  /**
   * When the DB has no apparent version, the component was never finalized. finalizeFromDBIfRequired takes no action.
   */
  @Test
  public void testFinalizeFromDBNoOpWhenDBNull() throws Exception {
    Table<String, String> finalizationStore = new InMemoryTestTable<>();
    try (RatisBasedVersionManager versionManager = createManager(initialVersion().serialize())) {
      versionManager.finalizeFromDBIfRequired(finalizationStore);
      assertVersions(versionManager, finalizationStore, initialVersion(), null);
    }
  }

  /**
   * When the DB matches a pre-finalized apparent version, no finalization or update is needed.
   */
  @Test
  public void testFinalizeFromDBNoOpWhenDBEqualsApparent() throws Exception {
    Table<String, String> finalizationStore = storeWithVersion(intermediateVersion());
    try (RatisBasedVersionManager versionManager = createManager(intermediateVersion().serialize())) {
      versionManager.finalizeFromDBIfRequired(finalizationStore);
      assertVersions(versionManager, finalizationStore, intermediateVersion(), intermediateVersion());
      assertTrue(versionManager.needsFinalization());
    }
  }

  /**
   * The steady state of a finalized component: the DB and VERSION file both record the software version.
   */
  @Test
  public void testFinalizeFromDBNoOpWhenFullyFinalized() throws Exception {
    Table<String, String> finalizationStore = storeWithVersion(expectedSoftwareVersion());
    try (RatisBasedVersionManager versionManager = createManager(expectedSoftwareVersion().serialize())) {
      versionManager.finalizeFromDBIfRequired(finalizationStore);
      assertVersions(versionManager, finalizationStore, expectedSoftwareVersion(), expectedSoftwareVersion());
      assertFalse(versionManager.needsFinalization());
    }
  }

  /**
   * The DB version exceeds the software version. This cannot be supported and the process must crash without
   * changing its apparent version.
   */
  @Test
  public void testFinalizeFromDBThrowsWhenDBVersionExceedsSoftwareVersion() throws Exception {
    String serializedDBVersion = String.valueOf(expectedSoftwareVersion().serialize() + 1);
    Table<String, String> finalizationStore = new InMemoryTestTable<>();
    finalizationStore.put(APPARENT_VERSION_KEY, serializedDBVersion);
    try (RatisBasedVersionManager versionManager = createManager(initialVersion().serialize())) {
      assertThrows(IOException.class, () -> versionManager.finalizeFromDBIfRequired(finalizationStore));
      assertEquals(initialVersion(), versionManager.getApparentVersion());
      assertEquals(initialVersion().serialize(), versionManager.getPersistedApparentVersion());
      assertEquals(serializedDBVersion, finalizationStore.get(APPARENT_VERSION_KEY));
    }
  }

  private ComponentVersion initialVersion() {
    return allVersionsInOrder().get(0);
  }

  /**
   * @return A version after the initial version and before the software version.
   */
  private ComponentVersion intermediateVersion() {
    List<ComponentVersion> allVersions = allVersionsInOrder();
    assertTrue(allVersions.size() >= 3, "Need at least three versions to have an intermediate version");
    return allVersions.get(allVersions.size() - 2);
  }

  private static Table<String, String> storeWithVersion(ComponentVersion version) throws IOException {
    Table<String, String> finalizationStore = new InMemoryTestTable<>();
    finalizationStore.put(APPARENT_VERSION_KEY, String.valueOf(version.serialize()));
    return finalizationStore;
  }

  private static void assertVersions(RatisBasedVersionManager versionManager, Table<String, String> finalizationStore,
      ComponentVersion expectedApparentVersion, ComponentVersion expectedDBVersion) throws IOException {
    assertEquals(expectedApparentVersion, versionManager.getApparentVersion());
    assertEquals(expectedApparentVersion.serialize(), versionManager.getPersistedApparentVersion());
    if (expectedDBVersion == null) {
      assertNull(finalizationStore.get(APPARENT_VERSION_KEY));
    } else {
      assertEquals(String.valueOf(expectedDBVersion.serialize()), finalizationStore.get(APPARENT_VERSION_KEY));
    }
  }
}
