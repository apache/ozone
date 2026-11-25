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

package org.apache.hadoop.ozone.recon.api;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;

/**
 * A single parameterization of {@link TestNSSummaryEndpoint}: it owns the
 * layout-specific fixture (config, OM provider mock, namespace population and
 * reprocess) and the expected-value assertions for that layout.
 *
 * <p>Two concrete shapes exist: a tree fixture (FSO and Legacy with
 * filesystem-paths enabled, see {@link AbstractTreeNSSummaryScenario}) and a
 * flat fixture (OBS + Legacy with filesystem-paths disabled, see
 * {@link FlatNSSummaryScenario}). Behaviour that only one shape supports is
 * gated in the suite via the capability flags below and reached through
 * {@code (AbstractTreeNSSummaryScenario) scenario} /
 * {@code (FlatNSSummaryScenario) scenario} casts.
 */
public abstract class NSSummaryTestScenario {

  static final String TEST_USER = "TestUser";

  // request paths shared by every layout
  static final String ROOT_PATH = "/";
  static final String INVALID_PATH = "/vol/path/not/found";
  static final String KEY_PATH = "/vol/bucket2/file4";

  // path-parsing fixture, identical across layouts
  static final String TEST_PATH_UTILITY = "/vol1/buck1/a/b/c/d/e/file1.txt";
  static final String PARENT_DIR = "vol1/buck1/a/b/c/d/e";
  static final String[] TEST_NAMES =
      new String[]{"vol1", "buck1", "a", "b", "c", "d", "e", "file1.txt"};
  static final String TEST_KEY_NAMES = "a/b/c/d/e/file1.txt";

  private final String displayName;

  protected NSSummaryTestScenario(String displayName) {
    this.displayName = displayName;
  }

  // ---------------------------------------------------------------------------
  // Fixture construction hooks
  // ---------------------------------------------------------------------------

  abstract OzoneConfiguration newConfiguration();

  abstract OzoneManagerServiceProviderImpl mockOmServiceProvider()
      throws IOException;

  abstract OMMetadataManager initializeOmMetadataManager(File omDbDir,
      OzoneConfiguration conf) throws IOException;

  /**
   * Populate the Recon OM DB with the full namespace (directories and
   * genuinely-replicated keys) and reprocess it into the NSSummary RocksDB.
   */
  abstract void populateAndReprocess(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager reconOMMetadataManager,
      OzoneConfiguration conf) throws Exception;

  /**
   * Write the extra single multi-block key exercised by
   * {@code testDiskUsageWithReplication}.
   */
  abstract void writeMultiBlockKey(
      ReconOMMetadataManager reconOMMetadataManager) throws IOException;

  abstract void verifyConstructFullPath(
      ReconOMMetadataManager reconOMMetadataManager,
      ReconNamespaceSummaryManager reconNamespaceSummaryManager)
      throws IOException;

  // Values needed to build the mock SCM before the fixture exists.
  abstract long rootQuota();

  abstract long rootDataSize();

  // ---------------------------------------------------------------------------
  // Capability flags used by the suite to gate structure-specific tests
  // ---------------------------------------------------------------------------

  boolean hasDirectories() {
    return false;
  }

  boolean hasVolumeThree() {
    return false;
  }

  boolean isFlatLayout() {
    return false;
  }

  // ---------------------------------------------------------------------------
  // Assertions common to every layout (expectations differ per scenario)
  // ---------------------------------------------------------------------------

  abstract void assertBasicInfoRoot(NSSummaryEndpoint endpoint,
      ReconOMMetadataManager reconOMMetadataManager) throws Exception;

  abstract void assertBasicInfoVolume(NSSummaryEndpoint endpoint)
      throws Exception;

  abstract void assertBasicInfoBucketOne(NSSummaryEndpoint endpoint)
      throws Exception;

  abstract void assertBasicInfoBucketTwo(NSSummaryEndpoint endpoint)
      throws Exception;

  abstract void assertDiskUsageRoot(NSSummaryEndpoint endpoint)
      throws Exception;

  abstract void assertDiskUsageVolume(NSSummaryEndpoint endpoint)
      throws Exception;

  abstract void assertDiskUsageKey(NSSummaryEndpoint endpoint) throws Exception;

  abstract void assertQuotaUsage(NSSummaryEndpoint endpoint) throws Exception;

  abstract void assertFileSizeDist(NSSummaryEndpoint endpoint) throws Exception;

  /** Expected replicated size of the {@link #writeMultiBlockKey} key. */
  abstract long multiBlockKeyReplicatedSize();

  abstract String multiBlockKeyPath();

  abstract void assertDataSizeUnderRootWithReplication(
      NSSummaryEndpoint endpoint) throws IOException;

  abstract void assertDataSizeUnderVolWithReplication(
      NSSummaryEndpoint endpoint) throws IOException;

  abstract void assertDataSizeUnderKeyWithReplication(
      NSSummaryEndpoint endpoint) throws IOException;

  @Override
  public String toString() {
    return displayName;
  }
}
