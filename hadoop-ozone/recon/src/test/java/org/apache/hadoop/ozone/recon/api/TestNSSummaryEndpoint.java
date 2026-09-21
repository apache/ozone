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

import static org.apache.hadoop.ozone.OzoneConsts.KB;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.handlers.BucketHandler;
import org.apache.hadoop.ozone.recon.api.handlers.EntityHandler;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for the NSSummary REST APIs, run against every bucket layout.
 *
 * <p>Each {@link NSSummaryTestScenario} builds its own namespace and owns the
 * expected values, so the tree (FSO, Legacy) and flat (OBS + Legacy) fixtures
 * share this single suite. Structure-specific tests are gated with
 * {@link org.junit.jupiter.api.Assumptions} on the scenario capability flags.
 */
@ParameterizedClass
@MethodSource("scenarios")
public class TestNSSummaryEndpoint extends NSSummaryEndpointTestBase {

  @TempDir
  private Path temporaryFolder;

  @Parameter
  private NSSummaryTestScenario scenario;

  private ReconOMMetadataManager reconOMMetadataManager;
  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private NSSummaryEndpoint nsSummaryEndpoint;

  static Stream<NSSummaryTestScenario> scenarios() {
    return Stream.of(
        new FsoNSSummaryScenario(),
        new LegacyNSSummaryScenario(),
        new FlatNSSummaryScenario());
  }

  @BeforeEach
  public void setUp() throws Exception {
    OzoneConfiguration conf = scenario.newConfiguration();
    OMMetadataManager omMetadataManager = scenario.initializeOmMetadataManager(
        Files.createDirectory(temporaryFolder.resolve("JunitOmDBDir")).toFile(),
        conf);
    OzoneManagerServiceProviderImpl ozoneManagerServiceProvider =
        scenario.mockOmServiceProvider();
    reconOMMetadataManager = getTestReconOmMetadataManager(omMetadataManager,
        Files.createDirectory(temporaryFolder.resolve("omMetadataDir"))
            .toFile());

    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder.toFile())
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(ozoneManagerServiceProvider)
            .withReconSqlDb()
            .withContainerDB()
            .addBinding(OzoneStorageContainerManager.class,
                getMockReconSCM(scenario.rootQuota(), scenario.rootDataSize()))
            .addBinding(StorageContainerServiceProvider.class,
                mock(StorageContainerServiceProviderImpl.class))
            .addBinding(NSSummaryEndpoint.class)
            .build();
    reconNamespaceSummaryManager =
        reconTestInjector.getInstance(ReconNamespaceSummaryManager.class);
    nsSummaryEndpoint = reconTestInjector.getInstance(NSSummaryEndpoint.class);

    scenario.populateAndReprocess(reconNamespaceSummaryManager,
        reconOMMetadataManager, conf);
  }

  // ---------------------------------------------------------------------------
  // Tests common to every layout
  // ---------------------------------------------------------------------------

  @Test
  public void testUtility() {
    String[] names = EntityHandler.parseRequestPath(
        NSSummaryTestScenario.TEST_PATH_UTILITY);
    assertArrayEquals(NSSummaryTestScenario.TEST_NAMES, names);
    assertEquals(NSSummaryTestScenario.TEST_KEY_NAMES,
        BucketHandler.getKeyName(names));
    assertEquals(NSSummaryTestScenario.TEST_PATH_UTILITY,
        BucketHandler.buildSubpath(NSSummaryTestScenario.PARENT_DIR,
            "file1.txt"));
  }

  @Test
  public void testGetBasicInfoRoot() throws Exception {
    scenario.assertBasicInfoRoot(nsSummaryEndpoint, reconOMMetadataManager);
  }

  @Test
  public void testGetBasicInfoVol() throws Exception {
    scenario.assertBasicInfoVolume(nsSummaryEndpoint);
  }

  @Test
  public void testGetBasicInfoBucketOne() throws Exception {
    scenario.assertBasicInfoBucketOne(nsSummaryEndpoint);
  }

  @Test
  public void testGetBasicInfoBucketTwo() throws Exception {
    scenario.assertBasicInfoBucketTwo(nsSummaryEndpoint);
  }

  @Test
  public void testGetBasicInfoNoPath() throws Exception {
    assertBasicInfoNoPath(nsSummaryEndpoint, NSSummaryTestScenario.INVALID_PATH);
  }

  @Test
  public void testGetBasicInfoKey() throws Exception {
    assertBasicInfoKey(nsSummaryEndpoint, NSSummaryTestScenario.KEY_PATH,
        2 * KB + 1);
  }

  @Test
  public void testDiskUsageRoot() throws Exception {
    scenario.assertDiskUsageRoot(nsSummaryEndpoint);
  }

  @Test
  public void testDiskUsageVolume() throws Exception {
    scenario.assertDiskUsageVolume(nsSummaryEndpoint);
  }

  @Test
  public void testDiskUsageKey() throws Exception {
    scenario.assertDiskUsageKey(nsSummaryEndpoint);
  }

  @Test
  public void testDiskUsageUnknown() throws Exception {
    Response invalidResponse = nsSummaryEndpoint.getDiskUsage(
        NSSummaryTestScenario.INVALID_PATH, false, false, false);
    DUResponse invalidObj = (DUResponse) invalidResponse.getEntity();
    assertEquals(ResponseStatus.PATH_NOT_FOUND, invalidObj.getStatus());
  }

  @Test
  public void testDiskUsageWithReplication() throws Exception {
    scenario.writeMultiBlockKey(reconOMMetadataManager);
    DUResponse replicaDUResponse = getDiskUsage(nsSummaryEndpoint,
        scenario.multiBlockKeyPath(), true);
    assertEquals(ResponseStatus.OK, replicaDUResponse.getStatus());
    assertEquals(scenario.multiBlockKeyReplicatedSize(),
        replicaDUResponse.getSizeWithReplica());
  }

  @Test
  public void testDataSizeUnderRootWithReplication() throws IOException {
    scenario.assertDataSizeUnderRootWithReplication(nsSummaryEndpoint);
  }

  @Test
  public void testDataSizeUnderVolWithReplication() throws IOException {
    scenario.assertDataSizeUnderVolWithReplication(nsSummaryEndpoint);
  }

  @Test
  public void testDataSizeUnderKeyWithReplication() throws IOException {
    scenario.assertDataSizeUnderKeyWithReplication(nsSummaryEndpoint);
  }

  @Test
  public void testQuotaUsage() throws Exception {
    scenario.assertQuotaUsage(nsSummaryEndpoint);
  }

  @Test
  public void testFileSizeDist() throws Exception {
    scenario.assertFileSizeDist(nsSummaryEndpoint);
  }

  @Test
  public void testConstructFullPath() throws IOException {
    scenario.verifyConstructFullPath(reconOMMetadataManager,
        reconNamespaceSummaryManager);
  }

  @Test
  public void testConstructFullPathWithNegativeParentIdTriggersRebuild()
      throws IOException {
    long dirOneObjectId = 1L;
    ReconNamespaceSummaryManager mockSummaryManager =
        mock(ReconNamespaceSummaryManager.class);
    NSSummary dir1Summary = new NSSummary();
    dir1Summary.setParentId(-1);
    when(mockSummaryManager.getNSSummary(dirOneObjectId))
        .thenReturn(dir1Summary);

    OmKeyInfo keyInfo = new OmKeyInfo.Builder()
        .setKeyName("file2")
        .setVolumeName("vol")
        .setBucketName("bucket1")
        .setObjectID(2L)
        .setParentObjectID(dirOneObjectId)
        .build();

    assertEquals("",
        ReconUtils.constructFullPath(keyInfo, mockSummaryManager),
        "Should return empty string when NSSummary has negative parentId");
  }

  // ---------------------------------------------------------------------------
  // Tree-only tests (FSO, Legacy)
  // ---------------------------------------------------------------------------

  @Test
  public void testGetBasicInfoDir() throws Exception {
    assumeTrue(scenario.hasDirectories());
    treeScenario().assertBasicInfoDir(nsSummaryEndpoint);
  }

  @Test
  public void testDiskUsageBucket() throws Exception {
    assumeTrue(scenario.hasDirectories());
    treeScenario().assertDiskUsageBucket(nsSummaryEndpoint);
  }

  @Test
  public void testDiskUsageDir() throws Exception {
    assumeTrue(scenario.hasDirectories());
    treeScenario().assertDiskUsageDir(nsSummaryEndpoint);
  }

  @Test
  public void testDataSizeUnderBucketWithReplication() throws IOException {
    assumeTrue(scenario.hasDirectories());
    treeScenario().assertDataSizeUnderBucketWithReplication(nsSummaryEndpoint);
  }

  @Test
  public void testDataSizeUnderDirWithReplication() throws IOException {
    assumeTrue(scenario.hasDirectories());
    treeScenario().assertDataSizeUnderDirWithReplication(nsSummaryEndpoint);
  }

  @Test
  public void testReplicatedSizePropagationUpwards() throws IOException {
    assumeTrue(scenario.hasDirectories());
    treeScenario().assertReplicatedSizePropagation(nsSummaryEndpoint);
  }

  @Test
  public void testDataSizeUnderVolumeWithRatisReplication() throws IOException {
    assumeTrue(scenario.hasVolumeThree());
    treeScenario().assertRatisReplicationUnderVolumeThree(nsSummaryEndpoint);
  }

  @Test
  public void testDataSizeUnderBucketWithRatisReplication() throws IOException {
    assumeTrue(scenario.hasVolumeThree());
    treeScenario().assertRatisReplicationUnderBucketFive(nsSummaryEndpoint);
  }

  @Test
  public void testDataSizeUnderDirWithRatisReplication() throws IOException {
    assumeTrue(scenario.hasVolumeThree());
    treeScenario().assertRatisReplicationUnderDirSix(nsSummaryEndpoint);
  }

  // ---------------------------------------------------------------------------
  // Flat-only tests (OBS + Legacy)
  // ---------------------------------------------------------------------------

  @Test
  public void testGetBasicInfoVolTwo() throws Exception {
    assumeTrue(scenario.isFlatLayout());
    flatScenario().assertBasicInfoVolTwo(nsSummaryEndpoint);
  }

  @Test
  public void testGetBasicInfoBucketThree() throws Exception {
    assumeTrue(scenario.isFlatLayout());
    flatScenario().assertBasicInfoBucketThree(nsSummaryEndpoint);
  }

  @Test
  public void testGetBasicInfoBucketFour() throws Exception {
    assumeTrue(scenario.isFlatLayout());
    flatScenario().assertBasicInfoBucketFour(nsSummaryEndpoint);
  }

  @Test
  public void testDiskUsageVolTwo() throws Exception {
    assumeTrue(scenario.isFlatLayout());
    flatScenario().assertDiskUsageVolTwo(nsSummaryEndpoint);
  }

  @Test
  public void testDiskUsageBucketOne() throws Exception {
    assumeTrue(scenario.isFlatLayout());
    flatScenario().assertDiskUsageBucketOne(nsSummaryEndpoint);
  }

  @Test
  public void testDiskUsageBucketTwo() throws Exception {
    assumeTrue(scenario.isFlatLayout());
    flatScenario().assertDiskUsageBucketTwo(nsSummaryEndpoint);
  }

  @Test
  public void testDiskUsageBucketThree() throws Exception {
    assumeTrue(scenario.isFlatLayout());
    flatScenario().assertDiskUsageBucketThree(nsSummaryEndpoint);
  }

  @Test
  public void testDiskUsageKeys() throws Exception {
    assumeTrue(scenario.isFlatLayout());
    flatScenario().assertDiskUsageKeys(nsSummaryEndpoint);
  }

  @Test
  public void testDataSizeUnderBucketOneWithReplication() throws IOException {
    assumeTrue(scenario.isFlatLayout());
    flatScenario().assertDataSizeUnderBucketOneWithReplication(
        nsSummaryEndpoint);
  }

  @Test
  public void testDataSizeUnderBucketThreeWithReplication() throws IOException {
    assumeTrue(scenario.isFlatLayout());
    flatScenario().assertDataSizeUnderBucketThreeWithReplication(
        nsSummaryEndpoint);
  }

  @Test
  public void testNormalizePathUptoBucket() {
    assumeTrue(scenario.isFlatLayout());
    flatScenario().assertNormalizePathUptoBucket();
  }

  private AbstractTreeNSSummaryScenario treeScenario() {
    return (AbstractTreeNSSummaryScenario) scenario;
  }

  private FlatNSSummaryScenario flatScenario() {
    return (FlatNSSummaryScenario) scenario;
  }
}
