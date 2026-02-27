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

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.TEST_USER;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getBucketLayout;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getOmKeyLocationInfo;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDataToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDirToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeKeyToOm;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.KeyInsightInfoResponse;
import org.apache.hadoop.ozone.recon.api.types.ListKeysResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.api.types.ReconBasicOmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconPipelineManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconGlobalStatsManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.ContainerKeyMapperTaskOBS;
import org.apache.hadoop.ozone.recon.tasks.GlobalStatsValue;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTaskWithFSO;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTaskWithLegacy;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTaskWithOBS;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit test for OmDBInsightEndPoint.
 */
public class TestOmDBInsightEndPoint extends AbstractReconSqlDBTest {
  @TempDir
  private Path temporaryFolder;
  private ReconContainerMetadataManager reconContainerMetadataManager;
  private ReconOMMetadataManager reconOMMetadataManager;
  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private OMDBInsightEndpoint omdbInsightEndpoint;
  private Pipeline pipeline;
  private Random random = new Random();
  private OzoneConfiguration ozoneConfiguration;
  private Set<Long> generatedIds = new HashSet<>();
  private static final String VOLUME_ONE = "volume1";
  private static final String OBS_BUCKET = "obs-bucket";
  private static final String FSO_BUCKET = "fso-bucket";
  private static final String EMPTY_OBS_BUCKET = "empty-obs-bucket";
  private static final String EMPTY_FSO_BUCKET = "empty-fso-bucket";
  private static final String LEGACY_BUCKET = "legacy-bucket";
  private static final String FSO_BUCKET_TWO = "fso-bucket2";

  private static final String DIR_ONE = "dir1";
  private static final String DIR_TWO = "dir2";
  private static final String DIR_THREE = "dir3";

  private static final String DIR_FOUR = "dir4";
  private static final String DIR_FIVE = "dir5";
  private static final String DIR_SIX = "dir6";
  private static final String DIR_SEVEN = "dir7";

  private static final String DIR_EIGHT = "dir8";
  private static final String DIR_NINE = "dir9";
  private static final String DIR_TEN = "dir10";

  private static final String TEST_FILE = "testfile";
  private static final String FILE_ONE = "file1";

  private static final String KEY_ONE = "key1";
  private static final String KEY_TWO = "key1/key2";
  private static final String KEY_THREE = "key1/key2/key3";
  private static final String KEY_FOUR = "key4";
  private static final String KEY_FIVE = "key5";
  private static final String KEY_SIX = "key6";
  private static final String NON_EXISTENT_KEY_SEVEN = "key7";

  private static final String KEY_EIGHT = "key8";
  private static final String KEY_NINE = "dir4/dir5/key9";
  private static final String KEY_TEN = "dir4/dir6/key10";
  private static final String KEY_ELEVEN = "key11";
  private static final String KEY_TWELVE = "key12";
  private static final String KEY_THIRTEEN = "dir4/dir7/key13";

  private static final String FILE_EIGHT = "key8";
  private static final String FILE_NINE = "key9";
  private static final String FILE_TEN = "key10";
  private static final String FILE_ELEVEN = "key11";
  private static final String FILE_TWELVE = "key12";
  private static final String FILE_THIRTEEN = "key13";

  private static final long PARENT_OBJECT_ID_ZERO = 0L;
  private static final long VOLUME_ONE_OBJECT_ID = 1L;

  private static final long OBS_BUCKET_OBJECT_ID = 2L;
  private static final long KEY_ONE_OBJECT_ID = 3L;
  private static final long KEY_TWO_OBJECT_ID = 4L;
  private static final long KEY_THREE_OBJECT_ID = 5L;
  private static final long KEY_FOUR_OBJECT_ID = 6L;
  private static final long KEY_FIVE_OBJECT_ID = 7L;
  private static final long KEY_SIX_OBJECT_ID = 8L;

  private static final long FSO_BUCKET_OBJECT_ID = 10L;
  private static final long DIR_ONE_OBJECT_ID = 11L;
  private static final long DIR_TWO_OBJECT_ID = 12L;
  private static final long DIR_THREE_OBJECT_ID = 13L;
  private static final long KEY_SEVEN_OBJECT_ID = 14L;
  private static final long KEY_EIGHT_OBJECT_ID = 15L;
  private static final long KEY_NINE_OBJECT_ID = 16L;
  private static final long KEY_TEN_OBJECT_ID = 17L;
  private static final long KEY_ELEVEN_OBJECT_ID = 18L;
  private static final long KEY_TWELVE_OBJECT_ID = 19L;

  private static final long LEGACY_BUCKET_OBJECT_ID = 20L;
  private static final long DIR_FOUR_OBJECT_ID = 21L;
  private static final long DIR_FIVE_OBJECT_ID = 22L;
  private static final long DIR_SIX_OBJECT_ID = 23L;
  private static final long KEY_THIRTEEN_OBJECT_ID = 24L;
  private static final long KEY_FOURTEEN_OBJECT_ID = 25L;
  private static final long KEY_FIFTEEN_OBJECT_ID = 26L;
  private static final long KEY_SIXTEEN_OBJECT_ID = 27L;
  private static final long KEY_SEVENTEEN_OBJECT_ID = 28L;
  private static final long KEY_EIGHTEEN_OBJECT_ID = 29L;

  private static final long FSO_BUCKET_TWO_OBJECT_ID = 30L;
  private static final long DIR_EIGHT_OBJECT_ID = 31L;
  private static final long DIR_NINE_OBJECT_ID = 32L;
  private static final long DIR_TEN_OBJECT_ID = 33L;
  private static final long KEY_NINETEEN_OBJECT_ID = 34L;
  private static final long KEY_TWENTY_OBJECT_ID = 35L;
  private static final long KEY_TWENTY_ONE_OBJECT_ID = 36L;
  private static final long KEY_TWENTY_TWO_OBJECT_ID = 37L;
  private static final long KEY_TWENTY_THREE_OBJECT_ID = 38L;
  private static final long KEY_TWENTY_FOUR_OBJECT_ID = 39L;

  private static final long EMPTY_OBS_BUCKET_OBJECT_ID = 40L;
  private static final long EMPTY_FSO_BUCKET_OBJECT_ID = 41L;

  private static final long KEY_ONE_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long KEY_TWO_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long KEY_THREE_SIZE = OzoneConsts.KB + 1; // bin 1
  private static final long KEY_FOUR_SIZE = OzoneConsts.KB + 1; // bin 1
  private static final long KEY_FIVE_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long KEY_SIX_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long KEY_SEVEN_SIZE = OzoneConsts.KB + 1; // bin 1
  private static final long KEY_EIGHT_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long KEY_NINE_SIZE = OzoneConsts.KB + 1; // bin 1
  private static final long KEY_TEN_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long KEY_ELEVEN_SIZE = OzoneConsts.KB + 1; // bin 1
  private static final long KEY_TWELVE_SIZE = 2 * OzoneConsts.KB + 1; // bin 2

  private static final long KEY_THIRTEEN_SIZE = 2 * OzoneConsts.KB + 1; // bin 2

  private static final long KEY_FOURTEEN_SIZE = OzoneConsts.KB + 1; // bin 1
  private static final long KEY_FIFTEEN_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long KEY_SIXTEEN_SIZE = OzoneConsts.KB + 1; // bin 1
  private static final long KEY_SEVENTEEN_SIZE = 2 * OzoneConsts.KB + 1; // bin 2
  private static final long KEY_EIGHTEEN_SIZE = OzoneConsts.KB + 1; // bin 1
  private static final long KEY_NINETEEN_SIZE = 2 * OzoneConsts.KB + 1; // bin 2

  private static final String OBS_BUCKET_PATH = "/volume1/obs-bucket";
  private static final String FSO_BUCKET_PATH = "/volume1/fso-bucket";
  private static final String LEGACY_BUCKET_PATH = "/volume1/legacy-bucket";
  private static final String EMPTY_OBS_BUCKET_PATH = "/volume1/empty-obs-bucket";
  private static final String EMPTY_FSO_BUCKET_PATH = "/volume1/empty-fso-bucket";
  private static final String FSO_BUCKET_TWO_PATH = "/volume1/fso-bucket2";

  private static final String DIR_ONE_PATH = "/volume1/fso-bucket/dir1";
  private static final String DIR_TWO_PATH = "/volume1/fso-bucket/dir1/dir2";
  private static final String DIR_THREE_PATH = "/volume1/fso-bucket/dir1/dir2/dir3";
  private static final String NON_EXISTENT_DIR_FOUR_PATH = "/volume1/fso-bucket/dir1/dir2/dir3/dir4";

  private static final long VOLUME_ONE_QUOTA = 2 * OzoneConsts.MB;
  private static final long OBS_BUCKET_QUOTA = OzoneConsts.MB;
  private static final long FSO_BUCKET_QUOTA = OzoneConsts.MB;

  private ReplicationConfig ratisOne = ReplicationConfig.fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
      HddsProtos.ReplicationFactor.ONE);
  private long epochMillis1 =
      ReconUtils.convertToEpochMillis("04-04-2024 12:30:00", "MM-dd-yyyy HH:mm:ss", TimeZone.getDefault());
  private long epochMillis2 =
      ReconUtils.convertToEpochMillis("04-05-2024 12:30:00", "MM-dd-yyyy HH:mm:ss", TimeZone.getDefault());

  public TestOmDBInsightEndPoint() {
    super();
  }

  public static Collection<Object[]> replicationConfigValues() {
    return Arrays.asList(new Object[][]{
        {ReplicationConfig.fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE)},
        {ReplicationConfig.fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS, HddsProtos.ReplicationFactor.ONE)},
        {ReplicationConfig.fromProto(HddsProtos.ReplicationType.EC, null,
            toProto(3, 2, ECReplicationConfig.EcCodec.RS, 1024))},
        {ReplicationConfig.fromProto(HddsProtos.ReplicationType.EC, null,
            toProto(6, 3, ECReplicationConfig.EcCodec.RS, 1024))},
        {ReplicationConfig.fromProto(HddsProtos.ReplicationType.EC, null,
            toProto(10, 4, ECReplicationConfig.EcCodec.XOR, 4096))}
    });
  }

  public static HddsProtos.ECReplicationConfig toProto(int data, int parity, ECReplicationConfig.EcCodec codec,
                                                       int ecChunkSize) {
    return HddsProtos.ECReplicationConfig.newBuilder()
        .setData(data)
        .setParity(parity)
        .setCodec(codec.toString())
        .setEcChunkSize(ecChunkSize)
        .build();
  }

  private long generateUniqueRandomLong() {
    long newValue;
    do {
      newValue = random.nextLong();
    } while (generatedIds.contains(newValue));

    generatedIds.add(newValue);
    return newValue;
  }

  @BeforeEach
  public void setUp() throws Exception {
    OMMetadataManager omMetadataManager = initializeNewOmMetadataManager(
        Files.createDirectory(temporaryFolder.resolve(
            "JunitOmMetadata")).toFile());
    reconOMMetadataManager = getTestReconOmMetadataManager(omMetadataManager,
        Files.createDirectory(temporaryFolder.resolve(
            "JunitOmMetadataTest")).toFile());
    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder.toFile())
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(mock(OzoneManagerServiceProviderImpl.class))
            // No longer using mock reconSCM as we need nodeDB in Facade
            //  to establish datanode UUID to hostname mapping
            .addBinding(OzoneStorageContainerManager.class,
                ReconStorageContainerManagerFacade.class)
            .withContainerDB()
            .addBinding(StorageContainerServiceProvider.class,
                mock(StorageContainerServiceProviderImpl.class))
            .addBinding(OMDBInsightEndpoint.class)
            .addBinding(ContainerHealthSchemaManager.class)
            .build();
    reconContainerMetadataManager =
        reconTestInjector.getInstance(ReconContainerMetadataManager.class);
    omdbInsightEndpoint = reconTestInjector.getInstance(
        OMDBInsightEndpoint.class);
    OzoneStorageContainerManager ozoneStorageContainerManager =
        reconTestInjector.getInstance(OzoneStorageContainerManager.class);
    ReconPipelineManager reconPipelineManager = (ReconPipelineManager)
                                                    ozoneStorageContainerManager.getPipelineManager();
    pipeline = getRandomPipeline();
    reconPipelineManager.addPipeline(pipeline);
    ozoneConfiguration = new OzoneConfiguration();
    reconNamespaceSummaryManager =
        reconTestInjector.getInstance(ReconNamespaceSummaryManager.class);
    setUpOmData();
    NSSummaryTaskWithLegacy nSSummaryTaskWithLegacy = new NSSummaryTaskWithLegacy(
        reconNamespaceSummaryManager,
        reconOMMetadataManager, ozoneConfiguration, 10);
    NSSummaryTaskWithOBS nsSummaryTaskWithOBS = new NSSummaryTaskWithOBS(
        reconNamespaceSummaryManager, reconOMMetadataManager, 10, 5, 20, 2000);
    NSSummaryTaskWithFSO nsSummaryTaskWithFSO = new NSSummaryTaskWithFSO(
        reconNamespaceSummaryManager, reconOMMetadataManager, 10, 5, 20, 2000);
    reconNamespaceSummaryManager.clearNSSummaryTable();
    nSSummaryTaskWithLegacy.reprocessWithLegacy(reconOMMetadataManager);
    nsSummaryTaskWithOBS.reprocessWithOBS(reconOMMetadataManager);
    nsSummaryTaskWithFSO.reprocessWithFSO(reconOMMetadataManager);
  }

  /**
   * Releases resources (network sockets, database files) after each test run.
   * This is critical to prevent resource leaks between tests, which would otherwise cause "Too many open files" errors.
   */
  @AfterEach
  public void tearDown() throws Exception {
    if (reconOMMetadataManager != null) {
      reconOMMetadataManager.stop();
    }
  }

  @SuppressWarnings("methodlength")
  private void setUpOmData() throws Exception {
    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
    BlockID blockID1 = new BlockID(1, 101);
    OmKeyLocationInfo omKeyLocationInfo1 = getOmKeyLocationInfo(blockID1,
        pipeline);
    omKeyLocationInfoList.add(omKeyLocationInfo1);

    BlockID blockID2 = new BlockID(2, 102);
    OmKeyLocationInfo omKeyLocationInfo2 = getOmKeyLocationInfo(blockID2,
        pipeline);
    omKeyLocationInfoList.add(omKeyLocationInfo2);

    OmKeyLocationInfoGroup omKeyLocationInfoGroup = new
        OmKeyLocationInfoGroup(0, omKeyLocationInfoList);

    //key = key_one, Blocks = [ {CID = 1, LID = 101}, {CID = 2, LID = 102} ]
    writeDataToOm(reconOMMetadataManager,
        "key_one", "bucketOne", "sampleVol",
        Collections.singletonList(omKeyLocationInfoGroup));

    List<OmKeyLocationInfoGroup> infoGroups = new ArrayList<>();
    BlockID blockID3 = new BlockID(1, 103);
    OmKeyLocationInfo omKeyLocationInfo3 = getOmKeyLocationInfo(blockID3,
        pipeline);

    List<OmKeyLocationInfo> omKeyLocationInfoListNew = new ArrayList<>();
    omKeyLocationInfoListNew.add(omKeyLocationInfo3);
    infoGroups.add(new OmKeyLocationInfoGroup(0,
        omKeyLocationInfoListNew));

    BlockID blockID4 = new BlockID(2, 104);
    OmKeyLocationInfo omKeyLocationInfo4 = getOmKeyLocationInfo(blockID4,
        pipeline);

    omKeyLocationInfoListNew = new ArrayList<>();
    omKeyLocationInfoListNew.add(omKeyLocationInfo4);
    infoGroups.add(new OmKeyLocationInfoGroup(1,
        omKeyLocationInfoListNew));

    //key = key_two, Blocks = [ {CID = 1, LID = 103}, {CID = 1, LID = 104} ]
    writeDataToOm(reconOMMetadataManager,
        "key_two", "bucketOne", "sampleVol", infoGroups);

    List<OmKeyLocationInfo> omKeyLocationInfoList2 = new ArrayList<>();
    BlockID blockID5 = new BlockID(3, 105);
    OmKeyLocationInfo omKeyLocationInfo5 = getOmKeyLocationInfo(blockID5,
        pipeline);
    omKeyLocationInfoList2.add(omKeyLocationInfo5);

    BlockID blockID6 = new BlockID(3, 106);
    OmKeyLocationInfo omKeyLocationInfo6 = getOmKeyLocationInfo(blockID6,
        pipeline);
    omKeyLocationInfoList2.add(omKeyLocationInfo6);

    OmKeyLocationInfoGroup omKeyLocationInfoGroup2 = new
        OmKeyLocationInfoGroup(0, omKeyLocationInfoList2);

    //key = key_three, Blocks = [ {CID = 2, LID = 2}, {CID = 2, LID = 3} ]
    writeDataToOm(reconOMMetadataManager,
        "key_three", "bucketOne", "sampleVol",
        Collections.singletonList(omKeyLocationInfoGroup2));

    //Generate Recon container DB data.
    OMMetadataManager omMetadataManagerMock = mock(OMMetadataManager.class);
    Table tableMock = mock(Table.class);
    when(tableMock.getName()).thenReturn("KeyTable");
    when(omMetadataManagerMock.getKeyTable(getBucketLayout()))
        .thenReturn(tableMock);
    ContainerKeyMapperTaskOBS containerKeyMapperTask =
        new ContainerKeyMapperTaskOBS(reconContainerMetadataManager,
            ozoneConfiguration);
    containerKeyMapperTask.reprocess(reconOMMetadataManager);

    String volumeOneKey = reconOMMetadataManager.getVolumeKey(VOLUME_ONE);
    OmVolumeArgs volumeOneArgs =
        OmVolumeArgs.newBuilder()
            .setObjectID(VOLUME_ONE_OBJECT_ID)
            .setVolume(VOLUME_ONE)
            .setAdminName(TEST_USER)
            .setOwnerName(TEST_USER)
            .setQuotaInBytes(VOLUME_ONE_QUOTA)
            .build();

    reconOMMetadataManager.getVolumeTable().put(volumeOneKey, volumeOneArgs);

    OmBucketInfo obsBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(VOLUME_ONE)
        .setBucketName(OBS_BUCKET)
        .setObjectID(OBS_BUCKET_OBJECT_ID)
        .setQuotaInBytes(OBS_BUCKET_QUOTA)
        .setBucketLayout(BucketLayout.OBJECT_STORE)
        .build();
    String obsBucketKey = reconOMMetadataManager.getBucketKey(
        obsBucketInfo.getVolumeName(), obsBucketInfo.getBucketName());

    reconOMMetadataManager.getBucketTable().put(obsBucketKey, obsBucketInfo);

    OmBucketInfo fsoBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(VOLUME_ONE)
        .setBucketName(FSO_BUCKET)
        .setObjectID(FSO_BUCKET_OBJECT_ID)
        .setQuotaInBytes(FSO_BUCKET_QUOTA)
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .build();
    String fsoBucketKey = reconOMMetadataManager.getBucketKey(
        fsoBucketInfo.getVolumeName(), fsoBucketInfo.getBucketName());

    reconOMMetadataManager.getBucketTable().put(fsoBucketKey, fsoBucketInfo);

    OmBucketInfo emptyOBSBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(VOLUME_ONE)
        .setBucketName(EMPTY_OBS_BUCKET)
        .setObjectID(EMPTY_OBS_BUCKET_OBJECT_ID)
        .setQuotaInBytes(OzoneConsts.MB)
        .setBucketLayout(BucketLayout.OBJECT_STORE)
        .build();
    String emptyOBSBucketKey = reconOMMetadataManager.getBucketKey(
        emptyOBSBucketInfo.getVolumeName(), emptyOBSBucketInfo.getBucketName());

    reconOMMetadataManager.getBucketTable().put(emptyOBSBucketKey, emptyOBSBucketInfo);

    OmBucketInfo emptyFSOBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(VOLUME_ONE)
        .setBucketName(EMPTY_FSO_BUCKET)
        .setObjectID(EMPTY_FSO_BUCKET_OBJECT_ID)
        .setQuotaInBytes(OzoneConsts.MB)
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .build();
    String emptyFSOBucketKey = reconOMMetadataManager.getBucketKey(
        emptyFSOBucketInfo.getVolumeName(), emptyFSOBucketInfo.getBucketName());

    reconOMMetadataManager.getBucketTable().put(emptyFSOBucketKey, emptyFSOBucketInfo);

    OmBucketInfo legacyBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(VOLUME_ONE)
        .setBucketName(LEGACY_BUCKET)
        .setObjectID(LEGACY_BUCKET_OBJECT_ID)
        .setQuotaInBytes(OzoneConsts.MB)
        .setBucketLayout(BucketLayout.LEGACY)
        .build();
    String legacyBucketKey = reconOMMetadataManager.getBucketKey(
        legacyBucketInfo.getVolumeName(), legacyBucketInfo.getBucketName());

    reconOMMetadataManager.getBucketTable().put(legacyBucketKey, legacyBucketInfo);

    OmBucketInfo fsoBucketTwoInfo = OmBucketInfo.newBuilder()
        .setVolumeName(VOLUME_ONE)
        .setBucketName(FSO_BUCKET_TWO)
        .setObjectID(FSO_BUCKET_TWO_OBJECT_ID)
        .setQuotaInBytes(OzoneConsts.MB)
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .build();
    String fsoBucketTwoKey = reconOMMetadataManager.getBucketKey(
        fsoBucketTwoInfo.getVolumeName(), fsoBucketTwoInfo.getBucketName());

    reconOMMetadataManager.getBucketTable().put(fsoBucketTwoKey, fsoBucketTwoInfo);

    // Write FSO keys data - Start
    writeDirToOm(reconOMMetadataManager, DIR_ONE_OBJECT_ID,
        FSO_BUCKET_OBJECT_ID, FSO_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID, DIR_ONE);
    writeDirToOm(reconOMMetadataManager, DIR_TWO_OBJECT_ID,
        DIR_ONE_OBJECT_ID, FSO_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID, DIR_TWO);
    writeDirToOm(reconOMMetadataManager, DIR_THREE_OBJECT_ID,
        DIR_TWO_OBJECT_ID, FSO_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID, DIR_THREE);

    writeKeyToOm(reconOMMetadataManager,
        TEST_FILE,
        FSO_BUCKET,
        VOLUME_ONE,
        TEST_FILE,
        KEY_SEVEN_OBJECT_ID,
        DIR_ONE_OBJECT_ID,
        FSO_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_SEVEN_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        ratisOne,
        epochMillis1, true);

    writeKeyToOm(reconOMMetadataManager,
        FILE_ONE,
        FSO_BUCKET,
        VOLUME_ONE,
        FILE_ONE,
        KEY_EIGHT_OBJECT_ID,
        DIR_ONE_OBJECT_ID,
        FSO_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_EIGHT_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        ratisOne,
        epochMillis2, true);

    writeKeyToOm(reconOMMetadataManager,
        TEST_FILE,
        FSO_BUCKET,
        VOLUME_ONE,
        TEST_FILE,
        KEY_NINE_OBJECT_ID,
        DIR_TWO_OBJECT_ID,
        FSO_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_NINE_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        ratisOne,
        epochMillis1, true);
    writeKeyToOm(reconOMMetadataManager,
        FILE_ONE,
        FSO_BUCKET,
        VOLUME_ONE,
        FILE_ONE,
        KEY_TEN_OBJECT_ID,
        DIR_TWO_OBJECT_ID,
        FSO_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_TEN_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        ratisOne,
        epochMillis2, true);

    writeKeyToOm(reconOMMetadataManager,
        TEST_FILE,
        FSO_BUCKET,
        VOLUME_ONE,
        TEST_FILE,
        KEY_ELEVEN_OBJECT_ID,
        DIR_THREE_OBJECT_ID,
        FSO_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_ELEVEN_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        ratisOne,
        epochMillis1, true);
    writeKeyToOm(reconOMMetadataManager,
        FILE_ONE,
        FSO_BUCKET,
        VOLUME_ONE,
        FILE_ONE,
        KEY_TWELVE_OBJECT_ID,
        DIR_THREE_OBJECT_ID,
        FSO_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_TWELVE_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        ratisOne,
        epochMillis2, true);
    // Write FSO Keys data - End

    // Write FSO bucket two keys - Start
    writeDirToOm(reconOMMetadataManager, DIR_EIGHT_OBJECT_ID,
        FSO_BUCKET_TWO_OBJECT_ID, FSO_BUCKET_TWO_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID, DIR_EIGHT);
    writeDirToOm(reconOMMetadataManager, DIR_NINE_OBJECT_ID,
        FSO_BUCKET_TWO_OBJECT_ID, FSO_BUCKET_TWO_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID, DIR_NINE);
    writeDirToOm(reconOMMetadataManager, DIR_TEN_OBJECT_ID,
        FSO_BUCKET_TWO_OBJECT_ID, FSO_BUCKET_TWO_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID, DIR_TEN);

    writeKeyToOm(reconOMMetadataManager,
        TEST_FILE,
        FSO_BUCKET_TWO,
        VOLUME_ONE,
        TEST_FILE,
        KEY_NINETEEN_OBJECT_ID,
        DIR_EIGHT_OBJECT_ID,
        FSO_BUCKET_TWO_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_FOURTEEN_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        ratisOne,
        epochMillis1, true);
    writeKeyToOm(reconOMMetadataManager,
        FILE_ONE,
        FSO_BUCKET_TWO,
        VOLUME_ONE,
        FILE_ONE,
        KEY_TWENTY_OBJECT_ID,
        DIR_EIGHT_OBJECT_ID,
        FSO_BUCKET_TWO_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_FIFTEEN_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        ratisOne,
        epochMillis1, true);

    writeKeyToOm(reconOMMetadataManager,
        TEST_FILE,
        FSO_BUCKET_TWO,
        VOLUME_ONE,
        TEST_FILE,
        KEY_TWENTY_ONE_OBJECT_ID,
        DIR_NINE_OBJECT_ID,
        FSO_BUCKET_TWO_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_SIXTEEN_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        ratisOne,
        epochMillis1, true);
    writeKeyToOm(reconOMMetadataManager,
        FILE_ONE,
        FSO_BUCKET_TWO,
        VOLUME_ONE,
        FILE_ONE,
        KEY_TWENTY_TWO_OBJECT_ID,
        DIR_NINE_OBJECT_ID,
        FSO_BUCKET_TWO_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_SEVENTEEN_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        ratisOne,
        epochMillis1, true);

    writeKeyToOm(reconOMMetadataManager,
        TEST_FILE,
        FSO_BUCKET_TWO,
        VOLUME_ONE,
        TEST_FILE,
        KEY_TWENTY_THREE_OBJECT_ID,
        DIR_TEN_OBJECT_ID,
        FSO_BUCKET_TWO_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_EIGHTEEN_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        ratisOne,
        epochMillis1, true);
    writeKeyToOm(reconOMMetadataManager,
        FILE_ONE,
        FSO_BUCKET_TWO,
        VOLUME_ONE,
        FILE_ONE,
        KEY_TWENTY_FOUR_OBJECT_ID,
        DIR_TEN_OBJECT_ID,
        FSO_BUCKET_TWO_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_NINETEEN_SIZE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        ratisOne,
        epochMillis1, true);
    // Write FSO bucket two keys - End

    // Write OBS Keys data - Start
    writeKeyToOm(reconOMMetadataManager,
        KEY_ONE,
        OBS_BUCKET,
        VOLUME_ONE,
        KEY_ONE,
        KEY_ONE_OBJECT_ID,
        OBS_BUCKET_OBJECT_ID,
        OBS_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_ONE_SIZE,
        BucketLayout.OBJECT_STORE,
        ratisOne,
        epochMillis1, true);
    writeKeyToOm(reconOMMetadataManager,
        KEY_TWO,
        OBS_BUCKET,
        VOLUME_ONE,
        KEY_TWO,
        KEY_TWO_OBJECT_ID,
        OBS_BUCKET_OBJECT_ID,
        OBS_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_TWO_SIZE,
        BucketLayout.OBJECT_STORE,
        ratisOne,
        epochMillis2, true);
    writeKeyToOm(reconOMMetadataManager,
        KEY_THREE,
        OBS_BUCKET,
        VOLUME_ONE,
        KEY_THREE,
        KEY_THREE_OBJECT_ID,
        OBS_BUCKET_OBJECT_ID,
        OBS_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_THREE_SIZE,
        BucketLayout.OBJECT_STORE,
        ratisOne,
        epochMillis2, true);
    writeKeyToOm(reconOMMetadataManager,
        KEY_FOUR,
        OBS_BUCKET,
        VOLUME_ONE,
        KEY_FOUR,
        KEY_FOUR_OBJECT_ID,
        OBS_BUCKET_OBJECT_ID,
        OBS_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_FOUR_SIZE,
        BucketLayout.OBJECT_STORE,
        ratisOne,
        epochMillis2, true);
    writeKeyToOm(reconOMMetadataManager,
        KEY_FIVE,
        OBS_BUCKET,
        VOLUME_ONE,
        KEY_FIVE,
        KEY_FIVE_OBJECT_ID,
        OBS_BUCKET_OBJECT_ID,
        OBS_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_FIVE_SIZE,
        BucketLayout.OBJECT_STORE,
        ratisOne,
        epochMillis2, true);
    writeKeyToOm(reconOMMetadataManager,
        KEY_SIX,
        OBS_BUCKET,
        VOLUME_ONE,
        KEY_SIX,
        KEY_SIX_OBJECT_ID,
        OBS_BUCKET_OBJECT_ID,
        OBS_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_SIX_SIZE,
        BucketLayout.OBJECT_STORE,
        ratisOne,
        epochMillis2, true);
    // Write OBS Keys data - End

    // Write LEGACY keys data - Start
    writeDirToOm(reconOMMetadataManager,
        (DIR_FOUR + OM_KEY_PREFIX),
        LEGACY_BUCKET,
        VOLUME_ONE,
        DIR_FOUR,
        DIR_FOUR_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        LEGACY_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        getBucketLayout());
    writeDirToOm(reconOMMetadataManager,
        (DIR_FOUR + OM_KEY_PREFIX + DIR_FIVE + OM_KEY_PREFIX),
        LEGACY_BUCKET,
        VOLUME_ONE,
        DIR_FIVE,
        DIR_FIVE_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        LEGACY_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        getBucketLayout());
    writeDirToOm(reconOMMetadataManager,
        (DIR_FOUR + OM_KEY_PREFIX + DIR_SIX + OM_KEY_PREFIX),
        LEGACY_BUCKET,
        VOLUME_ONE,
        DIR_SIX,
        DIR_SIX_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        LEGACY_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        getBucketLayout());
    writeDirToOm(reconOMMetadataManager,
        (DIR_FOUR + OM_KEY_PREFIX + DIR_SEVEN + OM_KEY_PREFIX),
        LEGACY_BUCKET,
        VOLUME_ONE,
        DIR_FOUR,
        DIR_FOUR_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        LEGACY_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        getBucketLayout());

    // write all legacy bucket keys
    writeKeyToOm(reconOMMetadataManager,
        KEY_EIGHT,
        LEGACY_BUCKET,
        VOLUME_ONE,
        FILE_EIGHT,
        KEY_THIRTEEN_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        LEGACY_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_EIGHT_SIZE,
        getBucketLayout(),
        ratisOne,
        epochMillis1, true);
    writeKeyToOm(reconOMMetadataManager,
        KEY_NINE,
        LEGACY_BUCKET,
        VOLUME_ONE,
        FILE_NINE,
        KEY_FOURTEEN_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        LEGACY_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_NINE_SIZE,
        getBucketLayout(),
        ratisOne,
        epochMillis2, true);
    writeKeyToOm(reconOMMetadataManager,
        KEY_TEN,
        LEGACY_BUCKET,
        VOLUME_ONE,
        FILE_TEN,
        KEY_FIFTEEN_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        LEGACY_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_TEN_SIZE,
        getBucketLayout(),
        ratisOne,
        epochMillis1, true);
    writeKeyToOm(reconOMMetadataManager,
        KEY_ELEVEN,
        LEGACY_BUCKET,
        VOLUME_ONE,
        FILE_ELEVEN,
        KEY_SIXTEEN_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        LEGACY_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_ELEVEN_SIZE,
        getBucketLayout(),
        ratisOne,
        epochMillis2, true);
    writeKeyToOm(reconOMMetadataManager,
        KEY_TWELVE,
        LEGACY_BUCKET,
        VOLUME_ONE,
        FILE_TWELVE,
        KEY_SEVENTEEN_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        LEGACY_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_TWELVE_SIZE,
        getBucketLayout(),
        ratisOne,
        epochMillis1, true);
    writeKeyToOm(reconOMMetadataManager,
        KEY_THIRTEEN,
        LEGACY_BUCKET,
        VOLUME_ONE,
        FILE_THIRTEEN,
        KEY_EIGHTEEN_OBJECT_ID,
        PARENT_OBJECT_ID_ZERO,
        LEGACY_BUCKET_OBJECT_ID,
        VOLUME_ONE_OBJECT_ID,
        KEY_THIRTEEN_SIZE,
        getBucketLayout(),
        ratisOne,
        epochMillis1, true);
    // Write LEGACY keys data - End
  }

  @Test
  public void testGetOpenKeyInfo() throws Exception {
    OmKeyInfo omKeyInfo =
        getOmKeyInfo("sampleVol", "bucketOne", "key_one", true);

    reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_one", omKeyInfo);
    OmKeyInfo omKeyInfo1 =
        reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
            .get("/sampleVol/bucketOne/key_one");
    assertEquals("key_one", omKeyInfo1.getKeyName());
    Response openKeyInfoResp =
        omdbInsightEndpoint.getOpenKeyInfo(-1, "", "", true, true);
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) openKeyInfoResp.getEntity();
    assertNotNull(keyInsightInfoResp);
    assertEquals("sampleVol/bucketOne/key_one",
        keyInsightInfoResp.getNonFSOKeyInfoList().get(0).getPath());
  }

  @Test
  public void testKeyCountsForValidAndInvalidKeyPrefix() throws IOException {
    Timestamp now = new Timestamp(System.currentTimeMillis());
    ReconGlobalStatsManager statsManager = omdbInsightEndpoint.getReconGlobalStatsManager();

    // Insert valid key count with valid key prefix
    insertGlobalStatsRecords(statsManager, now,
        "openKeyTable" + "Count", 3L);
    insertGlobalStatsRecords(statsManager, now,
        "openFileTable" + "Count", 3L);
    insertGlobalStatsRecords(statsManager, now,
        "openKeyTable" + "ReplicatedDataSize", 150L);
    insertGlobalStatsRecords(statsManager, now,
        "openFileTable" + "ReplicatedDataSize", 150L);
    insertGlobalStatsRecords(statsManager, now,
        "openKeyTable" + "UnReplicatedDataSize", 50L);
    insertGlobalStatsRecords(statsManager, now,
        "openFileTable" + "UnReplicatedDataSize", 50L);

    Response openKeyInfoResp =
        omdbInsightEndpoint.getOpenKeySummary();
    assertNotNull(openKeyInfoResp);

    Map<String, Long> openKeysSummary =
        (Map<String, Long>) openKeyInfoResp.getEntity();
    // Assert that the key prefix format is accepted in the global stats
    assertEquals(6L,
        openKeysSummary.get("totalOpenKeys"));
    assertEquals(300L,
        openKeysSummary.get("totalReplicatedDataSize"));
    assertEquals(100L,
        openKeysSummary.get("totalUnreplicatedDataSize"));

    // Delete the previous records and Update the new value for valid key prefix
    Table<String, GlobalStatsValue> globalStatsTable = statsManager.getGlobalStatsTable();
    globalStatsTable.delete("openKeyTable" + "Count");
    globalStatsTable.delete("openFileTable" + "Count");
    globalStatsTable.delete("openKeyTable" + "ReplicatedDataSize");
    globalStatsTable.delete("openFileTable" + "ReplicatedDataSize");
    globalStatsTable.delete("openKeyTable" + "UnReplicatedDataSize");
    globalStatsTable.delete("openFileTable" + "UnReplicatedDataSize");

    // Insert new record for a key with invalid prefix
    insertGlobalStatsRecords(statsManager, now, "openKeyTable" + "InvalidPrefix",
        3L);
    insertGlobalStatsRecords(statsManager, now, "openFileTable" + "InvalidPrefix",
        3L);

    openKeyInfoResp =
        omdbInsightEndpoint.getOpenKeySummary();
    assertNotNull(openKeyInfoResp);

    openKeysSummary =
        (Map<String, Long>) openKeyInfoResp.getEntity();
    // Assert that the key format is not accepted in the global stats
    assertEquals(0L,
        openKeysSummary.get("totalOpenKeys"));
    assertEquals(0L,
        openKeysSummary.get("totalReplicatedDataSize"));
    assertEquals(0L,
        openKeysSummary.get("totalUnreplicatedDataSize"));
  }

  @Test
  public void testKeysSummaryAttribute() throws IOException {
    Timestamp now = new Timestamp(System.currentTimeMillis());
    ReconGlobalStatsManager statsManager = omdbInsightEndpoint.getReconGlobalStatsManager();
    // Insert records for replicated and unreplicated data sizes
    insertGlobalStatsRecords(statsManager, now, "openFileTableReplicatedDataSize",
        30L);
    insertGlobalStatsRecords(statsManager, now, "openKeyTableReplicatedDataSize",
        30L);
    insertGlobalStatsRecords(statsManager, now, "deletedTableReplicatedDataSize",
        30L);
    insertGlobalStatsRecords(statsManager, now, "openFileTableUnReplicatedDataSize",
        10L);
    insertGlobalStatsRecords(statsManager, now, "openKeyTableUnReplicatedDataSize",
        10L);
    insertGlobalStatsRecords(statsManager, now, "deletedTableUnReplicatedDataSize",
        10L);

    // Insert records for table counts
    insertGlobalStatsRecords(statsManager, now, "openKeyTableCount", 3L);
    insertGlobalStatsRecords(statsManager, now, "openFileTableCount", 3L);
    insertGlobalStatsRecords(statsManager, now, "deletedTableCount", 3L);

    // Call the API of Open keys to get the response
    Response openKeyInfoResp =
        omdbInsightEndpoint.getOpenKeySummary();
    assertNotNull(openKeyInfoResp);

    Map<String, Long> openKeysSummary =
        (Map<String, Long>) openKeyInfoResp.getEntity();

    assertEquals(60L,
        openKeysSummary.get("totalReplicatedDataSize"));
    assertEquals(20L,
        openKeysSummary.get("totalUnreplicatedDataSize"));
    assertEquals(6L,
        openKeysSummary.get("totalOpenKeys"));

    // Call the API of Deleted keys to get the response
    Response deletedKeyInfoResp =
        omdbInsightEndpoint.getDeletedKeySummary();
    assertNotNull(deletedKeyInfoResp);

    Map<String, Long> deletedKeysSummary = (Map<String, Long>)
        deletedKeyInfoResp.getEntity();

    assertEquals(30L,
        deletedKeysSummary.get("totalReplicatedDataSize"));
    assertEquals(10L,
        deletedKeysSummary.get("totalUnreplicatedDataSize"));
    assertEquals(3L,
        deletedKeysSummary.get("totalDeletedKeys"));
  }

  private void insertGlobalStatsRecords(ReconGlobalStatsManager statsManager,
                                        Timestamp timestamp, String key,
                                        long value) throws IOException {
    GlobalStatsValue newRecord = new GlobalStatsValue(value);
    statsManager.getGlobalStatsTable().put(key, newRecord);
  }

  @Test
  public void testGetOpenKeyInfoLimitParam() throws Exception {
    OmKeyInfo omKeyInfo1 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_one", true);
    OmKeyInfo omKeyInfo2 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_two", true);
    OmKeyInfo omKeyInfo3 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_three", true);

    reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_one", omKeyInfo1);
    reconOMMetadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .put("/sampleVol/bucketOne/key_two", omKeyInfo2);
    reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_three", omKeyInfo3);
    Response openKeyInfoResp =
        omdbInsightEndpoint.getOpenKeyInfo(2, "", "", true, true);
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) openKeyInfoResp.getEntity();
    assertNotNull(keyInsightInfoResp);
    assertEquals(2,
        keyInsightInfoResp.getNonFSOKeyInfoList().size());
    assertEquals(0, keyInsightInfoResp.getFsoKeyInfoList().size());
    assertEquals(2, keyInsightInfoResp.getFsoKeyInfoList().size() +
        keyInsightInfoResp.getNonFSOKeyInfoList().size());
    assertEquals("sampleVol/bucketOne/key_three",
        keyInsightInfoResp.getNonFSOKeyInfoList().get(1).getPath());

    openKeyInfoResp = omdbInsightEndpoint.getOpenKeyInfo(3, "", "", true, true);
    keyInsightInfoResp =
        (KeyInsightInfoResponse) openKeyInfoResp.getEntity();
    assertNotNull(keyInsightInfoResp);
    assertEquals(2,
        keyInsightInfoResp.getNonFSOKeyInfoList().size());
    assertEquals(1, keyInsightInfoResp.getFsoKeyInfoList().size());
    assertEquals(3, keyInsightInfoResp.getFsoKeyInfoList().size() +
        keyInsightInfoResp.getNonFSOKeyInfoList().size());
    assertEquals("sampleVol/bucketOne/key_three",
        keyInsightInfoResp.getNonFSOKeyInfoList().get(1).getPath());
  }

  @Test
  public void testGetOpenKeyInfoWithIncludeFsoAndIncludeNonFsoParams()
      throws Exception {
    OmKeyInfo omKeyInfo1 =
        getOmKeyInfo("sampleVol", "non_fso_Bucket", "non_fso_key1", true);
    OmKeyInfo omKeyInfo2 =
        getOmKeyInfo("sampleVol", "non_fso_Bucket", "non_fso_key2", true);
    OmKeyInfo omKeyInfo3 =
        getOmKeyInfo("sampleVol", "non_fso_Bucket", "non_fso_key3", true);

    reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
        .put("/sampleVol/non_fso_Bucket/non_fso_key1", omKeyInfo1);
    reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
        .put("/sampleVol/non_fso_Bucket/non_fso_key2", omKeyInfo2);
    reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
        .put("/sampleVol/non_fso_Bucket/non_fso_key3", omKeyInfo3);

    omKeyInfo1 =
        getOmKeyInfo("sampleVol", "fso_Bucket", "fso_key1", false);
    omKeyInfo2 =
        getOmKeyInfo("sampleVol", "fso_Bucket", "fso_key2", false);
    omKeyInfo3 =
        getOmKeyInfo("sampleVol", "fso_Bucket", "fso_key3", false);
    OmKeyInfo omKeyInfo4 =
        getOmKeyInfo("sampleVol", "fso_Bucket", "fso_key4", false);

    reconOMMetadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .put("/sampleVol/fso_Bucket/fso_key1", omKeyInfo1);
    reconOMMetadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .put("/sampleVol/fso_Bucket/fso_key2", omKeyInfo2);
    reconOMMetadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .put("/sampleVol/fso_Bucket/fso_key3", omKeyInfo3);
    reconOMMetadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .put("/sampleVol/fso_Bucket/fso_key4", omKeyInfo4);

    // CASE 1 :- Display only FSO keys in response
    // includeFsoKeys=true, includeNonFsoKeys=false
    Response openKeyInfoResp =
        omdbInsightEndpoint.getOpenKeyInfo(10, "", "", true, false);
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) openKeyInfoResp.getEntity();
    assertNotNull(keyInsightInfoResp);
    assertEquals(4,
        keyInsightInfoResp.getFsoKeyInfoList().size());
    assertEquals(0,
        keyInsightInfoResp.getNonFSOKeyInfoList().size());

    // CASE 2 :- Display only Non-FSO keys in response
    // includeFsoKeys=false, includeNonFsoKeys=true
    openKeyInfoResp =
        omdbInsightEndpoint.getOpenKeyInfo(10, "", "", false, true);
    keyInsightInfoResp = (KeyInsightInfoResponse) openKeyInfoResp.getEntity();
    assertNotNull(keyInsightInfoResp);
    assertEquals(0,
        keyInsightInfoResp.getFsoKeyInfoList().size());
    assertEquals(3,
        keyInsightInfoResp.getNonFSOKeyInfoList().size());

    // CASE 3 :- Display both FSO and Non-FSO keys in response
    // includeFsoKeys=true, includeNonFsoKeys=true
    openKeyInfoResp =
        omdbInsightEndpoint.getOpenKeyInfo(10, "", "", true, true);
    keyInsightInfoResp = (KeyInsightInfoResponse) openKeyInfoResp.getEntity();
    assertNotNull(keyInsightInfoResp);
    assertEquals(4,
        keyInsightInfoResp.getFsoKeyInfoList().size());
    assertEquals(3,
        keyInsightInfoResp.getNonFSOKeyInfoList().size());

    // CASE 4 :- Don't Display both FSO and Non-FSO keys in response
    // includeFsoKeys=false, includeNonFsoKeys=false
    openKeyInfoResp =
        omdbInsightEndpoint.getOpenKeyInfo(10, "", "", false, false);
    assertEquals(204, openKeyInfoResp.getStatus());
    String entity = (String) openKeyInfoResp.getEntity();
    assertTrue(entity.contains("No keys matched the search prefix"),
        "Expected a message indicating no keys were found");
  }

  @Test
  public void testGetOpenKeyInfoPrevKeyParam() throws Exception {
    OmKeyInfo omKeyInfo1 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_1", true);
    OmKeyInfo omKeyInfo2 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_2", true);
    OmKeyInfo omKeyInfo3 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_3", true);

    reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_1", omKeyInfo1);
    reconOMMetadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .put("/sampleVol/bucketOne/key_2", omKeyInfo2);
    reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_3", omKeyInfo3);
    Response openKeyInfoResp =
        omdbInsightEndpoint.getOpenKeyInfo(-1, "/sampleVol/bucketOne/key_1", "",
            true, true);
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) openKeyInfoResp.getEntity();
    assertNotNull(keyInsightInfoResp);
    assertEquals(1, keyInsightInfoResp.getNonFSOKeyInfoList().size());
    assertEquals(1, keyInsightInfoResp.getFsoKeyInfoList().size());
    assertEquals(2, keyInsightInfoResp.getFsoKeyInfoList().size() + keyInsightInfoResp.getNonFSOKeyInfoList().size());
    assertEquals("sampleVol/bucketOne/key_3", keyInsightInfoResp.getNonFSOKeyInfoList().get(0).getPath());
    assertEquals("sampleVol/bucketOne/key_2", keyInsightInfoResp.getFsoKeyInfoList().get(0).getPath());
  }

  @Test
  public void testGetDeletedKeyInfoLimitParam() throws Exception {
    OmKeyInfo omKeyInfo1 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_one", true);
    OmKeyInfo omKeyInfo2 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_two", true);
    OmKeyInfo omKeyInfo3 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_three", true);

    reconOMMetadataManager.getKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_one", omKeyInfo1);
    reconOMMetadataManager.getKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_two", omKeyInfo2);
    reconOMMetadataManager.getKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_three", omKeyInfo3);

    OmKeyInfo omKeyInfoCopy =
        reconOMMetadataManager.getKeyTable(getBucketLayout())
            .get("/sampleVol/bucketOne/key_one");
    assertEquals("key_one", omKeyInfoCopy.getKeyName());
    RepeatedOmKeyInfo repeatedOmKeyInfo1 = new RepeatedOmKeyInfo(omKeyInfoCopy, 1);

    reconOMMetadataManager.getDeletedTable()
        .put("/sampleVol/bucketOne/key_one", repeatedOmKeyInfo1);
    assertEquals("key_one",
        repeatedOmKeyInfo1.getOmKeyInfoList().get(0).getKeyName());

    RepeatedOmKeyInfo repeatedOmKeyInfo2 = new RepeatedOmKeyInfo(omKeyInfo2, 1);
    RepeatedOmKeyInfo repeatedOmKeyInfo3 = new RepeatedOmKeyInfo(omKeyInfo2, 1);
    reconOMMetadataManager.getDeletedTable()
        .put("/sampleVol/bucketOne/key_two", repeatedOmKeyInfo2);
    reconOMMetadataManager.getDeletedTable()
        .put("/sampleVol/bucketOne/key_three", repeatedOmKeyInfo3);

    Response deletedKeyInfo = omdbInsightEndpoint.getDeletedKeyInfo(2, "", "");
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) deletedKeyInfo.getEntity();
    assertNotNull(keyInsightInfoResp);
    assertEquals(2,
        keyInsightInfoResp.getRepeatedOmKeyInfoList().size());
    assertEquals("key_two",
        keyInsightInfoResp.getRepeatedOmKeyInfoList().get(1).getOmKeyInfoList()
            .get(0).getKeyName());
  }

  @Test
  public void testGetDeletedKeyInfoPrevKeyParam() throws Exception {
    OmKeyInfo omKeyInfo1 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_one", true);
    OmKeyInfo omKeyInfo2 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_two", true);
    OmKeyInfo omKeyInfo3 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_three", true);

    RepeatedOmKeyInfo repeatedOmKeyInfo1 = new RepeatedOmKeyInfo(omKeyInfo1, 1);
    RepeatedOmKeyInfo repeatedOmKeyInfo2 = new RepeatedOmKeyInfo(omKeyInfo2, 1);
    RepeatedOmKeyInfo repeatedOmKeyInfo3 = new RepeatedOmKeyInfo(omKeyInfo3, 1);

    reconOMMetadataManager.getDeletedTable()
        .put("/sampleVol/bucketOne/key_one", repeatedOmKeyInfo1);
    reconOMMetadataManager.getDeletedTable()
        .put("/sampleVol/bucketOne/key_two", repeatedOmKeyInfo2);
    reconOMMetadataManager.getDeletedTable()
        .put("/sampleVol/bucketOne/key_three", repeatedOmKeyInfo3);

    Response deletedKeyInfo = omdbInsightEndpoint.getDeletedKeyInfo(2,
        "/sampleVol/bucketOne/key_one", "");
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) deletedKeyInfo.getEntity();
    assertNotNull(keyInsightInfoResp);
    assertEquals(2,
        keyInsightInfoResp.getRepeatedOmKeyInfoList().size());

    List<String> pendingDeleteKeys =
        keyInsightInfoResp.getRepeatedOmKeyInfoList().stream()
            .map(
                repeatedOmKeyInfo -> repeatedOmKeyInfo.getOmKeyInfoList().get(0)
                    .getKeyName())
            .collect(Collectors.toList());
    assertThat(pendingDeleteKeys).doesNotContain("key_one");
  }

  @Test
  public void testGetDeletedKeyInfo() throws Exception {
    OmKeyInfo omKeyInfo =
        getOmKeyInfo("sampleVol", "bucketOne", "key_one", true);

    reconOMMetadataManager.getKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_one", omKeyInfo);
    OmKeyInfo omKeyInfo1 = reconOMMetadataManager.getKeyTable(getBucketLayout())
        .get("/sampleVol/bucketOne/key_one");
    assertEquals("key_one", omKeyInfo1.getKeyName());
    RepeatedOmKeyInfo repeatedOmKeyInfo = new RepeatedOmKeyInfo(omKeyInfo, 1);
    reconOMMetadataManager.getDeletedTable()
        .put("/sampleVol/bucketOne/key_one", repeatedOmKeyInfo);
    RepeatedOmKeyInfo repeatedOmKeyInfo1 =
        reconOMMetadataManager.getDeletedTable()
            .get("/sampleVol/bucketOne/key_one");
    assertEquals("key_one",
        repeatedOmKeyInfo1.getOmKeyInfoList().get(0).getKeyName());
    Response deletedKeyInfo = omdbInsightEndpoint.getDeletedKeyInfo(-1, "", "");
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) deletedKeyInfo.getEntity();
    assertNotNull(keyInsightInfoResp);
    assertEquals("key_one",
        keyInsightInfoResp.getRepeatedOmKeyInfoList().get(0).getOmKeyInfoList()
            .get(0).getKeyName());
  }

  @Test
  public void testGetDeletedKeysWithPrevKeyProvidedAndStartPrefixEmpty()
      throws Exception {
    // Prepare mock data in the deletedTable.
    for (int i = 1; i <= 10; i++) {
      OmKeyInfo omKeyInfo =
          getOmKeyInfo("sampleVol", "bucketOne", "deleted_key_" + i, true);
      reconOMMetadataManager.getDeletedTable()
          .put("/sampleVol/bucketOne/deleted_key_" + i,
              new RepeatedOmKeyInfo(omKeyInfo, 1));
    }

    // Case 1: prevKey provided, startPrefix empty
    Response deletedKeyInfoResponse = omdbInsightEndpoint.getDeletedKeyInfo(5,
        "/sampleVol/bucketOne/deleted_key_3", "");
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) deletedKeyInfoResponse.getEntity();

    // Validate that the response skips the prevKey and returns subsequent records.
    assertNotNull(keyInsightInfoResp);
    assertEquals(5, keyInsightInfoResp.getRepeatedOmKeyInfoList().size());
    assertEquals("deleted_key_4",
        keyInsightInfoResp.getRepeatedOmKeyInfoList().get(0).getOmKeyInfoList().get(0).getKeyName());
    assertEquals("deleted_key_8",
        keyInsightInfoResp.getRepeatedOmKeyInfoList().get(4).getOmKeyInfoList().get(0).getKeyName());
  }

  @Test
  public void testGetDeletedKeysWithPrevKeyEmptyAndStartPrefixEmpty()
      throws Exception {
    // Prepare mock data in the deletedTable.
    for (int i = 1; i < 10; i++) {
      OmKeyInfo omKeyInfo =
          getOmKeyInfo("sampleVol", "bucketOne", "deleted_key_" + i, true);
      reconOMMetadataManager.getDeletedTable()
          .put("/sampleVol/bucketOne/deleted_key_" + i, new RepeatedOmKeyInfo(omKeyInfo, 1));
    }

    // Case 2: prevKey empty, startPrefix empty
    Response deletedKeyInfoResponse =
        omdbInsightEndpoint.getDeletedKeyInfo(5, "", "");
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) deletedKeyInfoResponse.getEntity();

    // Validate that the response retrieves from the beginning.
    assertNotNull(keyInsightInfoResp);
    assertEquals(5, keyInsightInfoResp.getRepeatedOmKeyInfoList().size());
    assertEquals("deleted_key_1",
        keyInsightInfoResp.getRepeatedOmKeyInfoList().get(0).getOmKeyInfoList().get(0).getKeyName());
    assertEquals("deleted_key_5",
        keyInsightInfoResp.getRepeatedOmKeyInfoList().get(4).getOmKeyInfoList().get(0).getKeyName());
  }

  @Test
  public void testGetDeletedKeysWithStartPrefixProvidedAndPrevKeyEmpty()
      throws Exception {
    // Prepare mock data in the deletedTable.
    for (int i = 1; i < 5; i++) {
      OmKeyInfo omKeyInfo =
          getOmKeyInfo("sampleVol", "bucketOne", "deleted_key_" + i, true);
      reconOMMetadataManager.getDeletedTable()
          .put("/sampleVol/bucketOne/deleted_key_" + i, new RepeatedOmKeyInfo(omKeyInfo, 1));
    }
    for (int i = 5; i < 10; i++) {
      OmKeyInfo omKeyInfo =
          getOmKeyInfo("sampleVol", "bucketTwo", "deleted_key_" + i, true);
      reconOMMetadataManager.getDeletedTable()
          .put("/sampleVol/bucketTwo/deleted_key_" + i, new RepeatedOmKeyInfo(omKeyInfo, 1));
    }

    // Case 3: startPrefix provided, prevKey empty
    Response deletedKeyInfoResponse =
        omdbInsightEndpoint.getDeletedKeyInfo(5, "",
            "/sampleVol/bucketOne/");
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) deletedKeyInfoResponse.getEntity();

    // Validate that the response retrieves starting from the prefix.
    assertNotNull(keyInsightInfoResp);
    assertEquals(4, keyInsightInfoResp.getRepeatedOmKeyInfoList().size());
    assertEquals("deleted_key_1",
        keyInsightInfoResp.getRepeatedOmKeyInfoList().get(0).getOmKeyInfoList().get(0).getKeyName());
    assertEquals("deleted_key_4",
        keyInsightInfoResp.getRepeatedOmKeyInfoList().get(3).getOmKeyInfoList().get(0).getKeyName());
  }

  @Test
  public void testGetDeletedKeysWithBothPrevKeyAndStartPrefixProvided()
      throws IOException {
    // Prepare mock data in the deletedTable.
    for (int i = 1; i < 10; i++) {
      OmKeyInfo omKeyInfo =
          getOmKeyInfo("sampleVol", "bucketOne", "deleted_key_" + i, true);
      reconOMMetadataManager.getDeletedTable()
          .put("/sampleVol/bucketOne/deleted_key_" + i, new RepeatedOmKeyInfo(omKeyInfo, 1));
    }
    for (int i = 10; i < 15; i++) {
      OmKeyInfo omKeyInfo =
          getOmKeyInfo("sampleVol", "bucketTwo", "deleted_key_" + i, true);
      reconOMMetadataManager.getDeletedTable()
          .put("/sampleVol/bucketTwo/deleted_key_" + i, new RepeatedOmKeyInfo(omKeyInfo, 1));
    }

    // Case 4: startPrefix and prevKey provided
    Response deletedKeyInfoResponse =
        omdbInsightEndpoint.getDeletedKeyInfo(5,
            "/sampleVol/bucketOne/deleted_key_5",
            "/sampleVol/bucketOne/");

    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) deletedKeyInfoResponse.getEntity();

    // Validate that the response retrieves starting from the prefix and skips the prevKey.
    assertNotNull(keyInsightInfoResp);
    assertEquals(4, keyInsightInfoResp.getRepeatedOmKeyInfoList().size());
    assertEquals("deleted_key_6",
        keyInsightInfoResp.getRepeatedOmKeyInfoList().get(0).getOmKeyInfoList().get(0).getKeyName());
    assertEquals("deleted_key_9",
        keyInsightInfoResp.getRepeatedOmKeyInfoList().get(3).getOmKeyInfoList().get(0).getKeyName());
  }

  private OmKeyInfo getOmKeyInfo(String volumeName, String bucketName,
                                 String keyName, boolean isFile) {
    return buildOmKeyInfo(volumeName, bucketName, keyName, isFile,
        StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE));
  }

  private OmKeyInfo getOmKeyInfo(String volumeName, String bucketName,
                                 String keyName, boolean isFile, ReplicationConfig replicationConfig) {
    return buildOmKeyInfo(volumeName, bucketName, keyName, isFile, replicationConfig);
  }

  private OmKeyInfo buildOmKeyInfo(String volumeName, String bucketName,
                                   String keyName, boolean isFile, ReplicationConfig replicationConfig) {
    return new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setFile(isFile)
        .setObjectID(generateUniqueRandomLong())
        .setReplicationConfig(replicationConfig)
        .setDataSize(random.nextLong())
        .build();
  }

  @Test
  public void testGetDeletedDirInfoLimitParam() throws Exception {
    OmKeyInfo omKeyInfo1 =
        getOmKeyInfo("sampleVol", "bucketOne", "dir_one", false);
    OmKeyInfo omKeyInfo2 =
        getOmKeyInfo("sampleVol", "bucketOne", "dir_two", false);
    OmKeyInfo omKeyInfo3 =
        getOmKeyInfo("sampleVol", "bucketOne", "dir_three", false);

    reconOMMetadataManager.getDeletedDirTable()
        .put("/sampleVol/bucketOne/dir_one", omKeyInfo1);
    reconOMMetadataManager.getDeletedDirTable()
        .put("/sampleVol/bucketOne/dir_two", omKeyInfo2);
    reconOMMetadataManager.getDeletedDirTable()
        .put("/sampleVol/bucketOne/dir_three", omKeyInfo3);

    OmKeyInfo omKeyInfoCopy =
        reconOMMetadataManager.getDeletedDirTable()
            .get("/sampleVol/bucketOne/dir_one");
    assertEquals("dir_one", omKeyInfoCopy.getKeyName());

    Response deletedDirInfo = omdbInsightEndpoint.getDeletedDirInfo(2, "");
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) deletedDirInfo.getEntity();
    assertNotNull(keyInsightInfoResp);
    assertEquals(2,
        keyInsightInfoResp.getDeletedDirInfoList().size());
    assertEquals("dir_one",
        keyInsightInfoResp.getDeletedDirInfoList().get(0).getKey());
  }

  @Test
  public void testGetDeletedDirInfoPrevKeyParam() throws Exception {
    OmKeyInfo omKeyInfo1 =
        getOmKeyInfo("sampleVol", "bucketOne", "dir_one", false);
    OmKeyInfo omKeyInfo2 =
        getOmKeyInfo("sampleVol", "bucketOne", "dir_two", false);
    OmKeyInfo omKeyInfo3 =
        getOmKeyInfo("sampleVol", "bucketOne", "dir_three", false);

    reconOMMetadataManager.getDeletedDirTable()
        .put("/sampleVol/bucketOne/dir_one", omKeyInfo1);
    reconOMMetadataManager.getDeletedDirTable()
        .put("/sampleVol/bucketOne/dir_two", omKeyInfo2);
    reconOMMetadataManager.getDeletedDirTable()
        .put("/sampleVol/bucketOne/dir_three", omKeyInfo3);

    OmKeyInfo omKeyInfoCopy =
        reconOMMetadataManager.getDeletedDirTable()
            .get("/sampleVol/bucketOne/dir_one");
    assertEquals("dir_one", omKeyInfoCopy.getKeyName());

    Response deletedDirInfo = omdbInsightEndpoint.getDeletedDirInfo(2,
        "/sampleVol/bucketOne/dir_one");
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) deletedDirInfo.getEntity();
    assertNotNull(keyInsightInfoResp);
    assertEquals(2,
        keyInsightInfoResp.getDeletedDirInfoList().size());
    assertEquals("dir_three",
        keyInsightInfoResp.getDeletedDirInfoList().get(0).getKey());
    assertEquals("/sampleVol/bucketOne/dir_two",
        keyInsightInfoResp.getLastKey());
  }

  @Test
  public void testGetDeletedDirInfo() throws Exception {
    OmKeyInfo omKeyInfo1 =
        getOmKeyInfo("sampleVol", "bucketOne", "dir_one", false);
    OmKeyInfo omKeyInfo2 =
        getOmKeyInfo("sampleVol", "bucketOne", "dir_two", false);
    OmKeyInfo omKeyInfo3 =
        getOmKeyInfo("sampleVol", "bucketOne", "dir_three", false);

    reconOMMetadataManager.getDeletedDirTable()
        .put("/sampleVol/bucketOne/dir_one", omKeyInfo1);
    reconOMMetadataManager.getDeletedDirTable()
        .put("/sampleVol/bucketOne/dir_two", omKeyInfo2);
    reconOMMetadataManager.getDeletedDirTable()
        .put("/sampleVol/bucketOne/dir_three", omKeyInfo3);

    OmKeyInfo omKeyInfoCopy =
        reconOMMetadataManager.getDeletedDirTable()
            .get("/sampleVol/bucketOne/dir_one");
    assertEquals("dir_one", omKeyInfoCopy.getKeyName());

    Response deletedDirInfo = omdbInsightEndpoint.getDeletedDirInfo(-1, "");
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) deletedDirInfo.getEntity();
    assertNotNull(keyInsightInfoResp);
    assertEquals(3,
        keyInsightInfoResp.getDeletedDirInfoList().size());
    assertEquals("sampleVol/bucketOne/dir_one", keyInsightInfoResp
        .getDeletedDirInfoList().get(0).getPath());
    assertEquals("dir_one",
        keyInsightInfoResp.getDeletedDirInfoList().get(0).getKey());
    assertEquals("/sampleVol/bucketOne/dir_two",
        keyInsightInfoResp.getLastKey());
  }

  @ParameterizedTest
  @MethodSource("replicationConfigValues")
  public void testGetDirectorySizeInfo(ReplicationConfig replicationConfig) throws Exception {

    OmKeyInfo omKeyInfo1 =
        getOmKeyInfo("sampleVol", "bucketOne", "dir1", false, replicationConfig);
    OmKeyInfo omKeyInfo2 =
        getOmKeyInfo("sampleVol", "bucketTwo", "dir2", false, replicationConfig);
    OmKeyInfo omKeyInfo3 =
        getOmKeyInfo("sampleVol", "bucketThree", "dir3", false,
            replicationConfig);

    // Add 3 entries to deleted dir table for directory dir1, dir2 and dir3
    // having object id 1, 2 and 3 respectively
    reconOMMetadataManager.getDeletedDirTable()
        .put("/18/21/21/dir1/1", omKeyInfo1);
    reconOMMetadataManager.getDeletedDirTable()
        .put("/18/26/26/dir2/2", omKeyInfo2);
    reconOMMetadataManager.getDeletedDirTable()
        .put("/18/28/28/dir3/3", omKeyInfo3);

    // Prepare NS summary data and populate the table
    Table<Long, NSSummary> table = omdbInsightEndpoint.getNsSummaryTable();
    // Set size of files to 5 for directory object id 1
    table.put(omKeyInfo1.getObjectID(), getNsSummary(5L, replicationConfig));
    // Set size of files to 6 for directory object id 2
    table.put(omKeyInfo2.getObjectID(), getNsSummary(6L, replicationConfig));
    // Set size of files to 7 for directory object id 3
    table.put(omKeyInfo3.getObjectID(), getNsSummary(7L, replicationConfig));

    Response deletedDirInfo = omdbInsightEndpoint.getDeletedDirInfo(-1, "");
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) deletedDirInfo.getEntity();
    assertNotNull(keyInsightInfoResp);
    assertEquals(3,
        keyInsightInfoResp.getDeletedDirInfoList().size());
    // Assert the total size under directory dir1 is 5L
    assertEquals(5L,
        keyInsightInfoResp.getDeletedDirInfoList().get(0).getSize());
    assertEquals(QuotaUtil.getReplicatedSize(5L, replicationConfig),
        keyInsightInfoResp.getDeletedDirInfoList().get(0).getReplicatedSize());
    // Assert the total size under directory dir2 is 6L
    assertEquals(6L,
        keyInsightInfoResp.getDeletedDirInfoList().get(1).getSize());
    assertEquals(QuotaUtil.getReplicatedSize(6L, replicationConfig),
        keyInsightInfoResp.getDeletedDirInfoList().get(1).getReplicatedSize());
    // Assert the total size under directory dir3 is 7L
    assertEquals(7L,
        keyInsightInfoResp.getDeletedDirInfoList().get(2).getSize());
    assertEquals(QuotaUtil.getReplicatedSize(7L, replicationConfig),
        keyInsightInfoResp.getDeletedDirInfoList().get(2).getReplicatedSize());

    // Assert the total of all the deleted directories is 18L
    assertEquals(18L, keyInsightInfoResp.getUnreplicatedDataSize());
    assertEquals(QuotaUtil.getReplicatedSize(18L, replicationConfig),
        keyInsightInfoResp.getReplicatedDataSize());
  }

  @Test
  public void testListKeysFSOBucket() {
    // bucket level DU
    Response bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, FSO_BUCKET_PATH,
        "", 1000);
    ListKeysResponse listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(6, listKeysResponse.getKeys().size());
    ReconBasicOmKeyInfo keyEntityInfo = listKeysResponse.getKeys().get(0);
    assertEquals("volume1/fso-bucket/dir1/file1", keyEntityInfo.getPath());
    assertEquals("/1/10/11/file1", keyEntityInfo.getKey());
    assertEquals("/1/10/13/testfile", listKeysResponse.getLastKey());
    assertEquals("RATIS", keyEntityInfo.getReplicationConfig().getReplicationType().toString());

    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, DIR_ONE_PATH,
        "", 1000);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(6, listKeysResponse.getKeys().size());

    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, DIR_TWO_PATH,
        "", 1000);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(4, listKeysResponse.getKeys().size());

    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, DIR_THREE_PATH,
        "", 1000);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(2, listKeysResponse.getKeys().size());
  }

  @Test
  public void testListKeysFSOBucketWithLimitAndPagination() {
    // bucket level keyList
    // Total 3 pages , each page 2 records. If each page we will retrieve 2 items, as total 6 FSO keys,
    // so till we get empty last key, we'll continue to fetch and empty last key signifies the last page.
    // First Page
    Response bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, FSO_BUCKET_PATH,
        "", 2);
    ListKeysResponse listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(2, listKeysResponse.getKeys().size());
    ReconBasicOmKeyInfo keyEntityInfo = listKeysResponse.getKeys().get(0);
    assertEquals("volume1/fso-bucket/dir1/file1", keyEntityInfo.getPath());
    assertEquals("/1/10/11/testfile", listKeysResponse.getLastKey());
    assertEquals("RATIS", keyEntityInfo.getReplicationConfig().getReplicationType().toString());

    // Second page
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0,
        FSO_BUCKET_PATH, listKeysResponse.getLastKey(), 2);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(2, listKeysResponse.getKeys().size());
    keyEntityInfo = listKeysResponse.getKeys().get(0);
    assertEquals("volume1/fso-bucket/dir1/dir2/file1", keyEntityInfo.getPath());
    assertEquals("/1/10/12/testfile", listKeysResponse.getLastKey());

    // Third and last page. And last page will have empty
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0,
        FSO_BUCKET_PATH, listKeysResponse.getLastKey(), 2);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(2, listKeysResponse.getKeys().size());
    keyEntityInfo = listKeysResponse.getKeys().get(0);
    assertEquals("volume1/fso-bucket/dir1/dir2/dir3/file1", keyEntityInfo.getPath());
    assertEquals("/1/10/13/testfile", listKeysResponse.getLastKey());

    // Try again if fourth page is available. Ideally there should not be any further records
    // and lastKey should be empty as per design.
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0,
        FSO_BUCKET_PATH, listKeysResponse.getLastKey(), 2);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(0, listKeysResponse.getKeys().size());
    assertEquals("", listKeysResponse.getLastKey());
  }

  @Test
  public void testListKeysFSOBucketDirOnePathWithLimitTwoAndPagination() {
    // bucket level keyList
    // Total 3 pages , each page 2 records. If each page we will retrieve 2 items, as total 6 FSO keys,
    // so till we get empty last key, we'll continue to fetch and empty last key signifies the last page.
    // First Page
    Response bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, DIR_ONE_PATH,
        "", 2);
    ListKeysResponse listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(2, listKeysResponse.getKeys().size());
    ReconBasicOmKeyInfo keyEntityInfo = listKeysResponse.getKeys().get(0);
    assertEquals("volume1/fso-bucket/dir1/file1", keyEntityInfo.getPath());
    assertEquals("/1/10/11/testfile", listKeysResponse.getLastKey());
    assertEquals("RATIS", keyEntityInfo.getReplicationConfig().getReplicationType().toString());

    // Second page
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0,
        DIR_ONE_PATH, listKeysResponse.getLastKey(), 2);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(2, listKeysResponse.getKeys().size());
    keyEntityInfo = listKeysResponse.getKeys().get(0);
    assertEquals("volume1/fso-bucket/dir1/dir2/file1", keyEntityInfo.getPath());
    assertEquals("/1/10/12/testfile", listKeysResponse.getLastKey());

    // Third and last page. And last page will have empty
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0,
        DIR_ONE_PATH, listKeysResponse.getLastKey(), 2);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(2, listKeysResponse.getKeys().size());
    keyEntityInfo = listKeysResponse.getKeys().get(0);
    assertEquals("volume1/fso-bucket/dir1/dir2/dir3/file1", keyEntityInfo.getPath());
    assertEquals("/1/10/13/testfile", listKeysResponse.getLastKey());

    // Try again if fourth page is available. Ideally there should not be any further records
    // and lastKey should be empty as per design.
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0,
        DIR_ONE_PATH, listKeysResponse.getLastKey(), 2);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(0, listKeysResponse.getKeys().size());
    assertEquals("", listKeysResponse.getLastKey());
  }

  @Test
  public void testListKeysFSOBucketDirOnePathWithLimitOneAndPagination() {
    // bucket level keyList
    // Total 3 pages , each page 2 records. If each page we will retrieve 2 items, as total 6 FSO keys,
    // so till we get empty last key, we'll continue to fetch and empty last key signifies the last page.
    // First Page
    Response bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, DIR_ONE_PATH,
        "", 1);
    ListKeysResponse listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(1, listKeysResponse.getKeys().size());
    ReconBasicOmKeyInfo keyEntityInfo = listKeysResponse.getKeys().get(0);
    assertEquals("volume1/fso-bucket/dir1/file1", keyEntityInfo.getPath());
    assertEquals("/1/10/11/file1", listKeysResponse.getLastKey());
    assertEquals("RATIS", keyEntityInfo.getReplicationConfig().getReplicationType().toString());

    // Second page
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0,
        DIR_ONE_PATH, listKeysResponse.getLastKey(), 2);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(2, listKeysResponse.getKeys().size());
    keyEntityInfo = listKeysResponse.getKeys().get(1);
    assertEquals("volume1/fso-bucket/dir1/dir2/file1", keyEntityInfo.getPath());
    assertEquals("/1/10/12/file1", listKeysResponse.getLastKey());

    // Third page
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0,
        DIR_ONE_PATH, listKeysResponse.getLastKey(), 2);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(2, listKeysResponse.getKeys().size());
    keyEntityInfo = listKeysResponse.getKeys().get(1);
    assertEquals("volume1/fso-bucket/dir1/dir2/dir3/file1", keyEntityInfo.getPath());
    assertEquals("/1/10/13/file1", listKeysResponse.getLastKey());

    // Fourth page will have just one key
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0,
        DIR_ONE_PATH, listKeysResponse.getLastKey(), 2);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(1, listKeysResponse.getKeys().size());
    keyEntityInfo = listKeysResponse.getKeys().get(0);
    assertEquals("volume1/fso-bucket/dir1/dir2/dir3/testfile", keyEntityInfo.getPath());
    assertEquals("/1/10/13/testfile", listKeysResponse.getLastKey());

    // Try again if fifth page is available. Ideally there should not be any further records
    // and lastKey should be empty as per design.
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0,
        DIR_ONE_PATH, listKeysResponse.getLastKey(), 2);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(0, listKeysResponse.getKeys().size());
    assertEquals("", listKeysResponse.getLastKey());
  }

  @Test
  public void testListKeysFSOBucketTwoPathWithLimitAcrossDirsAtBucketLevel() {
    // bucket level keyList
    // Total 3 pages , each page 2 records. If each page we will retrieve 2 items, as total 6 FSO keys,
    // so till we get empty last key, we'll continue to fetch and empty last key signifies the last page.
    // First Page
    Response bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, FSO_BUCKET_TWO_PATH,
        "", 3);
    ListKeysResponse listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(3, listKeysResponse.getKeys().size());
    ReconBasicOmKeyInfo keyEntityInfo = listKeysResponse.getKeys().get(0);
    assertEquals("volume1/fso-bucket2/dir8/file1", keyEntityInfo.getPath());
    assertEquals("/1/30/32/file1", listKeysResponse.getLastKey());
    assertEquals("RATIS", keyEntityInfo.getReplicationConfig().getReplicationType().toString());

    // Second page
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0,
        FSO_BUCKET_TWO_PATH, listKeysResponse.getLastKey(), 3);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(3, listKeysResponse.getKeys().size());
    keyEntityInfo = listKeysResponse.getKeys().get(0);
    assertEquals("volume1/fso-bucket2/dir9/testfile", keyEntityInfo.getPath());
    assertEquals("/1/30/33/testfile", listKeysResponse.getLastKey());

    // Try again if third page is available. Ideally there should not be any further records
    // and lastKey should be empty as per design.
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0,
        FSO_BUCKET_TWO_PATH, listKeysResponse.getLastKey(), 3);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(0, listKeysResponse.getKeys().size());
    assertEquals("", listKeysResponse.getLastKey());
  }

  @Test
  public void testListKeysFSOBucketDirTwoPathWithLimitAndPagination() {
    // bucket level keyList
    // Total 2 pages , each page 2 records. If each page we will retrieve 2 items, as total 4 FSO keys,
    // so till we get empty last key, we'll continue to fetch and empty last key signifies the last page.
    // First Page
    Response bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, DIR_TWO_PATH,
        "", 2);
    ListKeysResponse listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(2, listKeysResponse.getKeys().size());
    ReconBasicOmKeyInfo keyEntityInfo = listKeysResponse.getKeys().get(0);
    assertEquals("volume1/fso-bucket/dir1/dir2/file1", keyEntityInfo.getPath());
    assertEquals("/1/10/12/testfile", listKeysResponse.getLastKey());
    assertEquals("RATIS", keyEntityInfo.getReplicationConfig().getReplicationType().toString());

    // Second page
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0,
        DIR_TWO_PATH, listKeysResponse.getLastKey(), 2);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(2, listKeysResponse.getKeys().size());
    keyEntityInfo = listKeysResponse.getKeys().get(0);
    assertEquals("volume1/fso-bucket/dir1/dir2/dir3/file1", keyEntityInfo.getPath());
    assertEquals("/1/10/13/testfile", listKeysResponse.getLastKey());

    // Try again if third page is available. Ideally there should not be any further records
    // and lastKey should be empty as per design.
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0,
        DIR_TWO_PATH, listKeysResponse.getLastKey(), 2);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(0, listKeysResponse.getKeys().size());
    assertEquals("", listKeysResponse.getLastKey());
  }

  @Test
  public void testListKeysFSOBucketDirThreePathWithLimitAndPagination() {
    // bucket level keyList
    // Just 1 page , page will have just 2 records. Total 2 keys
    // so till we get empty last key, we'll continue to fetch and empty last key signifies the last page.
    // First Page
    Response bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, DIR_THREE_PATH,
        "", 2);
    ListKeysResponse listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(2, listKeysResponse.getKeys().size());
    ReconBasicOmKeyInfo keyEntityInfo = listKeysResponse.getKeys().get(0);
    assertEquals("volume1/fso-bucket/dir1/dir2/dir3/file1", keyEntityInfo.getPath());
    assertEquals("/1/10/13/testfile", listKeysResponse.getLastKey());
    assertEquals("RATIS", keyEntityInfo.getReplicationConfig().getReplicationType().toString());

    // Try again if second page is available. Ideally there should not be any further records
    // and lastKey should be empty as per design.
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0,
        DIR_THREE_PATH, listKeysResponse.getLastKey(), 2);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(0, listKeysResponse.getKeys().size());
    assertEquals("", listKeysResponse.getLastKey());
  }

  @Test
  public void testListKeysOBSBucket() throws Exception {
    // List keys under obs-bucket based on RATIS ReplicationConfig
    // creationDate filter and keySize filter both are empty, so only RATIS replication type filter
    // will be applied to return all RATIS keys under obs-bucket.
    Response bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, OBS_BUCKET_PATH,
        "", 1000);
    ListKeysResponse listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    // There are total 6 RATIS keys under this OBS bucket.
    assertEquals(6, listKeysResponse.getKeys().size());
    assertEquals("volume1/obs-bucket/key1", listKeysResponse.getKeys().get(0).getPath());
    assertEquals("/volume1/obs-bucket/key1", listKeysResponse.getKeys().get(0).getKey());
    assertEquals(OBS_BUCKET_PATH + OM_KEY_PREFIX + KEY_SIX, listKeysResponse.getLastKey());

    // Filter listKeys based on key creation date
    // creationDate filter passed 1 minute above of KEY1 creation date, so listKeys API will return
    // only 5 keys excluding Key1, as 5 RATIS keys got created after creationDate filter value.
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "04-04-2024 12:31:00", 0,
        OBS_BUCKET_PATH, "", 1000);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    // There are total 5 RATIS keys under this OBS bucket which were created after "04-04-2024 12:31:00".
    assertEquals(5, listKeysResponse.getKeys().size());
    assertEquals(OBS_BUCKET_PATH + OM_KEY_PREFIX + KEY_SIX, listKeysResponse.getLastKey());

    // creationDate filter passed same as KEY6 creation date, so listKeys API will return all 6 keys
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "04-04-2024 12:30:00", 0,
        OBS_BUCKET_PATH, "", 1000);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    // There are total 6 RATIS keys under this OBS bucket which were created on or after "04-04-2024 12:30:00".
    assertEquals(6, listKeysResponse.getKeys().size());
    assertEquals(OBS_BUCKET_PATH + OM_KEY_PREFIX + KEY_SIX, listKeysResponse.getLastKey());

    // creationDate filter passed as "04-04-2024 12:30:00", but replicationType filter is EC,
    // so listKeys API will return zero keys, because no EC key got created at or after creationDate filter value.
    bucketResponse = omdbInsightEndpoint.listKeys("EC", "04-04-2024 12:30:00", 0,
        OBS_BUCKET_PATH, "", 1000);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    // There are ZERO EC keys created under this OBS bucket which were created on or after "04-04-2024 12:30:00".
    assertEquals(0, listKeysResponse.getKeys().size());
    assertEquals("", listKeysResponse.getLastKey());

    // creationDate filter passed as "04-05-2024 12:30:00", and replicationType filter is RATIS,
    // so listKeys API will return 5 keys, as only 5 RATIS key got created at or after creationDate filter value.
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "04-05-2024 12:30:00", 0,
        OBS_BUCKET_PATH, "", 1000);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(5, listKeysResponse.getKeys().size());
    assertEquals(OBS_BUCKET_PATH + OM_KEY_PREFIX + KEY_SIX, listKeysResponse.getLastKey());

    // creationDate filter passed as "04-05-2024 12:30:00", and replicationType filter is RATIS,
    // so listKeys API should return 5 keys, as only 1 RATIS key got created on or after creationDate filter value.
    // but since keySize filter value is 1026 bytes and 3 RATIS keys created are of size 2025 bytes, and
    // other 3 keys created are of size 1025, so 3 keys will be filtered out of 5 keys, as 1 key will be filtered
    // out due to creationDate filter.
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "04-05-2024 12:30:00", 1026,
        OBS_BUCKET_PATH, "", 1000);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(3, listKeysResponse.getKeys().size());
    assertEquals(OBS_BUCKET_PATH + OM_KEY_PREFIX + KEY_SIX, listKeysResponse.getLastKey());
  }

  @Test
  public void testListKeysOBSBucketWithLimitAndPagination() throws Exception {
    // As per design, client should not change filter values between pages.
    // Below test will fetch multiple pages for same filters in query.

    // Total 3 pages , each page 2 records. If each page we will retrieve 2 items, as total 6 FSO keys,
    // so till we get empty last key, we'll continue to fetch and empty last key signifies the last page.
    // First Page
    Response bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, OBS_BUCKET_PATH,
        "", 2);
    ListKeysResponse listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(2, listKeysResponse.getKeys().size());
    ReconBasicOmKeyInfo keyEntityInfo = listKeysResponse.getKeys().get(0);
    assertEquals("volume1/obs-bucket/key1", keyEntityInfo.getPath());
    assertEquals("/volume1/obs-bucket/key1/key2", listKeysResponse.getLastKey());
    assertEquals("RATIS", keyEntityInfo.getReplicationConfig().getReplicationType().toString());

    // Second page
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0,
        OBS_BUCKET_PATH, listKeysResponse.getLastKey(), 2);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(2, listKeysResponse.getKeys().size());
    keyEntityInfo = listKeysResponse.getKeys().get(0);
    assertEquals("volume1/obs-bucket/key1/key2/key3", keyEntityInfo.getPath());
    assertEquals("/volume1/obs-bucket/key4", listKeysResponse.getLastKey());

    // Third and last page. And last page will have empty
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0,
        OBS_BUCKET_PATH, listKeysResponse.getLastKey(), 2);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(2, listKeysResponse.getKeys().size());
    keyEntityInfo = listKeysResponse.getKeys().get(0);
    assertEquals("volume1/obs-bucket/key5", keyEntityInfo.getPath());
    assertEquals("/volume1/obs-bucket/key6", listKeysResponse.getLastKey());

    // Try again if fourth page is available. Ideally there should not be any further records
    // and lastKey should be empty as per design.
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0,
        OBS_BUCKET_PATH, listKeysResponse.getLastKey(), 2);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(0, listKeysResponse.getKeys().size());
    assertEquals("", listKeysResponse.getLastKey());
  }

  @Test
  public void testListKeysForEmptyOBSBucket() {
    Response bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, EMPTY_OBS_BUCKET_PATH,
        "", 1000);
    ListKeysResponse listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(0, listKeysResponse.getKeys().size());
    assertEquals("", listKeysResponse.getLastKey());
  }

  @Test
  public void testListKeysWhenNSSummaryNotInitialized() throws Exception {
    reconNamespaceSummaryManager.clearNSSummaryTable();
    // bucket level DU
    Response bucketResponse =
        omdbInsightEndpoint.listKeys("RATIS", "", 0, FSO_BUCKET_TWO_PATH,
            "", 1000);
    ListKeysResponse listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(ResponseStatus.OK, listKeysResponse.getStatus());
    assertEquals(Response.Status.OK.getStatusCode(), bucketResponse.getStatus());
  }

  @Test
  public void testListKeysForEmptyFSOBucket() {
    Response bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, EMPTY_FSO_BUCKET_PATH,
        "", 1000);
    ListKeysResponse listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(0, listKeysResponse.getKeys().size());
    assertEquals("", listKeysResponse.getLastKey());
  }

  @Test
  public void testListKeysForNonExistentOBSPaths() {
    Response bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, OBS_BUCKET_PATH +
            OM_KEY_PREFIX + NON_EXISTENT_KEY_SEVEN,
        "", 2);
    String entityResp = (String) bucketResponse.getEntity();
    assertEquals("{\"message\": \"Unexpected runtime error while searching keys in OM DB: Not valid path: " +
        "java.lang.UnsupportedOperationException: Object stores do not support directories.\"}", entityResp);
  }

  @Test
  public void testListKeysForNonExistentFSOPaths() {
    Response bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, NON_EXISTENT_DIR_FOUR_PATH,
        "", 2);
    String entityResp = (String) bucketResponse.getEntity();
    assertEquals("{\"message\": \"Unexpected runtime error while searching keys in OM DB: Not valid path: " +
        "java.lang.IllegalArgumentException: Not valid path\"}", entityResp);
  }

  @Test
  public void testListKeysForNullOrEmptyStartPrefixPath() {
    Response nullStartPrefixResp = omdbInsightEndpoint.listKeys("RATIS", "", 0, null,
        "", 2);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), nullStartPrefixResp.getStatus());

    Response emptyStartPrefixResp = omdbInsightEndpoint.listKeys("RATIS", "", 0, "",
        "", 2);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), emptyStartPrefixResp.getStatus());

    Response invaliStartPrefixResp = omdbInsightEndpoint.listKeys("RATIS", "", 0, "null",
        "", 2);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), invaliStartPrefixResp.getStatus());
  }

  @Test
  public void testListKeysLegacyBucketWithFSEnabled() {
    Response bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, LEGACY_BUCKET_PATH,
        "", 1000);
    ListKeysResponse listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(6, listKeysResponse.getKeys().size());
    assertEquals("/volume1/legacy-bucket/key8", listKeysResponse.getLastKey());
  }

  @Test
  public void testListKeysLegacyBucketWithFSEnabledAndPagination() {
    Response bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, LEGACY_BUCKET_PATH,
        "", 2);
    ListKeysResponse listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(2, listKeysResponse.getKeys().size());
    assertEquals("/volume1/legacy-bucket/dir4/dir6/key10", listKeysResponse.getLastKey());

    // Second page
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, LEGACY_BUCKET_PATH,
        listKeysResponse.getLastKey(), 2);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(2, listKeysResponse.getKeys().size());
    assertEquals("/volume1/legacy-bucket/key11", listKeysResponse.getLastKey());

    // Third page
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, LEGACY_BUCKET_PATH,
        listKeysResponse.getLastKey(), 2);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(2, listKeysResponse.getKeys().size());
    assertEquals("/volume1/legacy-bucket/key8", listKeysResponse.getLastKey());

    // Fourth page should not have any keys left as we have iterated
    // all 6 keys in 3 pages with each page returns 2 keys.
    bucketResponse = omdbInsightEndpoint.listKeys("RATIS", "", 0, LEGACY_BUCKET_PATH,
        listKeysResponse.getLastKey(), 2);
    listKeysResponse = (ListKeysResponse) bucketResponse.getEntity();
    assertEquals(0, listKeysResponse.getKeys().size());
    assertEquals("", listKeysResponse.getLastKey());
  }

  private NSSummary getNsSummary(long size, ReplicationConfig replicationConfig) {
    NSSummary summary = new NSSummary();
    summary.setSizeOfFiles(size);
    summary.setReplicatedSizeOfFiles(QuotaUtil.getReplicatedSize(size, replicationConfig));
    return summary;
  }
}
