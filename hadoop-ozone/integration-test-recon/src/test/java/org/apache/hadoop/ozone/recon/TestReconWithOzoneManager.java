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

package org.apache.hadoop.ozone.recon;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_CONNECTION_REQUEST_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_CONNECTION_REQUEST_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_CONNECTION_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_CONNECTION_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SOCKET_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SOCKET_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_DELTA_UPDATE_LIMIT;
import static org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl.OmSnapshotTaskName.OmDeltaRequest;
import static org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl.OmSnapshotTaskName.OmSnapshotRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.slf4j.event.Level.INFO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.JsonTestUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.metrics.OzoneManagerSyncMetrics;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test Ozone Recon.
 */
public class TestReconWithOzoneManager {
  private static MiniOzoneCluster cluster = null;
  private static OMMetadataManager metadataManager;
  private static CloseableHttpClient httpClient;
  private static String taskStatusURL;
  private static ReconService recon;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    int socketTimeout = (int) conf.getTimeDuration(
        OZONE_RECON_OM_SOCKET_TIMEOUT,
        conf.get(
            ReconServerConfigKeys.RECON_OM_SOCKET_TIMEOUT,
            OZONE_RECON_OM_SOCKET_TIMEOUT_DEFAULT),
        TimeUnit.MILLISECONDS);
    int connectionTimeout = (int) conf.getTimeDuration(
        OZONE_RECON_OM_CONNECTION_TIMEOUT,
        conf.get(
            ReconServerConfigKeys.RECON_OM_CONNECTION_TIMEOUT,
            OZONE_RECON_OM_CONNECTION_TIMEOUT_DEFAULT),
        TimeUnit.MILLISECONDS);
    int connectionRequestTimeout = (int) conf.getTimeDuration(
        OZONE_RECON_OM_CONNECTION_REQUEST_TIMEOUT,
        conf.get(
            ReconServerConfigKeys.RECON_OM_CONNECTION_REQUEST_TIMEOUT,
            OZONE_RECON_OM_CONNECTION_REQUEST_TIMEOUT_DEFAULT),
        TimeUnit.MILLISECONDS
    );
    conf.setLong(RECON_OM_DELTA_UPDATE_LIMIT, 10);

    RequestConfig config = RequestConfig.custom()
        .setConnectTimeout(socketTimeout)
        .setConnectionRequestTimeout(connectionTimeout)
        .setSocketTimeout(connectionRequestTimeout).build();

    recon = new ReconService(conf);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .addService(recon)
        .build();
    cluster.waitForClusterToBeReady();
    metadataManager = cluster.getOzoneManager().getMetadataManager();

    cluster.getStorageContainerManager().exitSafeMode();

    String reconHTTPAddress = conf.get(OZONE_RECON_HTTP_ADDRESS_KEY);
    taskStatusURL = "http://" + reconHTTPAddress + "/api/v1/task/status";

    // initialize HTTPClient
    httpClient = HttpClientBuilder
        .create()
        .setDefaultRequestConfig(config)
        .build();
  }

  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Returns a {@link CloseableHttpClient} configured by given configuration.
   * If conf is null, returns a default instance.
   *
   * @param url        URL
   * @return a JSON String Response.
   */
  private String makeHttpCall(String url)
      throws IOException {
    HttpGet httpGet = new HttpGet(url);
    HttpResponse response = httpClient.execute(httpGet);
    int errorCode = response.getStatusLine().getStatusCode();
    HttpEntity entity = response.getEntity();

    if ((errorCode == HTTP_OK) || (errorCode == HTTP_CREATED)) {
      return EntityUtils.toString(entity);
    }

    if (entity != null) {
      throw new IOException("Unexpected exception when trying to reach " +
          "Recon Server, " + EntityUtils.toString(entity));
    } else {
      throw new IOException("Unexpected null in http payload," +
          " while processing request");
    }
  }

  @Test
  public void testOmDBSyncing() throws Exception {
    // add a vol, bucket and key
    addKeys(0, 1);

    // check if OM metadata has vol0/bucket0/key0 info
    String ozoneKey = metadataManager.getOzoneKey(
        "vol0", "bucket0", "key0");
    OmKeyInfo keyInfo1 =
        metadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);

    // verify if OM has /vol0/bucket0/key0
    assertEquals("vol0", keyInfo1.getVolumeName());
    assertEquals("bucket0", keyInfo1.getBucketName());

    OzoneManagerServiceProviderImpl impl = (OzoneManagerServiceProviderImpl)
        recon.getReconServer().getOzoneManagerServiceProvider();
    impl.syncDataFromOM();
    OzoneManagerSyncMetrics metrics = impl.getMetrics();

    // HTTP call to /api/task/status
    long omLatestSeqNumber = ((RDBStore) metadataManager.getStore())
        .getDb().getLatestSequenceNumber();

    String taskStatusResponse = makeHttpCall(taskStatusURL);
    long reconLatestSeqNumber = getReconTaskAttributeFromJson(
        taskStatusResponse,
        OmSnapshotRequest.name(),
        "lastUpdatedSeqNumber");

    // verify sequence number after full snapshot
    assertEquals(omLatestSeqNumber, reconLatestSeqNumber);
    assertEquals(0, metrics.getSequenceNumberLag());

    //add 4 keys to check for delta updates
    addKeys(1, 5);

    // update the next snapshot from om to verify delta updates
    impl.syncDataFromOM();

    // HTTP call to /api/task/status
    omLatestSeqNumber = ((RDBStore) metadataManager.getStore())
        .getDb().getLatestSequenceNumber();

    taskStatusResponse = makeHttpCall(taskStatusURL);
    reconLatestSeqNumber = getReconTaskAttributeFromJson(
        taskStatusResponse, OmDeltaRequest.name(), "lastUpdatedSeqNumber");

    //verify sequence number after Delta Updates
    assertEquals(omLatestSeqNumber, reconLatestSeqNumber);
    assertEquals(0, metrics.getSequenceNumberLag());

    long beforeRestartSnapShotTimeStamp = getReconTaskAttributeFromJson(
        taskStatusResponse,
        OmSnapshotRequest.name(),
        "lastUpdatedTimestamp");

    //restart Recon
    recon.stop();
    recon.start(cluster.getConf());

    impl = (OzoneManagerServiceProviderImpl)
        recon.getReconServer().getOzoneManagerServiceProvider();

    //add 5 more keys to OM
    addKeys(5, 10);

    // get the next snapshot from om
    impl.syncDataFromOM();

    // HTTP call to /api/task/status
    omLatestSeqNumber = ((RDBStore) metadataManager.getStore())
        .getDb().getLatestSequenceNumber();

    taskStatusResponse = makeHttpCall(taskStatusURL);
    reconLatestSeqNumber = getReconTaskAttributeFromJson(
        taskStatusResponse,
        OmDeltaRequest.name(),
        "lastUpdatedSeqNumber");

    long afterRestartSnapShotTimeStamp =
        getReconTaskAttributeFromJson(taskStatusResponse,
            OmSnapshotRequest.name(),
            "lastUpdatedTimestamp");

    // verify only Delta updates were added to recon after restart.
    assertThat(afterRestartSnapShotTimeStamp).isGreaterThanOrEqualTo(beforeRestartSnapShotTimeStamp);

    //verify sequence number after Delta Updates
    assertEquals(omLatestSeqNumber, reconLatestSeqNumber);
    assertEquals(0, metrics.getSequenceNumberLag());
  }

  // This test simulates the mis-match in sequence number between Recon OM
  // snapshot DB and OzoneManager OM active DB. Recon sync with OM DB every
  // 5-10 mins based on configured interval and if Recon OM snapshot DB
  // has higher sequence number than OM active DB last sequence number,
  // then OM DB updates call since last sequence number will fail as OM DB
  // will not know about that sequence number which is not yet written to
  // OM Rocks DB. In such case OM sets the 'isDBUpdateSuccess' flag as false.
  // And Recon should fall back on full snapshot and recover itself.
  @Test
  public void testOmDBSyncWithSeqNumberMismatch() throws Exception {
    LogCapturer logs = LogCapturer.captureLogs(RDBStore.class);
    GenericTestUtils.setLogLevel(RDBStore.class, INFO);

    LogCapturer omServiceProviderImplLogs = LogCapturer.captureLogs(OzoneManagerServiceProviderImpl.class);
    GenericTestUtils.setLogLevel(OzoneManagerServiceProviderImpl.class, INFO);

    // add a vol, bucket and key
    addKeys(10, 15);

    // check if OM metadata has vol10/bucket10/key10 info
    String ozoneKey = metadataManager.getOzoneKey(
        "vol10", "bucket10", "key10");
    OmKeyInfo keyInfo1 =
        metadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);

    // verify if OM has /vol10/bucket10/key10
    assertEquals("vol10", keyInfo1.getVolumeName());
    assertEquals("bucket10", keyInfo1.getBucketName());

    OzoneManagerServiceProviderImpl impl = (OzoneManagerServiceProviderImpl)
        recon.getReconServer().getOzoneManagerServiceProvider();
    impl.syncDataFromOM();
    OzoneManagerSyncMetrics metrics = impl.getMetrics();

    // HTTP call to /api/task/status
    long omLatestSeqNumber = ((RDBStore) metadataManager.getStore())
        .getDb().getLatestSequenceNumber();

    OMMetadataManager reconMetadataManagerInstance =
        recon.getReconServer().getOzoneManagerServiceProvider()
            .getOMMetadataManagerInstance();
    long reconLatestSeqNumber =
        ((RDBStore) reconMetadataManagerInstance.getStore()).getDb()
            .getLatestSequenceNumber();

    // verify sequence number after incremental delta snapshot
    assertEquals(omLatestSeqNumber, reconLatestSeqNumber);
    assertEquals(0, metrics.getSequenceNumberLag());

    String volume = "vol15";
    String bucket = "bucket15";
    String key = "key15";
    String omKey = reconMetadataManagerInstance.getOzoneKey(volume,
        bucket, key);

    // Write additional key in new volume and new bucket in Recon OM snapshot
    // DB to increase the last sequence number by 1 in comparison to OM
    // active DB, this simulates the mis-match between 2 DBs before triggering
    // syncDataFromOM call.
    reconMetadataManagerInstance.getKeyTable(getBucketLayout()).put(omKey,
        new OmKeyInfo.Builder()
            .setBucketName(bucket)
            .setVolumeName(volume)
            .setKeyName(key)
            .setReplicationConfig(StandaloneReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.ONE))
            .setOmKeyLocationInfos(
                Collections.singletonList(getOmKeyLocationInfoGroup()))
            .build());

    reconLatestSeqNumber =
        ((RDBStore) reconMetadataManagerInstance.getStore()).getDb()
            .getLatestSequenceNumber();
    assertEquals(omLatestSeqNumber + 1, reconLatestSeqNumber);

    // This OM DB sync call will eventually fail and throw exception at OM
    // cluster's getDBUpdates(since lastSequenceNumber) API as expected and
    // will return 'isDBUpdateSuccess' flag as false which will force Recon
    // to fallback on full snapshot. Full snapshot sync should normalize the
    // OM active DB lastSequence number and Recon's OM snapshot DB lastSequence
    // number.
    impl.syncDataFromOM();
    assertTrue(logs.getOutput()
        .contains("Requested sequence not yet written in the db"));
    assertTrue(logs.getOutput()
        .contains("Returned DBUpdates isDBUpdateSuccess: false"));
    reconLatestSeqNumber =
        ((RDBStore) reconMetadataManagerInstance.getStore()).getDb()
            .getLatestSequenceNumber();
    assertEquals(0, metrics.getSequenceNumberLag());
    assertEquals(omLatestSeqNumber, reconLatestSeqNumber);
    reconLatestSeqNumber =
        ((RDBStore) reconMetadataManagerInstance.getStore()).getDb()
            .getLatestSequenceNumber();
    assertEquals(omLatestSeqNumber, reconLatestSeqNumber);
    impl.syncDataFromOM();
    assertTrue(omServiceProviderImplLogs.getOutput()
        .contains("isDBUpdateSuccess: true"));
  }

  private static OmKeyLocationInfoGroup getOmKeyLocationInfoGroup() {
    Pipeline pipeline = HddsTestUtils.getRandomPipeline();
    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
    BlockID blockID = new BlockID(15, 1);
    OmKeyLocationInfo omKeyLocationInfo1 = getOmKeyLocationInfo(blockID,
        pipeline);
    omKeyLocationInfoList.add(omKeyLocationInfo1);
    OmKeyLocationInfoGroup omKeyLocationInfoGroup = new
        OmKeyLocationInfoGroup(0, omKeyLocationInfoList);
    return omKeyLocationInfoGroup;
  }

  private long getReconTaskAttributeFromJson(String taskStatusResponse,
                                             String taskName,
                                             String entityAttribute)
      throws IOException {
    List<Map<String, Object>> taskStatusList =
        JsonTestUtils.readTreeAsListOfMaps(taskStatusResponse);

    // Stream through the list to find the task entity matching the taskName
    Optional<Map<String, Object>> taskEntity = taskStatusList.stream()
        .filter(task -> taskName.equals(task.get("taskName")))
        .findFirst();

    if (taskEntity.isPresent()) {
      Number number = (Number) taskEntity.get().get(entityAttribute);
      return number.longValue();
    } else {
      throw new IOException(
          "Task entity for task name " + taskName + " not found");
    }
  }

  /**
   * Helper function to add voli/bucketi/keyi to containeri to OM Metadata.
   * For test purpose each container will have only one key.
   */
  private void addKeys(int start, int end) throws Exception {
    for (int i = start; i < end; i++) {
      Pipeline pipeline = HddsTestUtils.getRandomPipeline();
      List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
      BlockID blockID = new BlockID(i, 1);
      OmKeyLocationInfo omKeyLocationInfo1 = getOmKeyLocationInfo(blockID,
          pipeline);
      omKeyLocationInfoList.add(omKeyLocationInfo1);
      OmKeyLocationInfoGroup omKeyLocationInfoGroup = new
          OmKeyLocationInfoGroup(0, omKeyLocationInfoList);
      writeDataToOm("key" + i, "bucket" + i, "vol" + i,
          Collections.singletonList(omKeyLocationInfoGroup));
    }
  }

  private static OmKeyLocationInfo getOmKeyLocationInfo(BlockID blockID,
                                                        Pipeline pipeline) {
    return new OmKeyLocationInfo.Builder()
        .setBlockID(blockID)
        .setPipeline(pipeline)
        .build();
  }

  private static void writeDataToOm(String key, String bucket, String volume,
                                    List<OmKeyLocationInfoGroup>
                                        omKeyLocationInfoGroupList)
      throws IOException {

    String omKey = metadataManager.getOzoneKey(volume,
        bucket, key);

    metadataManager.getKeyTable(getBucketLayout()).put(omKey,
        new OmKeyInfo.Builder()
            .setBucketName(bucket)
            .setVolumeName(volume)
            .setKeyName(key)
            .setReplicationConfig(StandaloneReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.ONE))
            .setOmKeyLocationInfos(omKeyLocationInfoGroupList)
            .build());
  }

  private static BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }
}
