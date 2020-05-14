/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.recon;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_CONNECTION_REQUEST_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_CONNECTION_REQUEST_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_CONNECTION_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_CONNECTION_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_SOCKET_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_SOCKET_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl.OmSnapshotTaskName.OmDeltaRequest;
import static org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl.OmSnapshotTaskName.OmSnapshotRequest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;

/**
 * Test Ozone Recon.
 */
public class TestReconWithOzoneManager {
  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static OMMetadataManager metadataManager;
  private static CloseableHttpClient httpClient;
  private static String containerKeyServiceURL;
  private static String taskStatusURL;

  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    int socketTimeout = (int) conf.getTimeDuration(
        RECON_OM_SOCKET_TIMEOUT, RECON_OM_SOCKET_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);
    int connectionTimeout = (int) conf.getTimeDuration(
        RECON_OM_CONNECTION_TIMEOUT,
        RECON_OM_CONNECTION_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
    int connectionRequestTimeout = (int)conf.getTimeDuration(
        RECON_OM_CONNECTION_REQUEST_TIMEOUT,
        RECON_OM_CONNECTION_REQUEST_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
    RequestConfig config = RequestConfig.custom()
        .setConnectTimeout(socketTimeout)
        .setConnectionRequestTimeout(connectionTimeout)
        .setSocketTimeout(connectionRequestTimeout).build();

    cluster =
        MiniOzoneCluster.newBuilder(conf)
            .setNumDatanodes(1)
            .includeRecon(true)
            .build();
    cluster.waitForClusterToBeReady();
    metadataManager = cluster.getOzoneManager().getMetadataManager();

    cluster.getStorageContainerManager().exitSafeMode();

    InetSocketAddress address =
        cluster.getReconServer().getHttpServer().getHttpAddress();
    String reconHTTPAddress = address.getHostName() + ":" + address.getPort();
    containerKeyServiceURL = "http://" + reconHTTPAddress +
        "/api/v1/containers";
    taskStatusURL = "http://" + reconHTTPAddress + "/api/v1/task/status";

    // initialize HTTPClient
    httpClient = HttpClientBuilder
        .create()
        .setDefaultRequestConfig(config)
        .build();
  }

  @AfterClass
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
    OmKeyInfo keyInfo1 = metadataManager.getKeyTable().get(ozoneKey);

    TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
        omKeyValueTableIterator = metadataManager.getKeyTable().iterator();

    long omMetadataKeyCount = getTableKeyCount(omKeyValueTableIterator);

    // verify if OM has /vol0/bucket0/key0
    Assert.assertEquals("vol0", keyInfo1.getVolumeName());
    Assert.assertEquals("bucket0", keyInfo1.getBucketName());

    OzoneManagerServiceProviderImpl impl = (OzoneManagerServiceProviderImpl)
        cluster.getReconServer().getOzoneManagerServiceProvider();
    impl.syncDataFromOM();

    // HTTP call to /api/containers
    String containerResponse = makeHttpCall(containerKeyServiceURL);
    long reconMetadataContainerCount =
        getReconContainerCount(containerResponse);
    // verify count of keys after full snapshot
    Assert.assertEquals(omMetadataKeyCount, reconMetadataContainerCount);

    // verify if Recon Metadata captures vol0/bucket0/key0 info in container0
    LinkedTreeMap containerResponseMap = getContainerResponseMap(
        containerResponse, 0);
    Assert.assertEquals(0,
        (long)(double) containerResponseMap.get("ContainerID"));
    Assert.assertEquals(1,
        (long)(double) containerResponseMap.get("NumberOfKeys"));
    
    // HTTP call to /api/task/status
    long omLatestSeqNumber = ((RDBStore) metadataManager.getStore())
        .getDb().getLatestSequenceNumber();

    String taskStatusResponse = makeHttpCall(taskStatusURL);
    long reconLatestSeqNumber = getReconTaskAttributeFromJson(
        taskStatusResponse,
        OmSnapshotRequest.name(),
        "lastUpdatedSeqNumber");

    // verify sequence number after full snapshot
    Assert.assertEquals(omLatestSeqNumber, reconLatestSeqNumber);

    //add 4 keys to check for delta updates
    addKeys(1, 5);
    omKeyValueTableIterator = metadataManager.getKeyTable().iterator();
    omMetadataKeyCount = getTableKeyCount(omKeyValueTableIterator);

    // update the next snapshot from om to verify delta updates
    impl.syncDataFromOM();

    // HTTP call to /api/containers
    containerResponse = makeHttpCall(containerKeyServiceURL);
    reconMetadataContainerCount = getReconContainerCount(containerResponse);

    //verify count of keys
    Assert.assertEquals(omMetadataKeyCount, reconMetadataContainerCount);

    //verify if Recon Metadata captures vol3/bucket3/key3 info in container3
    containerResponseMap = getContainerResponseMap(
        containerResponse, 3);
    Assert.assertEquals(3,
        (long)(double) containerResponseMap.get("ContainerID"));
    Assert.assertEquals(1,
        (long)(double) containerResponseMap.get("NumberOfKeys"));

    // HTTP call to /api/task/status
    omLatestSeqNumber = ((RDBStore) metadataManager.getStore())
        .getDb().getLatestSequenceNumber();

    taskStatusResponse = makeHttpCall(taskStatusURL);
    reconLatestSeqNumber = getReconTaskAttributeFromJson(
        taskStatusResponse, OmDeltaRequest.name(), "lastUpdatedSeqNumber");

    //verify sequence number after Delta Updates
    Assert.assertEquals(omLatestSeqNumber, reconLatestSeqNumber);

    long beforeRestartSnapShotTimeStamp = getReconTaskAttributeFromJson(
        taskStatusResponse,
        OmSnapshotRequest.name(),
        "lastUpdatedTimestamp");

    //restart Recon
    cluster.restartReconServer();

    impl = (OzoneManagerServiceProviderImpl)
        cluster.getReconServer().getOzoneManagerServiceProvider();

    //add 5 more keys to OM
    addKeys(5, 10);
    omKeyValueTableIterator = metadataManager.getKeyTable().iterator();
    omMetadataKeyCount = getTableKeyCount(omKeyValueTableIterator);

    // get the next snapshot from om
    impl.syncDataFromOM();

    // HTTP call to /api/containers
    containerResponse = makeHttpCall(containerKeyServiceURL);
    reconMetadataContainerCount = getReconContainerCount(containerResponse);

    // verify count of keys
    Assert.assertEquals(omMetadataKeyCount, reconMetadataContainerCount);

    // verify if Recon Metadata captures vol7/bucket7/key7 info in container7
    containerResponseMap = getContainerResponseMap(
        containerResponse, 7);
    Assert.assertEquals(7,
        (long)(double) containerResponseMap.get("ContainerID"));
    Assert.assertEquals(1,
        (long)(double) containerResponseMap.get("NumberOfKeys"));

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
    Assert.assertEquals(beforeRestartSnapShotTimeStamp,
        afterRestartSnapShotTimeStamp);

    //verify sequence number after Delta Updates
    Assert.assertEquals(omLatestSeqNumber, reconLatestSeqNumber);
  }

  private long getReconTaskAttributeFromJson(String taskStatusResponse,
       String taskName, String entityAttribute) {
    ArrayList<LinkedTreeMap> taskStatusList = new Gson()
        .fromJson(taskStatusResponse, ArrayList.class);
    Optional<LinkedTreeMap> taskEntity =
        taskStatusList
            .stream()
            .filter(task -> task.get("taskName").equals(taskName))
            .findFirst();
    Assert.assertTrue(taskEntity.isPresent());
    return (long)(double) taskEntity.get().get(entityAttribute);
  }

  private long getReconContainerCount(String containerResponse) {
    Map map = new Gson().fromJson(containerResponse, HashMap.class);
    LinkedTreeMap linkedTreeMap = (LinkedTreeMap) map.get("data");
    return (long)(double) linkedTreeMap.get("totalCount");
  }

  private LinkedTreeMap getContainerResponseMap(String containerResponse,
      int expectedContainerID) {
    Map map = new Gson().fromJson(containerResponse, HashMap.class);
    LinkedTreeMap linkedTreeMap = (LinkedTreeMap) map.get("data");
    ArrayList containers = (ArrayList) linkedTreeMap.get("containers");
    return (LinkedTreeMap)containers.get(expectedContainerID);
  }

  /**
   * Helper function to add voli/bucketi/keyi to containeri to OM Metadata.
   * For test purpose each container will have only one key.
   */
  private void addKeys(int start, int end) throws Exception {
    for(int i = start; i < end; i++) {
      Pipeline pipeline = getRandomPipeline();
      List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
      BlockID blockID = new BlockID(i, 1);
      OmKeyLocationInfo omKeyLocationInfo1 = getOmKeyLocationInfo(blockID,
          pipeline);
      omKeyLocationInfoList.add(omKeyLocationInfo1);
      OmKeyLocationInfoGroup omKeyLocationInfoGroup = new
          OmKeyLocationInfoGroup(0, omKeyLocationInfoList);
      writeDataToOm("key"+i, "bucket"+i, "vol"+i,
          Collections.singletonList(omKeyLocationInfoGroup));
    }
  }

  private long getTableKeyCount(TableIterator<String, ? extends
      Table.KeyValue<String, OmKeyInfo>> iterator) {
    long keyCount = 0;
    while(iterator.hasNext()) {
      keyCount++;
      iterator.next();
    }
    return keyCount;
  }

  private static Pipeline getRandomPipeline() {
    return Pipeline.newBuilder()
        .setReplication(1)
        .setId(PipelineID.randomId())
        .setNodes(Collections.EMPTY_LIST)
        .setState(Pipeline.PipelineState.OPEN)
        .setType(HddsProtos.ReplicationType.STAND_ALONE)
        .build();
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

    metadataManager.getKeyTable().put(omKey,
        new OmKeyInfo.Builder()
            .setBucketName(bucket)
            .setVolumeName(volume)
            .setKeyName(key)
            .setReplication(1)
            .setReplicationType(HddsProtos.ReplicationType.STAND_ALONE)
            .setOmKeyLocationInfos(omKeyLocationInfoGroupList)
            .build());
  }
}
