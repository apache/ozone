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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_CONNECTION_REQUEST_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_CONNECTION_REQUEST_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_CONNECTION_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_CONNECTION_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SOCKET_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SOCKET_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl.OmSnapshotTaskName.OmSnapshotRequest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.junit.Rule;
import org.junit.rules.Timeout;

/**
 * Test Ozone Recon.
 */
public class TestReconWithOzoneManagerFSO {

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = Timeout.seconds(300);
  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static OMMetadataManager metadataManager;
  private static CloseableHttpClient httpClient;
  private static String nssummaryServiceURL;
  private static String taskStatusURL;
  private static ObjectStore store;
  private static String basicEndpoint;

  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
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
    int connectionRequestTimeout = (int)conf.getTimeDuration(
            OZONE_RECON_OM_CONNECTION_REQUEST_TIMEOUT,
            conf.get(
                    ReconServerConfigKeys.RECON_OM_CONNECTION_REQUEST_TIMEOUT,
                    OZONE_RECON_OM_CONNECTION_REQUEST_TIMEOUT_DEFAULT),
            TimeUnit.MILLISECONDS
    );
    conf.setBoolean("ozone.om.enable.filesystem.paths", true);
    conf.set("ozone.om.metadata.layout", "PREFIX");

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
    nssummaryServiceURL = "http://" + reconHTTPAddress +
            "/api/v1/nssummary";
    taskStatusURL = "http://" + reconHTTPAddress + "/api/v1/task/status";

    basicEndpoint = "/basic";

    store = cluster.getClient().getObjectStore();
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
  private String makeHttpCall(String url, String pathRequest)
          throws IOException, URISyntaxException {
    HttpGet httpGet = new HttpGet(url);
    if (pathRequest != null) {
      URI uri = new URIBuilder(httpGet.getURI())
              .addParameter("path", pathRequest).build();
      httpGet.setURI(uri);
    }

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

  private void writeTestData(String volumeName,
                             String bucketName,
                             String keyName) throws Exception {

    String keyString = UUID.randomUUID().toString();
    byte[] data = ContainerTestHelper.getFixedLengthString(
            keyString, 100).getBytes(UTF_8);
    OzoneOutputStream keyStream = TestHelper.createKey(
            keyName, ReplicationType.STAND_ALONE, ReplicationFactor.ONE,
            100, store, volumeName, bucketName);
    keyStream.write(data);
    keyStream.close();
  }

  private void writeKeys(String vol, String bucket, String key)
          throws Exception {
    store.createVolume(vol);
    OzoneVolume volume = store.getVolume(vol);
    volume.createBucket(bucket);
    writeTestData(vol, bucket, key);
  }

  @Test
  public void testOmDBSyncing() throws Exception {
    // add a vol, bucket and key
    addKeys(0, 1);

    OzoneManagerServiceProviderImpl impl = (OzoneManagerServiceProviderImpl)
            cluster.getReconServer().getOzoneManagerServiceProvider();
    impl.syncDataFromOM();

    // check if OM metadata has vol0/bucket0/key0 info
    String bucketKey = metadataManager.getBucketKey("vol0", "bucket0");
    long bucketId = metadataManager.getBucketTable()
            .get(bucketKey).getObjectID();

    String ozoneKey = metadataManager.getOzonePathKey(bucketId, "key0");
    OmKeyInfo keyInfo1 = metadataManager.getKeyTable().get(ozoneKey);

    TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
            omKeyValueTableIterator = metadataManager.getKeyTable().iterator();

    long omMetadataKeyCount = getTableKeyCount(omKeyValueTableIterator);
    Assert.assertEquals(1, omMetadataKeyCount);

    // verify if OM has /vol0/bucket0/key0
    Assert.assertEquals("vol0", keyInfo1.getVolumeName());
    Assert.assertEquals("bucket0", keyInfo1.getBucketName());

    // HTTP call to /api/nssummary
    String basicNSSummaryResponse = makeHttpCall(
            nssummaryServiceURL + basicEndpoint, "/vol0");
    Map basicNSSummaryMap =
            getReconNSSummary(basicNSSummaryResponse);

    String entityType = (String) basicNSSummaryMap.get("type");
    Assert.assertEquals(EntityType.VOLUME.toString(), entityType);

    // HTTP call to /api/task/status
    long omLatestSeqNumber = ((RDBStore) metadataManager.getStore())
            .getDb().getLatestSequenceNumber();

    String taskStatusResponse = makeHttpCall(taskStatusURL, null);
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
    Assert.assertEquals(4, omMetadataKeyCount);

    String bucketKey3 = metadataManager.getBucketKey("vol3", "bucket3");
    long bucketId3 = metadataManager.getBucketTable()
            .get(bucketKey3).getObjectID();
    String ozoneKey3 = metadataManager.getOzonePathKey(bucketId3, "key3");
    OmKeyInfo keyInfo3 = metadataManager.getKeyTable().get(ozoneKey3);
    Assert.assertEquals("vol3", keyInfo3.getVolumeName());
    Assert.assertEquals("bucket3", keyInfo3.getBucketName());

    // update the next snapshot from om to verify delta updates
    impl.syncDataFromOM();

    String basicNSSummaryResponse2 = makeHttpCall(
            nssummaryServiceURL + basicEndpoint, "/vol3/bucket3");
    Map basicNSSummaryMap2 = getReconNSSummary(basicNSSummaryResponse2);
    Assert.assertEquals(EntityType.BUCKET.toString(),
            basicNSSummaryMap2.get("type"));
    Assert.assertEquals(1.0, basicNSSummaryMap2.get("key"));
  }

  private long getReconTaskAttributeFromJson(String taskStatusResponse,
                                             String taskName,
                                             String entityAttribute) {
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

  private Map getReconNSSummary(String nssummaryResponse) {
    return new Gson().fromJson(nssummaryResponse, HashMap.class);
  }

  /**
   * Helper function to add voli/bucketi/keyi to containeri to OM Metadata.
   * For test purpose each container will have only one key.
   */
  private void addKeys(int start, int end) throws Exception {
    for(int i = start; i < end; i++) {
      writeKeys("vol"+i, "bucket"+i, "key"+i);
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
}

