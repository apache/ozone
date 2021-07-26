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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
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
  private static CloseableHttpClient httpClient;
  private static String nssummaryServiceURL;
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

    cluster.getStorageContainerManager().exitSafeMode();

    InetSocketAddress address =
            cluster.getReconServer().getHttpServer().getHttpAddress();
    String reconHTTPAddress = address.getHostName() + ":" + address.getPort();
    nssummaryServiceURL = "http://" + reconHTTPAddress +
            "/api/v1/namespace";

    basicEndpoint = "/summary";

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

    // HTTP call to /api/namespace
    String basicNSSummaryResponse = makeHttpCall(
            nssummaryServiceURL + basicEndpoint, "/vol0");
    Map basicNSSummaryMap =
            getReconNSSummary(basicNSSummaryResponse);

    String entityType = (String) basicNSSummaryMap.get("type");
    Assert.assertEquals(EntityType.VOLUME.toString(), entityType);

    //add 4 keys to check for delta updates
    addKeys(1, 5);
    // update the next snapshot from om to verify delta updates
    impl.syncDataFromOM();

    String basicNSSummaryResponse2 = makeHttpCall(
            nssummaryServiceURL + basicEndpoint, "/vol3/bucket3");
    Map basicNSSummaryMap2 = getReconNSSummary(basicNSSummaryResponse2);
    Assert.assertEquals(EntityType.BUCKET.toString(),
            basicNSSummaryMap2.get("type"));
    Assert.assertEquals(1.0, basicNSSummaryMap2.get("key"));
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
}
