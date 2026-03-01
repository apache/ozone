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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.recon.api.ContainerEndpoint;
import org.apache.hadoop.ozone.recon.api.types.KeyMetadata;
import org.apache.hadoop.ozone.recon.api.types.KeysResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskControllerImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test fo recon container endpoint.
 */
public class TestReconContainerEndpoint {

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private ObjectStore store;
  private ReconService recon;
  private TestReconOmMetaManagerUtils omMetaManagerUtils = new TestReconOmMetaManagerUtils();

  @BeforeEach
  public void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        OMConfigKeys.OZONE_BUCKET_LAYOUT_FILE_SYSTEM_OPTIMIZED);
    // Configure multiple task threads for concurrent task execution
    conf.setInt("ozone.recon.task.thread.count", 6);
    recon = new ReconService(conf);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .addService(recon)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    store = client.getObjectStore();
  }

  @AfterEach
  public void shutdown() throws IOException {
    if (client != null) {
      client.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testContainerEndpointForFSOLayout() throws Exception {
    // Setup: Create multiple volumes, buckets, and key hierarchies
    String volName = "testvol";
    String bucketName = "fsobucket";
    // Scenario 1: Deeply nested directories
    String nestedDirKey = "dir1/dir2/dir3/file1";
    // Scenario 2: Single file in a bucket
    String singleFileKey = "file1";

    // Create volume and bucket
    store.createVolume(volName);
    OzoneVolume volume = store.getVolume(volName);
    volume.createBucket(bucketName, BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED).build());

    // Write keys to the bucket
    writeTestData(volName, bucketName, nestedDirKey, "data1");
    writeTestData(volName, bucketName, singleFileKey, "data2");

    // Synchronize data from OM to Recon
    OzoneManagerServiceProviderImpl impl = (OzoneManagerServiceProviderImpl)
        recon.getReconServer().getOzoneManagerServiceProvider();
    impl.syncDataFromOM();

    // Wait for async event processing to complete
    // Events are processed asynchronously, so wait for processing to finish
    ReconTaskControllerImpl reconTaskController =
        (ReconTaskControllerImpl) recon.getReconServer().getReconTaskController();
    CompletableFuture<Void> completableFuture =
        omMetaManagerUtils.waitForEventBufferEmpty(reconTaskController.getEventBuffer());
    GenericTestUtils.waitFor(completableFuture::isDone, 100, 30000);

    //Search for the bucket from the bucket table and verify its FSO
    OmBucketInfo bucketInfo = cluster.getOzoneManager().getBucketInfo(volName, bucketName);
    assertNotNull(bucketInfo);
    assertEquals(BucketLayout.FILE_SYSTEM_OPTIMIZED,
        bucketInfo.getBucketLayout());

    // Assuming a known container ID that these keys have been written into
    long testContainerID = 1L;

    // Query the ContainerEndpoint for the keys in the specified container
    Response response = getContainerEndpointResponse(testContainerID);

    assertNotNull(response, "Response should not be null.");
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus(),
        "Expected HTTP 200 OK response.");

    KeysResponse data = (KeysResponse) response.getEntity();
    Collection<KeyMetadata> keyMetadataList = data.getKeys();

    assertEquals(1, data.getTotalCount());
    assertEquals(1, keyMetadataList.size());

    // Assert the file name and the complete path.
    KeyMetadata keyMetadata = keyMetadataList.iterator().next();
    assertEquals("file1", keyMetadata.getKey());
    assertEquals("testvol/fsobucket/dir1/dir2/dir3/file1", keyMetadata.getCompletePath());

    testContainerID = 2L;
    response = getContainerEndpointResponse(testContainerID);
    data = (KeysResponse) response.getEntity();
    keyMetadataList = data.getKeys();
    assertEquals(1, data.getTotalCount());
    assertEquals(1, keyMetadataList.size());

    // Assert the file name and the complete path.
    keyMetadata = keyMetadataList.iterator().next();
    assertEquals("file1", keyMetadata.getKey());
    assertEquals("testvol/fsobucket/file1", keyMetadata.getCompletePath());
  }

  @Test
  public void testContainerEndpointForOBSBucket() throws Exception {
    String volumeName = "testvol2";
    String obsBucketName = "obsbucket";
    String obsSingleFileKey = "file1";

    // Setup volume and OBS bucket
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(obsBucketName,
        BucketArgs.newBuilder().setBucketLayout(BucketLayout.OBJECT_STORE)
            .build());

    // Write a single file to the OBS bucket
    writeTestData(volumeName, obsBucketName, obsSingleFileKey, "Hello OBS!");

    OzoneManagerServiceProviderImpl impl =
        (OzoneManagerServiceProviderImpl) recon.getReconServer()
            .getOzoneManagerServiceProvider();
    impl.syncDataFromOM();

    // Wait for async event processing to complete
    // Events are processed asynchronously, so wait for processing to finish
    ReconTaskControllerImpl reconTaskController =
        (ReconTaskControllerImpl) recon.getReconServer().getReconTaskController();
    CompletableFuture<Void> completableFuture =
        omMetaManagerUtils.waitForEventBufferEmpty(reconTaskController.getEventBuffer());
    GenericTestUtils.waitFor(completableFuture::isDone, 100, 30000);

    // Search for the bucket from the bucket table and verify its OBS
    OmBucketInfo bucketInfo = cluster.getOzoneManager().getBucketInfo(volumeName, obsBucketName);
    assertNotNull(bucketInfo);
    assertEquals(BucketLayout.OBJECT_STORE, bucketInfo.getBucketLayout());

    // Initialize the ContainerEndpoint
    long containerId = 1L;
    Response response = getContainerEndpointResponse(containerId);

    assertNotNull(response, "Response should not be null.");
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus(),
        "Expected HTTP 200 OK response.");
    KeysResponse data = (KeysResponse) response.getEntity();
    Collection<KeyMetadata> keyMetadataList = data.getKeys();

    assertEquals(1, data.getTotalCount());
    assertEquals(1, keyMetadataList.size());

    KeyMetadata keyMetadata = keyMetadataList.iterator().next();
    assertEquals("file1", keyMetadata.getKey());
    assertEquals("testvol2/obsbucket/file1", keyMetadata.getCompletePath());
  }

  private Response getContainerEndpointResponse(long containerId) {
    OzoneStorageContainerManager reconSCM =
        recon.getReconServer().getReconStorageContainerManager();
    ReconOMMetadataManager omMetadataManagerInstance =
        (ReconOMMetadataManager)
            recon.getReconServer().getOzoneManagerServiceProvider()
                .getOMMetadataManagerInstance();
    ContainerEndpoint containerEndpoint =
        new ContainerEndpoint(reconSCM,
            null, // ContainerHealthSchemaManagerV2 - not needed for this test
            recon.getReconServer().getReconNamespaceSummaryManager(),
            recon.getReconServer().getReconContainerMetadataManager(),
            omMetadataManagerInstance);
    return containerEndpoint.getKeysForContainer(containerId, 10, "");
  }

  private void writeTestData(String volumeName, String bucketName,
                             String keyPath, String data) throws Exception {
    try (OzoneOutputStream out = client.getObjectStore().getVolume(volumeName)
        .getBucket(bucketName)
        .createKey(keyPath, data.length())) {
      out.write(data.getBytes(StandardCharsets.UTF_8));
    }
  }

}
