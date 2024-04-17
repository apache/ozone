package org.apache.hadoop.ozone.recon;

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
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TestReconContainerEndpoint {

  private static OzoneClient client;
  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static ObjectStore store;

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        OMConfigKeys.OZONE_BUCKET_LAYOUT_FILE_SYSTEM_OPTIMIZED);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .includeRecon(true)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    store = client.getObjectStore();
  }

  @AfterAll
  public static void shutdown() throws IOException {
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
        cluster.getReconServer().getOzoneManagerServiceProvider();
    impl.syncDataFromOM();

    //Search for the bucket from the bucket table and verify its FSO
    String buckKey = "/" + volName + "/" + bucketName;
    OmBucketInfo bucketInfo =
        cluster.getReconServer().getOzoneManagerServiceProvider()
            .getOMMetadataManagerInstance().getBucketTable().get(buckKey);
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
    assertEquals("testvol/testbucket/dir1/dir2/dir3/file1",
        keyMetadata.getCompletePath());

    testContainerID = 2L;
    response = getContainerEndpointResponse(testContainerID);
    data = (KeysResponse) response.getEntity();
    keyMetadataList = data.getKeys();
    assertEquals(1, data.getTotalCount());
    assertEquals(1, keyMetadataList.size());

    // Assert the file name and the complete path.
    keyMetadata = keyMetadataList.iterator().next();
    assertEquals("file1", keyMetadata.getKey());
    assertEquals("testvol/testbucket/file1", keyMetadata.getCompletePath());
  }

  @Test
  public void testContainerEndpointForOBSBucket() throws Exception {
    String volumeName = "testvol";
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
        (OzoneManagerServiceProviderImpl) cluster.getReconServer()
            .getOzoneManagerServiceProvider();
    impl.syncDataFromOM();

    // Search for the bucket from the bucket table and verify its OBS
    String bucketKey = "/" + volumeName + "/" + obsBucketName;
    OmBucketInfo bucketInfo =
        cluster.getReconServer().getOzoneManagerServiceProvider()
            .getOMMetadataManagerInstance().getBucketTable().get(bucketKey);
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
    assertEquals("testvol/obsbucket/file1", keyMetadata.getCompletePath());
  }

  private Response getContainerEndpointResponse(long containerId) {
    OzoneStorageContainerManager reconSCM =
        cluster.getReconServer().getReconStorageContainerManager();
    ReconContainerManager reconContainerManager =
        (ReconContainerManager) reconSCM.getContainerManager();
    ContainerHealthSchemaManager containerHealthSchemaManager =
        reconContainerManager.getContainerSchemaManager();
    ReconOMMetadataManager omMetadataManagerInstance =
        (ReconOMMetadataManager)
            cluster.getReconServer().getOzoneManagerServiceProvider()
                .getOMMetadataManagerInstance();
    ContainerEndpoint containerEndpoint =
        new ContainerEndpoint(reconSCM, containerHealthSchemaManager,
            cluster.getReconServer().getReconNamespaceSummaryManager(),
            cluster.getReconServer().getReconContainerMetadataManager(),
            omMetadataManagerInstance);
    return containerEndpoint.getKeysForContainer(containerId, 10, "");
  }

  private void writeTestData(String volumeName, String bucketName,
                             String keyPath, String data) throws Exception {
    try (OzoneOutputStream out = client.getObjectStore().getVolume(volumeName)
        .getBucket(bucketName)
        .createKey(keyPath, data.length())) {
      out.write(data.getBytes());
    }
  }

}
