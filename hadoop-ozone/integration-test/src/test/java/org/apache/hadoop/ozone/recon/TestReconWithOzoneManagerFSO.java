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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.UUID;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.recon.api.NSSummaryEndpoint;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.junit.Rule;
import org.junit.rules.Timeout;

import javax.ws.rs.core.Response;

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
  private static ObjectStore store;

  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        OMConfigKeys.OZONE_BUCKET_LAYOUT_FILE_SYSTEM_OPTIMIZED);
    cluster =
            MiniOzoneCluster.newBuilder(conf)
                    .setNumDatanodes(1)
                    .includeRecon(true)
                    .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.ONE, 30000);

    store = cluster.getClient().getObjectStore();
  }

  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void writeTestData(String volumeName,
                             String bucketName,
                             String keyName) throws Exception {

    String keyString = UUID.randomUUID().toString();
    byte[] data = ContainerTestHelper.getFixedLengthString(
            keyString, 100).getBytes(UTF_8);
    OzoneOutputStream keyStream = TestHelper.createKey(
            keyName, ReplicationType.RATIS, ReplicationFactor.ONE,
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
  public void testNamespaceSummaryAPI() throws Exception {
    // add a vol, bucket and key
    addKeys(0, 10, "dir");
    OzoneManagerServiceProviderImpl impl = (OzoneManagerServiceProviderImpl)
            cluster.getReconServer().getOzoneManagerServiceProvider();
    impl.syncDataFromOM();
    ReconNamespaceSummaryManager namespaceSummaryManager =
            cluster.getReconServer().getReconNamespaceSummaryManager();
    ReconOMMetadataManager omMetadataManagerInstance =
            (ReconOMMetadataManager)
                    cluster.getReconServer().getOzoneManagerServiceProvider()
                            .getOMMetadataManagerInstance();
    OzoneStorageContainerManager reconSCM =
            cluster.getReconServer().getReconStorageContainerManager();
    NSSummaryEndpoint endpoint = new NSSummaryEndpoint(namespaceSummaryManager,
            omMetadataManagerInstance, reconSCM);
    Response basicInfo = endpoint.getBasicInfo("/vol1/bucket1/dir1");
    NamespaceSummaryResponse entity =
            (NamespaceSummaryResponse) basicInfo.getEntity();
    Assert.assertSame(entity.getEntityType(), EntityType.DIRECTORY);
    Assert.assertEquals(1, entity.getNumTotalKey());
    Assert.assertEquals(0, entity.getNumTotalDir());
    for (int i = 0; i < 10; i++) {
      Assert.assertNotNull(impl.getOMMetadataManagerInstance()
              .getVolumeTable().get("/vol" + i));
    }
    addKeys(10, 12, "dir");
    impl.syncDataFromOM();

    // test Recon is sync'ed with OM.
    for (int i = 10; i < 12; i++) {
      Assert.assertNotNull(impl.getOMMetadataManagerInstance()
              .getVolumeTable().getSkipCache("/vol" + i));
    }

    // test root response
    Response rootBasicRes = endpoint.getBasicInfo("/");
    NamespaceSummaryResponse rootBasicEntity =
            (NamespaceSummaryResponse) rootBasicRes.getEntity();
    Assert.assertSame(EntityType.ROOT, rootBasicEntity.getEntityType());
    // one additional dummy volume at creation
    Assert.assertEquals(13, rootBasicEntity.getNumVolume());
    Assert.assertEquals(12, rootBasicEntity.getNumBucket());
    Assert.assertEquals(12, rootBasicEntity.getNumTotalDir());
    Assert.assertEquals(12, rootBasicEntity.getNumTotalKey());
  }

  /**
   * Helper function to add voli/bucketi/keyi to containeri to OM Metadata.
   * For test purpose each container will have only one key.
   */
  private void addKeys(int start, int end, String dirPrefix) throws Exception {
    for (int i = start; i < end; i++) {
      writeKeys("vol" + i, "bucket" + i, dirPrefix + i + "/key" + i);
    }
  }
}
