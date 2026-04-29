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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_GAP;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.recon.TestReconEndpointUtil.getReconWebAddress;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.recon.api.DataNodeMetricsService;
import org.apache.hadoop.ozone.recon.api.types.DataNodeMetricsServiceResponse;
import org.apache.hadoop.ozone.recon.api.types.DatanodeStorageReport;
import org.apache.hadoop.ozone.recon.api.types.ScmPendingDeletion;
import org.apache.hadoop.ozone.recon.api.types.StorageCapacityDistributionResponse;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for Storage Distribution endpoint integration tests.
 * Provides shared cluster lifecycle management, helper methods, and common
 * verification logic. Subclasses supply the replication-type-specific
 * configuration (number of datanodes, replication config) and the concrete
 * {@code @Test} method.
 */
public abstract class AbstractTestStorageDistributionEndpoint {

  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractTestStorageDistributionEndpoint.class);

  private static OzoneConfiguration conf;
  private static MiniOzoneCluster cluster;
  private static OzoneManager om;
  private static StorageContainerManager scm;
  private static OzoneClient client;
  private static ReconService recon;
  private FileSystem fs;
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  protected static final String STORAGE_DIST_ENDPOINT = "/api/v1/storageDistribution";
  protected static final String PENDING_DELETION_ENDPOINT = "/api/v1/pendingDeletion";

  protected static OzoneConfiguration getConf() {
    return conf;
  }

  protected static MiniOzoneCluster getCluster() {
    return cluster;
  }

  protected static OzoneManager getOm() {
    return om;
  }

  protected static StorageContainerManager getScm() {
    return scm;
  }

  protected static OzoneClient getClient() {
    return client;
  }

  protected static ReconService getRecon() {
    return recon;
  }

  protected FileSystem getFs() {
    return fs;
  }

  protected void setFs(FileSystem fileSystem) {
    this.fs = fileSystem;
  }

  /**
   * Returns the number of datanodes to start in the mini cluster.
   * Subclasses return a value appropriate for their replication type.
   */
  protected abstract int getNumDatanodes();

  /**
   * Initialises the {@link OzoneConfiguration}, builds a {@link MiniOzoneCluster}
   * with {@code numDatanodes} datanodes, and waits for the cluster to be ready.
   * Subclasses call this from their own {@code @BeforeAll} static method.
   */
  protected static void initializeCluster(int numDatanodes) throws Exception {
    conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_DIR_DELETING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100, TimeUnit.MILLISECONDS);
    conf.setLong(OZONE_SCM_HA_RATIS_SNAPSHOT_GAP, 1L);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 50, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL, 500, TimeUnit.MILLISECONDS);
    conf.set(ReconServerConfigKeys.OZONE_RECON_DN_METRICS_COLLECTION_MINIMUM_API_DELAY, "5s");

    ScmConfig scmConfig = conf.getObject(ScmConfig.class);
    scmConfig.setBlockDeletionInterval(Duration.ofMillis(100));
    conf.setFromObject(scmConfig);
    conf.set(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "0s");

    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
    dnConf.setBlockDeletionInterval(Duration.ofMillis(30000));
    conf.setFromObject(dnConf);

    recon = new ReconService(conf);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numDatanodes)
        .addService(recon)
        .build();
    cluster.waitForClusterToBeReady();
    om = cluster.getOzoneManager();
    scm = cluster.getStorageContainerManager();
    client = cluster.newClient();
  }

  protected OzoneBucket createVolumeAndBucket(ReplicationConfig replicationConfig)
      throws Exception {
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume("vol1");
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .setDefaultReplicationConfig(new DefaultReplicationConfig(replicationConfig))
        .build();
    OzoneVolume volume = objectStore.getVolume("vol1");
    volume.createBucket("bucket1", bucketArgs);
    return volume.getBucket("bucket1");
  }

  protected void createOpenKeysAndMultipartKeys(String volumeName,
      String bucketName, ReplicationConfig config) throws Exception {
    ObjectStore objectStore = client.getObjectStore();
    OzoneManagerProtocol ozoneManagerClient =
        client.getObjectStore().getClientProxy().getOzoneManagerClient();

    OmKeyArgs openKey = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName("openkey1")
        .setReplicationConfig(config)
        .setDataSize(100) // unreplicated size; replicated = 10 * 3 = 30
        .setAcls(new ArrayList<>())
        .setOwnerName("ownerName")
        .build();
    ozoneManagerClient.openKey(openKey);

    OmMultipartInfo multipartInfo = objectStore.getClientProxy()
        .initiateMultipartUpload(volumeName, bucketName, "mpukey1",
            config, new HashMap<>());
    OzoneOutputStream partStream = objectStore.getClientProxy()
        .createMultipartKey(volumeName, bucketName, "mpukey1",
            100L, 1, multipartInfo.getUploadID());
    partStream.write(new byte[100]);
    partStream.close();
  }

  protected boolean verifyStorageDistributionAfterKeyCreation() {
    try {
      StringBuilder urlBuilder = new StringBuilder();
      urlBuilder.append(getReconWebAddress(conf)).append(STORAGE_DIST_ENDPOINT);
      String response = TestReconEndpointUtil.makeHttpCall(conf, urlBuilder);
      StorageCapacityDistributionResponse storageResponse =
          MAPPER.readValue(response, StorageCapacityDistributionResponse.class);

      assertEquals(20, storageResponse.getGlobalNamespace().getTotalKeys());
      assertEquals(1200, storageResponse.getGlobalNamespace().getTotalUsedSpace());
      assertEquals(600, storageResponse.getUsedSpaceBreakDown().getOpenKeyBytes().getTotalOpenKeyBytes());
      assertEquals(300, storageResponse.getUsedSpaceBreakDown().getOpenKeyBytes().getMultipartOpenKeyBytes());
      assertEquals(300, storageResponse.getUsedSpaceBreakDown().getOpenKeyBytes().getOpenKeyAndFileBytes());
      assertEquals(600, storageResponse.getUsedSpaceBreakDown().getFinalizedKeyBytes());
      assertEquals(getNumDatanodes(), storageResponse.getDataNodeUsage().size());

      List<DatanodeStorageReport> reports = storageResponse.getDataNodeUsage();
      List<HddsProtos.DatanodeUsageInfoProto> scmReports =
          scm.getClientProtocolServer().getDatanodeUsageInfo(true, getNumDatanodes(), 1);

      long totalReserved = 0;
      long totalMinFreeSpace = 0;
      long totalPreAllocated = 0;
      long totalFileSystemCapacity = 0;
      long totalOzoneCapacity = 0;
      long totalOzoneUsedSpace = 0;
      long totalRemaining = 0;

      for (DatanodeStorageReport report : reports) {
        for (HddsProtos.DatanodeUsageInfoProto scmReport : scmReports) {
          if (scmReport.getNode().getUuid().equals(report.getDatanodeUuid())) {
            assertEquals(scmReport.getFreeSpaceToSpare(), report.getMinimumFreeSpace());
            assertEquals(scmReport.getReserved(), report.getReserved());
            assertEquals(scmReport.getCapacity(), report.getCapacity());
            assertEquals(scmReport.getRemaining(), report.getRemaining());
            assertEquals(scmReport.getUsed(), report.getUsed());
            assertEquals(scmReport.getCommitted(), report.getCommitted());
            assertEquals(scmReport.getFsAvailable(), report.getFilesystemAvailable());
            assertEquals(scmReport.getFsCapacity(), report.getFilesystemCapacity());
            totalReserved += scmReport.getReserved();
            totalMinFreeSpace += scmReport.getFreeSpaceToSpare();
            totalPreAllocated += scmReport.getCommitted();
            totalFileSystemCapacity += scmReport.getFsCapacity();
            totalOzoneCapacity += scmReport.getCapacity();
            totalOzoneUsedSpace += scmReport.getUsed();
            totalRemaining += scmReport.getRemaining();
          }
        }
      }
      assertEquals(totalReserved, storageResponse.getGlobalStorage().getTotalReservedSpace());
      assertEquals(totalMinFreeSpace, storageResponse.getGlobalStorage().getTotalMinimumFreeSpace());
      assertEquals(totalPreAllocated, storageResponse.getGlobalStorage().getTotalOzoneCommittedSpace());
      assertEquals(totalOzoneCapacity, storageResponse.getGlobalStorage().getTotalOzoneCapacity());
      assertEquals(totalOzoneUsedSpace, storageResponse.getGlobalStorage().getTotalOzoneUsedSpace());
      assertEquals(totalFileSystemCapacity, storageResponse.getGlobalStorage().getTotalFileSystemCapacity());
      assertEquals(totalRemaining, storageResponse.getGlobalStorage().getTotalOzoneFreeSpace());
      return true;
    } catch (Throwable e) {
      LOG.debug("Waiting for storage distribution assertions to pass", e);
      return false;
    }
  }

  protected boolean verifyPendingDeletionAfterKeyDeletionOm() {
    try {
      syncDataFromOM();
      StringBuilder urlBuilder = new StringBuilder();
      urlBuilder.append(getReconWebAddress(conf)).append(PENDING_DELETION_ENDPOINT).append("?component=om");
      String response = TestReconEndpointUtil.makeHttpCall(conf, urlBuilder);
      Map<String, Number> pendingDeletionMap = MAPPER.readValue(response, Map.class);
      assertEquals(300L, pendingDeletionMap.get("totalSize").longValue());
      assertEquals(300L, pendingDeletionMap.get("pendingDirectorySize").longValue() +
          pendingDeletionMap.get("pendingKeySize").longValue());
      return true;
    } catch (Exception e) {
      LOG.debug("Waiting for storage distribution assertions to pass", e);
      return false;
    }
  }

  protected boolean verifyPendingDeletionAfterKeyDeletionScm() {
    try {
      StringBuilder urlBuilder = new StringBuilder();
      urlBuilder.append(getReconWebAddress(conf)).append(PENDING_DELETION_ENDPOINT).append("?component=scm");
      String response = TestReconEndpointUtil.makeHttpCall(conf, urlBuilder);
      ScmPendingDeletion pendingDeletion = MAPPER.readValue(response, ScmPendingDeletion.class);
      assertEquals(300, pendingDeletion.getTotalReplicatedBlockSize());
      assertEquals(100, pendingDeletion.getTotalBlocksize());
      assertEquals(10, pendingDeletion.getTotalBlocksCount());
      return true;
    } catch (Throwable e) {
      LOG.debug("Waiting for storage distribution assertions to pass", e);
      return false;
    }
  }

  protected boolean verifyPendingDeletionAfterKeyDeletionDn() {
    try {
      scm.getScmHAManager().asSCMHADBTransactionBuffer().flush();
      StringBuilder urlBuilder = new StringBuilder();
      urlBuilder.append(getReconWebAddress(conf)).append(PENDING_DELETION_ENDPOINT).append("?component=dn");
      String response = TestReconEndpointUtil.makeHttpCall(conf, urlBuilder);
      DataNodeMetricsServiceResponse pendingDeletion =
          MAPPER.readValue(response, DataNodeMetricsServiceResponse.class);
      assertNotNull(pendingDeletion);
      assertEquals(300, pendingDeletion.getTotalPendingDeletionSize());
      assertEquals(DataNodeMetricsService.MetricCollectionStatus.FINISHED, pendingDeletion.getStatus());
      assertEquals(pendingDeletion.getTotalNodesQueried(),
          pendingDeletion.getPendingDeletionPerDataNode().size());
      assertEquals(0, pendingDeletion.getTotalNodeQueryFailures());
      pendingDeletion.getPendingDeletionPerDataNode().forEach(dn ->
          assertEquals(300 / getNumDatanodes(), dn.getPendingBlockSize()));
      return true;
    } catch (Throwable e) {
      LOG.debug("Waiting for storage distribution assertions to pass", e);
      return false;
    }
  }

  protected boolean verifyPendingDeletionClearsAtDn() {
    try {
      scm.getScmHAManager().asSCMHADBTransactionBuffer().flush();
      StringBuilder urlBuilder = new StringBuilder();
      urlBuilder.append(getReconWebAddress(conf)).append(PENDING_DELETION_ENDPOINT).append("?component=dn");
      String response = TestReconEndpointUtil.makeHttpCall(conf, urlBuilder);
      DataNodeMetricsServiceResponse pendingDeletion =
          MAPPER.readValue(response, DataNodeMetricsServiceResponse.class);
      assertNotNull(pendingDeletion);
      assertEquals(0, pendingDeletion.getTotalPendingDeletionSize());
      assertEquals(DataNodeMetricsService.MetricCollectionStatus.FINISHED, pendingDeletion.getStatus());
      assertEquals(pendingDeletion.getTotalNodesQueried(),
          pendingDeletion.getPendingDeletionPerDataNode().size());
      assertEquals(0, pendingDeletion.getTotalNodeQueryFailures());
      pendingDeletion.getPendingDeletionPerDataNode().forEach(dn ->
          assertEquals(0, dn.getPendingBlockSize()));
      return true;
    } catch (Throwable e) {
      LOG.debug("Waiting for storage distribution assertions to pass", e);
      return false;
    }
  }

  protected boolean verifyPendingDeletionAfterKeyDeletionOnDnFailure() {
    try {
      StringBuilder urlBuilder = new StringBuilder();
      urlBuilder.append(getReconWebAddress(conf)).append(PENDING_DELETION_ENDPOINT).append("?component=dn");
      String response = TestReconEndpointUtil.makeHttpCall(conf, urlBuilder);
      DataNodeMetricsServiceResponse pendingDeletion =
          MAPPER.readValue(response, DataNodeMetricsServiceResponse.class);
      assertNotNull(pendingDeletion);
      assertEquals(1, pendingDeletion.getTotalNodeQueryFailures());
      assertTrue(pendingDeletion.getPendingDeletionPerDataNode()
          .stream()
          .anyMatch(dn -> dn.getPendingBlockSize() == -1));
      return true;
    } catch (Throwable e) {
      return false;
    }
  }

  protected static void syncDataFromOM() {
    OzoneManagerServiceProviderImpl impl = (OzoneManagerServiceProviderImpl)
        recon.getReconServer().getOzoneManagerServiceProvider();
    impl.syncDataFromOM();
  }

  protected static void closeAllContainers() {
    for (ContainerInfo container : scm.getContainerManager().getContainers()) {
      scm.getEventQueue().fireEvent(SCMEvents.CLOSE_CONTAINER, container.containerID());
    }
  }

  @AfterEach
  public void cleanup() {
    assertDoesNotThrow(() -> {
      if (fs != null) {
        Path root = new Path("/");
        FileStatus[] fileStatuses = fs.listStatus(root);
        for (FileStatus fileStatus : fileStatuses) {
          fs.delete(fileStatus.getPath(), true);
        }
      }
    });
    IOUtils.closeQuietly(fs);
  }

  @AfterAll
  public static void tear() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
