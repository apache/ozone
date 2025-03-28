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

package org.apache.hadoop.ozone.debug.replicas;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class checks block existence using GetBlock calls to the Datanodes.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestMetadataCheck {

  private static final Logger LOG = LoggerFactory.getLogger(TestMetadataCheck.class);
  private static MiniOzoneCluster cluster;
  private static OzoneClient client;
  private static OzoneConfiguration conf;
  private static MetadataCheck metadataCheck;
  private static final String VOLUME_NAME = UUID.randomUUID().toString();
  private static final String BUCKET_NAME = UUID.randomUUID().toString();
  private static final String KEY_NAME = UUID.randomUUID().toString();
  private static final StringWriter OUT = new StringWriter();
  private static PrintWriter printWriter;

  @BeforeAll
  public static void setUp() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    writeKey(KEY_NAME);

    printWriter = new PrintWriter(OUT);
    metadataCheck = new MetadataCheck(client, LOG, printWriter, conf);
  }

  @AfterEach
  public void cleanUp() {
    OUT.flush();
  }

  @AfterAll
  public static void tearDown() {
    IOUtils.closeQuietly(client, cluster);
  }

  @Order(1)
  @Test
  void testBlockExists() throws IOException {
    OzoneKeyDetails keyDetails = client.getProxy().getKeyDetails(VOLUME_NAME, BUCKET_NAME, KEY_NAME);

    metadataCheck.verifyKey(keyDetails);
    String cliOutput = OUT.toString();

    assertThat(cliOutput).contains("\"status\":\"BLOCK_EXISTS\"");
    assertThat(cliOutput).contains("\"pass\":true");
  }

  @Order(2)
  @Test
  void testMissingReplicas() throws IOException {
    OzoneKeyDetails keyDetails = client.getProxy().getKeyDetails(VOLUME_NAME, BUCKET_NAME, KEY_NAME);

    List<OmKeyLocationInfo> keyLocations = lookupKey(cluster);
    assertThat(keyLocations).isNotEmpty();

    OmKeyLocationInfo keyLocation = keyLocations.get(0);
    BlockID blockID = keyLocation.getBlockID();
    // Iterate over Datanodes
    for (HddsDatanodeService datanode : cluster.getHddsDatanodes()) {
      ContainerSet dnContainerSet = datanode.getDatanodeStateMachine().getContainer().getContainerSet();

      // Retrieve the container for the block
      KeyValueContainer container = (KeyValueContainer) dnContainerSet.getContainer(blockID.getContainerID());
      KeyValueContainerData containerData = container.getContainerData();

      try (DBHandle db = BlockUtils.getDB(containerData, conf)) {
        Table<String, BlockData> blockDataTable = db.getStore().getBlockDataTable();

        String blockKey = containerData.getBlockKey(blockID.getLocalID());

        // Ensure the block exists before deletion
        assertNotNull(blockDataTable.get(blockKey));

        // Delete the block from RocksDB
        blockDataTable.delete(blockKey);

        // Verify deletion
        assertNull(blockDataTable.get(blockKey));
      }
    }

    metadataCheck.verifyKey(keyDetails);
    String cliOutput = OUT.toString();

    assertThat(cliOutput).contains("\"status\":\"MISSING_REPLICAS\"");
    assertThat(cliOutput).contains("\"pass\":false");
  }

  private static List<OmKeyLocationInfo> lookupKey(MiniOzoneCluster ozoneCluster)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName(KEY_NAME)
        .build();
    OmKeyInfo keyInfo = ozoneCluster.getOzoneManager().lookupKey(keyArgs);

    OmKeyLocationInfoGroup locations = keyInfo.getLatestVersionLocations();
    assertNotNull(locations);
    return locations.getLocationList();
  }

  private static void writeKey(String keyName) throws IOException {
    try (OzoneClient client = OzoneClientFactory.getRpcClient(conf)) {
      TestDataUtil.createVolumeAndBucket(client, VOLUME_NAME, BUCKET_NAME);
      TestDataUtil.createKey(
          client.getObjectStore().getVolume(VOLUME_NAME).getBucket(BUCKET_NAME),
          keyName, "test".getBytes(StandardCharsets.UTF_8));
    }
  }

}
