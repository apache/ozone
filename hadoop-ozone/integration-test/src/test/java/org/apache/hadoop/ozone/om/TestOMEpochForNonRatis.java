/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om;

import java.util.HashMap;

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OmUtils.EPOCH_ID_SHIFT;
import static org.apache.hadoop.ozone.OmUtils.EPOCH_WHEN_RATIS_NOT_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY;

/**
 * Tests OM epoch generation for when Ratis is not enabled.
 */
@Timeout(240)
public class TestOMEpochForNonRatis {
  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static OzoneClient client;

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_OM_RATIS_ENABLE_KEY, false);
    cluster =  MiniOzoneCluster.newBuilder(conf)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testUniqueTrxnIndexOnOMRestart() throws Exception {
    // When OM is restarted, the transaction index for requests should not
    // start from 0. It should incrementally increase from the last
    // transaction index which was stored in DB before restart.

    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String keyName = "key" + RandomStringUtils.randomNumeric(5);

    OzoneManager om = cluster.getOzoneManager();
    ObjectStore objectStore = client.getObjectStore();

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    OzoneManagerProtocolClientSideTranslatorPB omClient =
        new OzoneManagerProtocolClientSideTranslatorPB(
            OmTransportFactory.create(conf, ugi, null),
            RandomStringUtils.randomAscii(5));

    objectStore.createVolume(volumeName);

    // Verify that the last transactionIndex stored in DB after volume
    // creation equals the transaction index corresponding to volume's
    // objectID. Also, the volume transaction index should be 1 as this is
    // the first transaction in this cluster.
    OmVolumeArgs volumeInfo = omClient.getVolumeInfo(volumeName);
    long volumeTrxnIndex = OmUtils.getTxIdFromObjectId(
        volumeInfo.getObjectID());
    assertEquals(1, volumeTrxnIndex);
    assertEquals(volumeTrxnIndex, om.getLastTrxnIndexForNonRatis());

    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    ozoneVolume.createBucket(bucketName);

    // Verify last transactionIndex is updated after bucket creation
    OmBucketInfo bucketInfo = omClient.getBucketInfo(volumeName, bucketName);
    long bucketTrxnIndex = OmUtils.getTxIdFromObjectId(
        bucketInfo.getObjectID());
    assertEquals(2, bucketTrxnIndex);
    assertEquals(bucketTrxnIndex, om.getLastTrxnIndexForNonRatis());

    // Restart the OM and create new object
    cluster.restartOzoneManager();

    String data = "random data";
    OzoneOutputStream ozoneOutputStream = ozoneVolume.getBucket(bucketName)
        .createKey(keyName, data.length(), ReplicationType.RATIS,
            ReplicationFactor.ONE, new HashMap<>());
    ozoneOutputStream.write(data.getBytes(UTF_8), 0, data.length());
    ozoneOutputStream.close();

    // Verify last transactionIndex is updated after key creation and the
    // transaction index after restart is incremented from the last
    // transaction index before restart.
    OmKeyInfo omKeyInfo = omClient.lookupKey(new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build());
    long keyTrxnIndex = OmUtils.getTxIdFromObjectId(
        omKeyInfo.getObjectID());
    assertEquals(3, keyTrxnIndex);
    // Key commit is a separate transaction. Hence, the last trxn index in DB
    // should be 1 more than KeyTrxnIndex
    assertEquals(4, om.getLastTrxnIndexForNonRatis());
  }

  @Test
  public void testEpochIntegrationInObjectID() throws Exception {
    // Create a volume and check the objectID has the epoch as
    // EPOCH_FOR_RATIS_NOT_ENABLED in the first 2 bits.

    ObjectStore objectStore = client.getObjectStore();

    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    objectStore.createVolume(volumeName);

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    OzoneManagerProtocolClientSideTranslatorPB omClient =
        new OzoneManagerProtocolClientSideTranslatorPB(
            OmTransportFactory.create(conf, ugi, null),
            RandomStringUtils.randomAscii(5));

    long volObjId = omClient.getVolumeInfo(volumeName).getObjectID();
    long epochInVolObjId = volObjId >> EPOCH_ID_SHIFT;

    assertEquals(EPOCH_WHEN_RATIS_NOT_ENABLED, epochInVolObjId);
  }
}
