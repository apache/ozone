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

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.*;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOmMetadataManagerImpl;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.KEY;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;

public class TestRecon {
  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static OMMetadataManager metadataManager;
  private static ReconOmMetadataManagerImpl reconOmMetadataManager;
  private static File dir;
  private static KeyManagerImpl keyManager;
  private static UserGroupInformation ugi;

  @BeforeClass
  public static void init() throws Exception {
    dir = GenericTestUtils.getRandomizedTestDir();
    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.toString());
    ugi = UserGroupInformation.getCurrentUser();

    cluster =  MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
    metadataManager = cluster.getOzoneManager().getMetadataManager();

    cluster.getStorageContainerManager().exitSafeMode();

    //override initial delay and delay configs
  }

  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testReconServer() throws Exception {
    String dbDir = cluster.getReconServer().getInjector()
        .getInstance(OzoneConfiguration.class).get(OZONE_RECON_DB_DIR);

    //add a vol, bucket and key
    createVolume("vol1");
    createBucket("vol1", "bucket1");
    createKey("vol1", "bucket1", "key1");

    //check if ommetadata has vol1/bucket1/key1 info
    String ozoneKey = metadataManager.getOzoneKey(
        "vol1", "bucket1", "key1");
    OmKeyInfo keyInfo1 = metadataManager.getKeyTable().get(ozoneKey);

    String buckName = keyInfo1.getBucketName();
    String volName = keyInfo1.getVolumeName();

    TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
        omKeyValueTableIterator = metadataManager.getKeyTable().iterator();

    long omMetadataKeyCount = getTableKeyCount(omKeyValueTableIterator);

    Assert.assertEquals("vol1", volName);
    Assert.assertEquals("bucket1", buckName);


    reconOmMetadataManager = cluster.getReconServer()
        .getInjector().getInstance(ReconOmMetadataManagerImpl.class);

    //pause to get the next snapshot from om
    Thread.sleep(15000);

    //verify if recon metadata captures vol1/bucket1/key1 info
    Assert.assertEquals(volName,reconOmMetadataManager.getKeyTable()
        .get(ozoneKey).getVolumeName());

    TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
        reconKeyValueTableIterator = reconOmMetadataManager.
        getKeyTable().iterator();
    long reconMetadataKeyCount = getTableKeyCount(reconKeyValueTableIterator);

    //verify if recon has the full snapshot
    Assert.assertEquals(omMetadataKeyCount, reconMetadataKeyCount);

    //add 5 keys to check for delta updates
    addKeys();
    omKeyValueTableIterator = metadataManager.getKeyTable().iterator();
    omMetadataKeyCount = getTableKeyCount(omKeyValueTableIterator);
    Assert.assertEquals(5, omMetadataKeyCount);

    //pause to get the next snapshot from om to verify delta updates
    Thread.sleep(15000);

    reconKeyValueTableIterator = reconOmMetadataManager.
        getKeyTable().iterator();

    reconMetadataKeyCount = getTableKeyCount(reconKeyValueTableIterator);

    ozoneKey = metadataManager.getOzoneKey(
        "vol3", "bucket3", "key3");

    //verify if recon stores vol3/bucket3/key3 info from om delta updates
    Assert.assertEquals("vol3",reconOmMetadataManager.getKeyTable()
        .get(ozoneKey).getVolumeName());

    // verify if delta updates were applied.
    Assert.assertEquals(omMetadataKeyCount, reconMetadataKeyCount);



  }

  @Test
  public void testReconRestart() {
    System.out.println();
    ;
  }

  private void addKeys() throws Exception {
    for(int i=0; i < 5; i++) {
      createVolume("vol" + i);
      createBucket("vol" + i, "bucket" + i);
      createKey("vol" + i, "bucket" + i, "key" + i);
    }
  }


  private long getTableKeyCount(TableIterator<String, ? extends
      Table.KeyValue<String, OmKeyInfo>> iterator) {
    long keyCount = 0;
    while(iterator.hasNext()) {
      keyCount ++;
      iterator.next();
    }
    return keyCount;
  }

  private static void createVolume(String volumeName) throws IOException {
    OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setAdminName("bilbo")
        .setOwnerName("bilbo")
        .build();
    TestOMRequestUtils.addVolumeToOM(metadataManager, volumeArgs);
  }

  private static void createBucket(String volumeName, String bucketName)
      throws IOException {
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .build();

    TestOMRequestUtils.addBucketToOM(metadataManager, bucketInfo);
  }

  private static void createKey(String volume, String bucket, String keyName)
      throws Exception {
    TestOMRequestUtils.addKeyToTable(false, volume, bucket, keyName, 1,
        HddsProtos.ReplicationType.STAND_ALONE,
        HddsProtos.ReplicationFactor.ONE, metadataManager);

    //TestOMRequestUtils.addKeyToTableCache() ??
  }
}
