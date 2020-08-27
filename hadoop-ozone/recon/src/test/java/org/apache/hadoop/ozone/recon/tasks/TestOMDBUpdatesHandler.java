/*
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

package org.apache.hadoop.ozone.recon.tasks;

import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.PUT;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.UPDATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDB;
import org.rocksdb.TransactionLogIterator;
import org.rocksdb.WriteBatch;

/**
 * Class used to test OMDBUpdatesHandler.
 */
public class TestOMDBUpdatesHandler {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OMDBDefinition omdbDefinition = new OMDBDefinition();

  private OzoneConfiguration createNewTestPath() throws IOException {
    OzoneConfiguration configuration = new OzoneConfiguration();
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(configuration, newFolder.toString());
    return configuration;
  }

  @Test
  public void testPut() throws Exception {
    OzoneConfiguration configuration = createNewTestPath();
    OmMetadataManagerImpl metaMgr = new OmMetadataManagerImpl(configuration);

    // Create 1 volume, 2 keys and write to source OM DB.
    String volumeKey = metaMgr.getVolumeKey("sampleVol");
    OmVolumeArgs args =
        OmVolumeArgs.newBuilder()
            .setVolume("sampleVol")
            .setAdminName("bilbo")
            .setOwnerName("bilbo")
            .build();
    metaMgr.getVolumeTable().put(volumeKey, args);

    OmKeyInfo firstKey = getOmKeyInfo("sampleVol", "bucketOne", "key_one");
    metaMgr.getKeyTable().put("/sampleVol/bucketOne/key_one", firstKey);

    OmKeyInfo secondKey = getOmKeyInfo("sampleVol", "bucketOne", "key_two");
    metaMgr.getKeyTable().put("/sampleVol/bucketOne/key_two", secondKey);

    // Write the secondKey to the target OM DB.
    OzoneConfiguration conf2 = createNewTestPath();
    OmMetadataManagerImpl reconOmmetaMgr = new OmMetadataManagerImpl(conf2);
    reconOmmetaMgr.getKeyTable().put("/sampleVol/bucketOne/key_two", secondKey);

    RDBStore rdbStore = (RDBStore) metaMgr.getStore();
    RocksDB rocksDB = rdbStore.getDb();
    // Get all updates from source DB. (3 PUTs)
    TransactionLogIterator transactionLogIterator =
        rocksDB.getUpdatesSince(0);
    List<byte[]> writeBatches = new ArrayList<>();

    while(transactionLogIterator.isValid()) {
      TransactionLogIterator.BatchResult result =
          transactionLogIterator.getBatch();
      result.writeBatch().markWalTerminationPoint();
      WriteBatch writeBatch = result.writeBatch();
      writeBatches.add(writeBatch.data());
      transactionLogIterator.next();
    }

    // OMDBUpdatesHandler has access to target DB. Hence it has only the
    // "secondKey".
    OMDBUpdatesHandler omdbUpdatesHandler =
        new OMDBUpdatesHandler(reconOmmetaMgr);
    for (byte[] data : writeBatches) {
      WriteBatch writeBatch = new WriteBatch(data);
      // Capture the 3 PUT events from source DB.
      writeBatch.iterate(omdbUpdatesHandler);
    }

    List<OMDBUpdateEvent> events = omdbUpdatesHandler.getEvents();
    assertEquals(3, events.size());

    OMDBUpdateEvent volEvent = events.get(0);
    assertEquals(PUT, volEvent.getAction());
    assertEquals(volumeKey, volEvent.getKey());
    assertEquals(args.getVolume(), ((OmVolumeArgs)volEvent.getValue())
        .getVolume());

    OMDBUpdateEvent keyEvent = events.get(1);
    assertEquals(PUT, keyEvent.getAction());
    assertEquals("/sampleVol/bucketOne/key_one", keyEvent.getKey());
    assertNull(keyEvent.getOldValue());

    OMDBUpdateEvent updateEvent = events.get(2);
    assertEquals(UPDATE, updateEvent.getAction());
    assertEquals("/sampleVol/bucketOne/key_two", updateEvent.getKey());
    assertNotNull(updateEvent.getOldValue());
    assertEquals(secondKey.getKeyName(),
        ((OmKeyInfo)updateEvent.getOldValue()).getKeyName());
  }

  @Test
  public void testDelete() throws Exception {

    OzoneConfiguration configuration = createNewTestPath();
    OmMetadataManagerImpl metaMgr = new OmMetadataManagerImpl(configuration);

    OzoneConfiguration conf2 = createNewTestPath();
    OmMetadataManagerImpl metaMgrCopy = new OmMetadataManagerImpl(conf2);

    // Write 1 volume, 1 key into source and target OM DBs.
    String volumeKey = metaMgr.getVolumeKey("sampleVol");
    String nonExistVolumeKey = metaMgr.getVolumeKey("nonExistingVolume");
    OmVolumeArgs args =
        OmVolumeArgs.newBuilder()
            .setVolume("sampleVol")
            .setAdminName("bilbo")
            .setOwnerName("bilbo")
            .build();
    metaMgr.getVolumeTable().put(volumeKey, args);
    metaMgrCopy.getVolumeTable().put(volumeKey, args);

    OmKeyInfo omKeyInfo = getOmKeyInfo("sampleVol", "bucketOne", "key_one");
    metaMgr.getKeyTable().put("/sampleVol/bucketOne/key_one", omKeyInfo);
    metaMgrCopy.getKeyTable().put("/sampleVol/bucketOne/key_one", omKeyInfo);

    // Delete the volume and key from target DB.
    metaMgr.getKeyTable().delete("/sampleVol/bucketOne/key_one");
    metaMgr.getVolumeTable().delete(volumeKey);
    // Delete a non-existing volume and key
    metaMgr.getKeyTable().delete("/sampleVol/bucketOne/key_two");
    metaMgr.getVolumeTable().delete(metaMgr.getVolumeKey("nonExistingVolume"));

    RDBStore rdbStore = (RDBStore) metaMgr.getStore();
    RocksDB rocksDB = rdbStore.getDb();
    TransactionLogIterator transactionLogIterator =
        rocksDB.getUpdatesSince(3);
    List<byte[]> writeBatches = new ArrayList<>();

    while(transactionLogIterator.isValid()) {
      TransactionLogIterator.BatchResult result =
          transactionLogIterator.getBatch();
      result.writeBatch().markWalTerminationPoint();
      WriteBatch writeBatch = result.writeBatch();
      writeBatches.add(writeBatch.data());
      transactionLogIterator.next();
    }

    // OMDBUpdatesHandler has access to target DB. So it has the volume and
    // key.
    OMDBUpdatesHandler omdbUpdatesHandler =
        new OMDBUpdatesHandler(metaMgrCopy);
    for (byte[] data : writeBatches) {
      WriteBatch writeBatch = new WriteBatch(data);
      writeBatch.iterate(omdbUpdatesHandler);
    }

    List<OMDBUpdateEvent> events = omdbUpdatesHandler.getEvents();
    assertEquals(4, events.size());

    OMDBUpdateEvent keyEvent = events.get(0);
    assertEquals(OMDBUpdateEvent.OMDBUpdateAction.DELETE, keyEvent.getAction());
    assertEquals("/sampleVol/bucketOne/key_one", keyEvent.getKey());
    assertEquals(omKeyInfo, keyEvent.getValue());

    OMDBUpdateEvent volEvent = events.get(1);
    assertEquals(OMDBUpdateEvent.OMDBUpdateAction.DELETE, volEvent.getAction());
    assertEquals(volumeKey, volEvent.getKey());
    assertNotNull(volEvent.getValue());
    OmVolumeArgs volumeInfo = (OmVolumeArgs) volEvent.getValue();
    assertEquals("sampleVol", volumeInfo.getVolume());

    // Assert the values of non existent keys are set to null.
    OMDBUpdateEvent nonExistKey = events.get(2);
    assertEquals(OMDBUpdateEvent.OMDBUpdateAction.DELETE,
        nonExistKey.getAction());
    assertEquals("/sampleVol/bucketOne/key_two", nonExistKey.getKey());
    assertNull(nonExistKey.getValue());

    OMDBUpdateEvent nonExistVolume = events.get(3);
    assertEquals(OMDBUpdateEvent.OMDBUpdateAction.DELETE,
        nonExistVolume.getAction());
    assertEquals(nonExistVolumeKey, nonExistVolume.getKey());
    assertNull(nonExistVolume.getValue());
  }

  @Test
  public void testGetKeyType() throws IOException {
    OzoneConfiguration configuration = createNewTestPath();
    OmMetadataManagerImpl metaMgr = new OmMetadataManagerImpl(configuration);
    OMDBUpdatesHandler omdbUpdatesHandler =
        new OMDBUpdatesHandler(metaMgr);

    assertEquals(String.class, omdbDefinition.getKeyType(
        metaMgr.getKeyTable().getName()).get());
    assertEquals(OzoneTokenIdentifier.class, omdbDefinition.getKeyType(
        metaMgr.getDelegationTokenTable().getName()).get());
  }

  @Test
  public void testGetValueType() throws IOException {
    OzoneConfiguration configuration = createNewTestPath();
    OmMetadataManagerImpl metaMgr = new OmMetadataManagerImpl(configuration);
    OMDBUpdatesHandler omdbUpdatesHandler =
        new OMDBUpdatesHandler(metaMgr);

    assertEquals(OmKeyInfo.class, omdbDefinition.getValueType(
        metaMgr.getKeyTable().getName()).get());
    assertEquals(OmVolumeArgs.class, omdbDefinition.getValueType(
        metaMgr.getVolumeTable().getName()).get());
    assertEquals(OmBucketInfo.class, omdbDefinition.getValueType(
        metaMgr.getBucketTable().getName()).get());
  }

  private OmKeyInfo getOmKeyInfo(String volumeName, String bucketName,
                                 String keyName) {
    return new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setReplicationFactor(HddsProtos.ReplicationFactor.ONE)
        .setReplicationType(HddsProtos.ReplicationType.STAND_ALONE)
        .setDataSize(new Random().nextLong())
        .build();
  }
}