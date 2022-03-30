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
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.DELETE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionLogIterator;
import org.rocksdb.WriteBatch;

/**
 * Class used to test OMDBUpdatesHandler.
 */
public class TestOMDBUpdatesHandler {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OMMetadataManager omMetadataManager;
  private OMMetadataManager reconOmMetadataManager;
  private OMDBDefinition omdbDefinition = new OMDBDefinition();
  private Random random = new Random();

  private OzoneConfiguration createNewTestPath() throws IOException {
    OzoneConfiguration configuration = new OzoneConfiguration();
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(configuration, newFolder.toString());
    return configuration;
  }

  @Before
  public void setUp() throws Exception {
    OzoneConfiguration configuration = createNewTestPath();
    omMetadataManager = new OmMetadataManagerImpl(configuration);

    OzoneConfiguration reconConfiguration = createNewTestPath();
    reconOmMetadataManager = new OmMetadataManagerImpl(reconConfiguration);
  }

  @Test
  public void testPut() throws Exception {
    // Create 1 volume, 2 keys and write to source OM DB.
    String volumeKey = omMetadataManager.getVolumeKey("sampleVol");
    OmVolumeArgs args =
        OmVolumeArgs.newBuilder()
            .setVolume("sampleVol")
            .setAdminName("bilbo")
            .setOwnerName("bilbo")
            .build();
    omMetadataManager.getVolumeTable().put(volumeKey, args);

    OmKeyInfo firstKey = getOmKeyInfo("sampleVol", "bucketOne", "key_one");
    omMetadataManager.getKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_one", firstKey);

    OmKeyInfo secondKey = getOmKeyInfo("sampleVol", "bucketOne", "key_two");
    omMetadataManager.getKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_two", secondKey);

    // Write the secondKey to the target OM DB.
    reconOmMetadataManager.getKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_two", secondKey);

    List<byte[]> writeBatches = getBytesFromOmMetaManager(0);
    OMDBUpdatesHandler omdbUpdatesHandler = captureEvents(writeBatches);

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
    // Write 1 volume, 1 key into source and target OM DBs.
    String volumeKey = omMetadataManager.getVolumeKey("sampleVol");
    String nonExistVolumeKey = omMetadataManager.getVolumeKey("nonExistingVolume");
    OmVolumeArgs args =
        OmVolumeArgs.newBuilder()
            .setVolume("sampleVol")
            .setAdminName("bilbo")
            .setOwnerName("bilbo")
            .build();
    omMetadataManager.getVolumeTable().put(volumeKey, args);
    reconOmMetadataManager.getVolumeTable().put(volumeKey, args);

    OmKeyInfo omKeyInfo = getOmKeyInfo("sampleVol", "bucketOne", "key_one");
    omMetadataManager.getKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_one", omKeyInfo);
    reconOmMetadataManager.getKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_one", omKeyInfo);

    // Delete the volume and key from target DB.
    omMetadataManager.getKeyTable(getBucketLayout())
        .delete("/sampleVol/bucketOne/key_one");
    omMetadataManager.getVolumeTable().delete(volumeKey);
    // Delete a non-existing volume and key
    omMetadataManager.getKeyTable(getBucketLayout())
        .delete("/sampleVol/bucketOne/key_two");
    omMetadataManager.getVolumeTable().delete(omMetadataManager.getVolumeKey("nonExistingVolume"));

    List<byte[]> writeBatches = getBytesFromOmMetaManager(3);
    OMDBUpdatesHandler omdbUpdatesHandler = captureEvents(writeBatches);

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
  public void testOperateOnSameEntry() throws Exception {
    // Create 1 volume, 1 key and write to source OM DB.
    String volumeKey = omMetadataManager.getVolumeKey("sampleVol");
    OmVolumeArgs args =
        OmVolumeArgs.newBuilder()
            .setVolume("sampleVol")
            .setAdminName("bilbo")
            .setOwnerName("bilbo")
            .build();
    omMetadataManager.getVolumeTable().put(volumeKey, args);

    OmKeyInfo key = getOmKeyInfo("sampleVol", "bucketOne", "key");
    omMetadataManager.getKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key", key);

    OmKeyInfo keyNewValue = getOmKeyInfo("sampleVol", "bucketOne", "key_new");
    omMetadataManager.getKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key", keyNewValue);

    OmKeyInfo keyNewValue2 = getOmKeyInfo("sampleVol", "bucketOne", "key_new2");
    omMetadataManager.getKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key", keyNewValue2);

    omMetadataManager.getKeyTable(getBucketLayout())
        .delete("/sampleVol/bucketOne/key");
    omMetadataManager.getKeyTable(getBucketLayout())
        .delete("/sampleVol/bucketOne/key");
    omMetadataManager.getKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key", keyNewValue2);

    List<byte[]> writeBatches = getBytesFromOmMetaManager(0);
    OMDBUpdatesHandler omdbUpdatesHandler = captureEvents(writeBatches);

    List<OMDBUpdateEvent> events = omdbUpdatesHandler.getEvents();
    assertEquals(7, events.size());

    OMDBUpdateEvent volEvent = events.get(0);
    assertEquals(PUT, volEvent.getAction());
    assertEquals(volumeKey, volEvent.getKey());
    assertEquals(args.getVolume(), ((OmVolumeArgs)volEvent.getValue())
        .getVolume());

    OMDBUpdateEvent keyPutEvent = events.get(1);
    assertEquals(PUT, keyPutEvent.getAction());
    assertEquals("/sampleVol/bucketOne/key", keyPutEvent.getKey());
    assertEquals("key", ((OmKeyInfo)keyPutEvent.getValue()).getKeyName());
    assertNull(keyPutEvent.getOldValue());

    OMDBUpdateEvent keyUpdateEvent = events.get(2);
    assertEquals(UPDATE, keyUpdateEvent.getAction());
    assertEquals("/sampleVol/bucketOne/key", keyUpdateEvent.getKey());
    assertEquals("key_new", ((OmKeyInfo)keyUpdateEvent.getValue()).getKeyName());
    assertNotNull(keyUpdateEvent.getOldValue());
    assertEquals("key",
        ((OmKeyInfo)keyUpdateEvent.getOldValue()).getKeyName());

    OMDBUpdateEvent keyUpdateEvent2 = events.get(3);
    assertEquals(UPDATE, keyUpdateEvent2.getAction());
    assertEquals("/sampleVol/bucketOne/key", keyUpdateEvent2.getKey());
    assertEquals("key_new2", ((OmKeyInfo)keyUpdateEvent2.getValue()).getKeyName());
    assertNotNull(keyUpdateEvent2.getOldValue());
    assertEquals("key_new",
        ((OmKeyInfo)keyUpdateEvent2.getOldValue()).getKeyName());

    OMDBUpdateEvent keyDeleteEvent = events.get(4);
    assertEquals(DELETE, keyDeleteEvent.getAction());
    assertEquals("/sampleVol/bucketOne/key", keyDeleteEvent.getKey());
    assertEquals("key_new2", ((OmKeyInfo)keyDeleteEvent.getValue()).getKeyName());

    OMDBUpdateEvent keyDeleteEvent2 = events.get(5);
    assertEquals(DELETE, keyDeleteEvent2.getAction());
    assertEquals("/sampleVol/bucketOne/key", keyDeleteEvent2.getKey());
    assertEquals("key_new2", ((OmKeyInfo)keyDeleteEvent2.getValue()).getKeyName());

    OMDBUpdateEvent keyPut2 = events.get(6);
    assertEquals(UPDATE, keyPut2.getAction());
    assertEquals("/sampleVol/bucketOne/key", keyPut2.getKey());
    assertEquals("key_new2", ((OmKeyInfo)keyPut2.getValue()).getKeyName());
    assertNotNull(keyPut2.getOldValue());
    assertEquals("key_new2",
        ((OmKeyInfo)keyPut2.getOldValue()).getKeyName());
  }

  @Test
  public void testGetKeyType() throws IOException {
    assertEquals(String.class, omdbDefinition.getKeyType(
        omMetadataManager.getKeyTable(getBucketLayout()).getName()).get());
    assertEquals(OzoneTokenIdentifier.class, omdbDefinition.getKeyType(
        omMetadataManager.getDelegationTokenTable().getName()).get());
  }

  @Test
  public void testGetValueType() throws IOException {
    assertEquals(OmKeyInfo.class, omdbDefinition.getValueType(
        omMetadataManager.getKeyTable(getBucketLayout()).getName()).get());
    assertEquals(OmVolumeArgs.class, omdbDefinition.getValueType(
        omMetadataManager.getVolumeTable().getName()).get());
    assertEquals(OmBucketInfo.class, omdbDefinition.getValueType(
        omMetadataManager.getBucketTable().getName()).get());
  }

  @NotNull
  private List<byte[]> getBytesFromOmMetaManager(int getUpdatesSince) throws RocksDBException {
    RDBStore rdbStore = (RDBStore) omMetadataManager.getStore();
    RocksDB rocksDB = rdbStore.getDb();
    // Get all updates from source DB
    TransactionLogIterator transactionLogIterator =
        rocksDB.getUpdatesSince(getUpdatesSince);
    List<byte[]> writeBatches = new ArrayList<>();

    while (transactionLogIterator.isValid()) {
      TransactionLogIterator.BatchResult result =
          transactionLogIterator.getBatch();
      result.writeBatch().markWalTerminationPoint();
      WriteBatch writeBatch = result.writeBatch();
      writeBatches.add(writeBatch.data());
      transactionLogIterator.next();
    }
    return writeBatches;
  }

  @NotNull
  private OMDBUpdatesHandler captureEvents(List<byte[]> writeBatches) throws RocksDBException {
    OMDBUpdatesHandler omdbUpdatesHandler =
        new OMDBUpdatesHandler(reconOmMetadataManager);
    for (byte[] data : writeBatches) {
      WriteBatch writeBatch = new WriteBatch(data);
      // Capture the events from source DB.
      writeBatch.iterate(omdbUpdatesHandler);
    }
    return omdbUpdatesHandler;
  }

  private OmKeyInfo getOmKeyInfo(String volumeName, String bucketName,
                                 String keyName) {
    return new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setReplicationConfig(StandaloneReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.ONE))
        .setDataSize(random.nextLong())
        .build();
  }

  private BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }
}