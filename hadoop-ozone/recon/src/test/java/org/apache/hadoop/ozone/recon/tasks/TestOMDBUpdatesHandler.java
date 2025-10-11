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

package org.apache.hadoop.ozone.recon.tasks;

import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.DELETE;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.PUT;
import static org.apache.hadoop.ozone.recon.tasks.OMDBUpdateEvent.OMDBUpdateAction.UPDATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.managed.ManagedTransactionLogIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionLogIterator;
import org.rocksdb.WriteBatch;

/**
 * Class used to test OMDBUpdatesHandler.
 */
public class TestOMDBUpdatesHandler {

  @TempDir
  private Path temporaryFolder;

  private OMMetadataManager omMetadataManager;
  private OMMetadataManager reconOmMetadataManager;
  private final OMDBDefinition omdbDefinition = OMDBDefinition.get();
  private Random random = new Random();

  private OzoneConfiguration createNewTestPath(String folderName)
      throws IOException {
    OzoneConfiguration configuration = new OzoneConfiguration();
    Path tempDirPath =
        Files.createDirectory(temporaryFolder.resolve(folderName));
    ServerUtils.setOzoneMetaDirPath(configuration, tempDirPath.toString());
    return configuration;
  }

  @BeforeEach
  public void setUp() throws Exception {
    OzoneConfiguration configuration = createNewTestPath("config");
    omMetadataManager = new OmMetadataManagerImpl(configuration, null);

    OzoneConfiguration reconConfiguration = createNewTestPath("reconConfig");
    reconOmMetadataManager = new OmMetadataManagerImpl(reconConfiguration,
        null);
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


    Text tester = new Text("tester");
    OzoneTokenIdentifier identifier =
        new OzoneTokenIdentifier(tester, tester, tester);
    identifier.setOmCertSerialId("certID");
    identifier.setOmServiceId("");

    omMetadataManager.getDelegationTokenTable().put(identifier, 12345L);

    List<byte[]> writeBatches = getBytesFromOmMetaManager(0);
    OMDBUpdatesHandler omdbUpdatesHandler = captureEvents(writeBatches);

    List<OMDBUpdateEvent> events = omdbUpdatesHandler.getEvents();
    assertEquals(4, events.size());

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
    omMetadataManager.getVolumeTable()
        .delete(omMetadataManager.getVolumeKey("nonExistingVolume"));

    List<byte[]> writeBatches = getBytesFromOmMetaManager(3);
    OMDBUpdatesHandler omdbUpdatesHandler = captureEvents(writeBatches);

    List<OMDBUpdateEvent> events = omdbUpdatesHandler.getEvents();

    // Assert for non existent keys, no events will be captured and handled.
    assertEquals(2, events.size());

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
    assertEquals("key",
        ((OmKeyInfo)keyPutEvent.getValue()).getKeyName());
    assertNull(keyPutEvent.getOldValue());

    OMDBUpdateEvent keyUpdateEvent = events.get(2);
    assertEquals(UPDATE, keyUpdateEvent.getAction());
    assertEquals("/sampleVol/bucketOne/key", keyUpdateEvent.getKey());
    assertEquals("key_new",
        ((OmKeyInfo)keyUpdateEvent.getValue()).getKeyName());
    assertNotNull(keyUpdateEvent.getOldValue());
    assertEquals("key",
        ((OmKeyInfo)keyUpdateEvent.getOldValue()).getKeyName());

    OMDBUpdateEvent keyUpdateEvent2 = events.get(3);
    assertEquals(UPDATE, keyUpdateEvent2.getAction());
    assertEquals("/sampleVol/bucketOne/key", keyUpdateEvent2.getKey());
    assertEquals("key_new2",
        ((OmKeyInfo)keyUpdateEvent2.getValue()).getKeyName());
    assertNotNull(keyUpdateEvent2.getOldValue());
    assertEquals("key_new",
        ((OmKeyInfo)keyUpdateEvent2.getOldValue()).getKeyName());

    OMDBUpdateEvent keyDeleteEvent = events.get(4);
    assertEquals(DELETE, keyDeleteEvent.getAction());
    assertEquals("/sampleVol/bucketOne/key", keyDeleteEvent.getKey());
    assertEquals("key_new2",
        ((OmKeyInfo)keyDeleteEvent.getValue()).getKeyName());

    OMDBUpdateEvent keyDeleteEvent2 = events.get(5);
    assertEquals(DELETE, keyDeleteEvent2.getAction());
    assertEquals("/sampleVol/bucketOne/key", keyDeleteEvent2.getKey());
    assertEquals("key_new2",
        ((OmKeyInfo)keyDeleteEvent2.getValue()).getKeyName());

    OMDBUpdateEvent keyPut2 = events.get(6);
    assertEquals(PUT, keyPut2.getAction());
    assertEquals("/sampleVol/bucketOne/key", keyPut2.getKey());
    assertEquals("key_new2",
        ((OmKeyInfo)keyPut2.getValue()).getKeyName());
    assertNotNull(keyPut2.getOldValue());
    assertEquals("key_new2",
        ((OmKeyInfo)keyPut2.getOldValue()).getKeyName());
  }

  /**
   * Test to verify that events with duplicate keys in different tables
   * (FileTable and DirectoryTable) are handled correctly without causing
   * ClassCastException or event conflicts.
   *
   * This test simulates creating a file, deleting the file, and then creating
   * a directory with the same name under the same parent ID in different tables.
   * It ensures that the events are correctly processed and stored in the
   * `omdbLatestUpdateEvents` map without causing any type mismatches or
   * exceptions.
   *
   * @throws Exception if any error occurs during the test execution.
   */
  @Test
  public void testEventsHavingDuplicateRocksDBKey() throws Exception {
    // Step 1: Create a file with the name "sameName" in the fileTable
    OmKeyInfo fileKeyInfo = getOmKeyInfo("sampleVol", "bucketOne", "sameName");
    omMetadataManager.getFileTable().put("/sampleVol/bucketOne/parentId/sameName", fileKeyInfo);

    // Step 2: Delete the file by adding its information to the deletedTable
    RepeatedOmKeyInfo repeatedKeyInfo = new RepeatedOmKeyInfo(fileKeyInfo, 0);
    omMetadataManager.getDeletedTable().put("/sampleVol/bucketOne/parentId/sameName", repeatedKeyInfo);

    // Step 3: Create a directory with the same name "sameName" in the directoryTable
    OmDirectoryInfo dirInfo = OmDirectoryInfo.newBuilder()
        .setName("sameName")
        .setParentObjectID(fileKeyInfo.getParentObjectID())
        .setObjectID(fileKeyInfo.getObjectID())
        .setCreationTime(System.currentTimeMillis())
        .setModificationTime(System.currentTimeMillis())
        .build();
    omMetadataManager.getDirectoryTable().put("/sampleVol/bucketOne/parentId/sameName", dirInfo);

    // Capture the events from the OM Metadata Manager
    List<byte[]> writeBatches = getBytesFromOmMetaManager(0);
    OMDBUpdatesHandler omdbUpdatesHandler = captureEvents(writeBatches);

    // Retrieve the captured events and assert the correct number of events
    List<OMDBUpdateEvent> events = omdbUpdatesHandler.getEvents();
    // Verify no events were discarded
    assertEquals(3, events.size());

    // Validate the file creation event
    OMDBUpdateEvent filePutEvent = events.get(0);
    assertEquals(PUT, filePutEvent.getAction());
    assertEquals("/sampleVol/bucketOne/parentId/sameName", filePutEvent.getKey());
    assertEquals("sameName", ((OmKeyInfo) filePutEvent.getValue()).getKeyName());
    assertNull(filePutEvent.getOldValue());

    // Validate the file deletion event
    OMDBUpdateEvent fileDeleteEvent = events.get(1);
    assertEquals(PUT, fileDeleteEvent.getAction());
    assertEquals("/sampleVol/bucketOne/parentId/sameName", fileDeleteEvent.getKey());
    assertEquals("sameName",
        ((RepeatedOmKeyInfo) fileDeleteEvent.getValue()).getOmKeyInfoList().get(0).getKeyName());

    // Validate the directory creation event
    OMDBUpdateEvent dirPutEvent = events.get(2);
    assertEquals(PUT, dirPutEvent.getAction());
    assertEquals("/sampleVol/bucketOne/parentId/sameName", dirPutEvent.getKey());
    assertEquals("sameName", ((OmDirectoryInfo) dirPutEvent.getValue()).getName());
    // There will be no old value as the key was not present in the directoryTable before
    assertNull(dirPutEvent.getOldValue());
  }

  @Test
  public void testGetKeyType() throws IOException {
    final String keyTable = omMetadataManager
        .getKeyTable(getBucketLayout()).getName();
    assertEquals(String.class,
        omdbDefinition.getColumnFamily(keyTable).getKeyType());

    final String delegationTokenTable = omMetadataManager
        .getDelegationTokenTable().getName();
    assertEquals(OzoneTokenIdentifier.class,
        omdbDefinition.getColumnFamily(delegationTokenTable).getKeyType());
  }

  @Test
  public void testGetValueType() throws IOException {
    final String keyTable = omMetadataManager
        .getKeyTable(getBucketLayout()).getName();
    assertEquals(OmKeyInfo.class,
        omdbDefinition.getColumnFamily(keyTable).getValueType());

    final String volumeTable = omMetadataManager.getVolumeTable().getName();
    assertEquals(OmVolumeArgs.class,
        omdbDefinition.getColumnFamily(volumeTable).getValueType());

    final String bucketTable = omMetadataManager.getBucketTable().getName();
    assertEquals(OmBucketInfo.class,
        omdbDefinition.getColumnFamily(bucketTable).getValueType());
  }

  @Nonnull
  private List<byte[]> getBytesFromOmMetaManager(int getUpdatesSince)
      throws RocksDBException, IOException {
    RDBStore rdbStore = (RDBStore) omMetadataManager.getStore();
    final RocksDatabase rocksDB = rdbStore.getDb();
    // Get all updates from source DB
    ManagedTransactionLogIterator logIterator =
        rocksDB.getUpdatesSince(getUpdatesSince);
    List<byte[]> writeBatches = new ArrayList<>();

    while (logIterator.get().isValid()) {
      TransactionLogIterator.BatchResult result =
          logIterator.get().getBatch();
      result.writeBatch().markWalTerminationPoint();
      WriteBatch writeBatch = result.writeBatch();
      writeBatches.add(writeBatch.data());
      logIterator.get().next();
    }
    return writeBatches;
  }

  @Nonnull
  private OMDBUpdatesHandler captureEvents(List<byte[]> writeBatches)
      throws RocksDBException {
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
