/*
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

package org.apache.hadoop.ozone.freon.containergenerator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.RocksDBConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo.Builder;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserVolumeInfo;
import org.apache.hadoop.util.Time;

import com.codahale.metrics.Timer;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Container generator for OM metadata.
 */
@Command(name = "cgom",
    description = "Offline container metadata generator for Ozone Manager",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class GeneratorOm extends BaseGenerator implements
    Callable<Void> {

  @Option(names = {"-v", "--volume"},
      description = "Name of the bucket which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "vol1")
  private String volumeName;

  @Option(names = {"-b", "--bucket"},
      description = "Name of the bucket which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "bucket1")
  private String bucketName;

  private DBStore omDb;

  private Table<String, OmKeyInfo> omKeyTable;

  private Timer timer;

  private OzoneConfiguration config;

  @Override
  public Void call() throws Exception {
    init();
    setThreadNo(1);

    config = createOzoneConfiguration();

    File metaDir = OMStorage.getOmDbDir(config);

    RocksDBConfiguration rocksDBConfiguration =
        config.getObject(RocksDBConfiguration.class);

    DBStoreBuilder dbStoreBuilder =
        DBStoreBuilder.newBuilder(config,
            rocksDBConfiguration)
            .setName(OM_DB_NAME)
            .setPath(metaDir.toPath());

    OmMetadataManagerImpl.addOMTablesAndCodecs(dbStoreBuilder);

    omDb = dbStoreBuilder.build();

    // initialization: create one bucket and volume in OM.
    writeOmBucketVolume();

    omKeyTable = omDb.getTable(OmMetadataManagerImpl.KEY_TABLE, String.class,
        OmKeyInfo.class);

    timer = getMetrics().timer("om-generator");
    runTests(this::writeOmKeys);

    omDb.close();
    return null;
  }

  public void writeOmKeys(long index) throws Exception {
    timer.time((Callable<Void>) () -> {

      long containerId = getContainerIdOffset() + index;

      int keyPerContainer = getKeysPerContainer(config);
      BatchOperation omKeyTableBatchOperation = omDb.initBatchOperation();
      for (long localId = 0; localId < keyPerContainer; localId++) {
        BlockID blockId = new BlockID(containerId, localId);
        writeOmData(localId, blockId, omKeyTableBatchOperation);
      }
      commitAndResetOMKeyTableBatchOperation(omKeyTableBatchOperation);
      return null;
    });

  }

  private void writeOmBucketVolume() throws IOException {

    Table<String, OmVolumeArgs> volTable =
        omDb.getTable(OmMetadataManagerImpl.VOLUME_TABLE, String.class,
            OmVolumeArgs.class);

    String admin = getUserId();
    String owner = getUserId();

    OmVolumeArgs omVolumeArgs = new OmVolumeArgs.Builder().setVolume(volumeName)
        .setAdminName(admin)
        .setCreationTime(Time.now())
        .setOwnerName(owner)
        .setObjectID(1L)
        .setUpdateID(1L)
        .setQuotaInBytes(100L)
        .addOzoneAcls(OzoneAcl.toProtobuf(
            new OzoneAcl(IAccessAuthorizer.ACLIdentityType.WORLD, "",
                IAccessAuthorizer.ACLType.ALL, ACCESS)))
        .addOzoneAcls(OzoneAcl.toProtobuf(
            new OzoneAcl(IAccessAuthorizer.ACLIdentityType.USER, getUserId(),
                IAccessAuthorizer.ACLType.ALL, ACCESS))
        ).build();

    volTable.put("/" + volumeName, omVolumeArgs);

    final Table<String, PersistedUserVolumeInfo> userTable =
        omDb.getTable(OmMetadataManagerImpl.USER_TABLE, String.class,
            PersistedUserVolumeInfo.class);

    PersistedUserVolumeInfo currentUserVolumeInfo =
        userTable.get(getUserId());

    if (currentUserVolumeInfo == null) {
      currentUserVolumeInfo = PersistedUserVolumeInfo.newBuilder()
          .addVolumeNames(volumeName)
          .build();

    } else if (!currentUserVolumeInfo.getVolumeNamesList()
        .contains(volumeName)) {

      currentUserVolumeInfo = PersistedUserVolumeInfo.newBuilder()
          .addAllVolumeNames(currentUserVolumeInfo.getVolumeNamesList())
          .addVolumeNames(volumeName)
          .build();
    }

    userTable.put(getUserId(), currentUserVolumeInfo);

    Table<String, OmBucketInfo> bucketTable =
        omDb.getTable(OmMetadataManagerImpl.BUCKET_TABLE, String.class,
            OmBucketInfo.class);

    OmBucketInfo omBucketInfo = new OmBucketInfo.Builder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName).build();
    bucketTable.put("/" + volumeName + "/" + bucketName, omBucketInfo);
  }

  private void addDirectoryKey(
      String keyName,
      BatchOperation omKeyTableBatchOperation
  ) throws IOException {
    OmKeyInfo l3DirInfo = new Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(0)
        .setCreationTime(System.currentTimeMillis())
        .setModificationTime(System.currentTimeMillis())
        .setReplicationFactor(ReplicationFactor.ONE)
        .setReplicationType(ReplicationType.RATIS)
        .build();
    omKeyTable.putWithBatch(omKeyTableBatchOperation,
        "/" + volumeName + "/" + bucketName + "/" + keyName, l3DirInfo);
  }

  private void writeOmData(
      long l,
      BlockID blockId,
      BatchOperation omKeyTableBatchOperation
  ) throws IOException {

    List<OmKeyLocationInfo> omkl = new ArrayList<>();
    omkl.add(new OmKeyLocationInfo.Builder()
        .setBlockID(blockId)
        .setLength(getKeySize())
        .setOffset(0)
        .build());

    OmKeyLocationInfoGroup infoGroup = new OmKeyLocationInfoGroup(0, omkl);

    long l4n = l % 1_000;
    long l3n = l / 1_000 % 1_000;
    long l2n = l / 1_000_000 % 1_000;
    long l1n = l / 1_000_000_000 % 1_000;

    String level3 = "L3-" + l3n;
    String level2 = "L2-" + l2n;
    String level1 = "L1-" + l1n;

    if (l2n == 0 && l3n == 0 && l4n == 0) {
      // create l1 directory
      addDirectoryKey(level1 + "/", omKeyTableBatchOperation);
    }

    if (l3n == 0 && l4n == 0) {
      // create l2 directory
      addDirectoryKey(level1 + "/" + level2 + "/", omKeyTableBatchOperation);
    }

    if (l4n == 0) {
      // create l3 directory
      addDirectoryKey(level1 + "/" + level2 + "/" + level3 + "/",
          omKeyTableBatchOperation);
    }

    String keyName =
        "/vol1/bucket1/" + level1 + "/" + level2 + "/" + level3 + "/key" + l;

    OmKeyInfo keyInfo = new Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(level1 + "/" + level2 + "/" + level3 + "/key" + l)
        .setDataSize(getKeySize())
        .setCreationTime(System.currentTimeMillis())
        .setModificationTime(System.currentTimeMillis())
        .setReplicationFactor(
            ReplicationFactor.valueOf(ReplicationFactor.THREE_VALUE))
        .setReplicationType(ReplicationType.STAND_ALONE)
        .addOmKeyLocationInfoGroup(infoGroup)
        .build();
    omKeyTable.putWithBatch(omKeyTableBatchOperation, keyName, keyInfo);

  }

  private void commitAndResetOMKeyTableBatchOperation(
      BatchOperation omKeyTableBatchOperation
  ) throws IOException {
    omDb.commitBatchOperation(omKeyTableBatchOperation);
    omKeyTableBatchOperation.close();
  }
}
