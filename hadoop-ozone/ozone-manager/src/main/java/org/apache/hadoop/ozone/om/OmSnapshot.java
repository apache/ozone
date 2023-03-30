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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Metadata Reading class for OM Snapshots.
 *
 * This abstraction manages all the metadata key/acl reading from a
 * rocksDb instance, for OM snapshots.  It's basically identical to
 * the ozoneManager OmMetadataReader with two exceptions: 
 *
 * 1. Its keymanager and prefix manager contain an OmMetadataManager
 * that reads from a snapshot.  
 *
 * 2. It normalizes/denormalizes each request as it comes in to
 * remove/replace the ".snapshot/snapshotName" prefix.
 */
public class OmSnapshot implements IOmMetadataReader, Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(OmSnapshot.class);

  private static final AuditLogger AUDIT = new AuditLogger(
      AuditLoggerType.OMLOGGER);

  private final OmMetadataReader omMetadataReader;
  private final String volumeName;
  private final String bucketName;
  private final String snapshotName;
  // To access snapshot checkpoint DB metadata
  private final OMMetadataManager omMetadataManager;

  public OmSnapshot(KeyManager keyManager,
                    PrefixManager prefixManager,
                    OzoneManager ozoneManager,
                    String volumeName,
                    String bucketName,
                    String snapshotName) {
    omMetadataReader = new OmMetadataReader(keyManager, prefixManager,
        ozoneManager, LOG, AUDIT,
        OmSnapshotMetrics.getInstance());
    this.snapshotName = snapshotName;
    this.bucketName = bucketName;
    this.volumeName = volumeName;
    this.omMetadataManager = keyManager.getMetadataManager();
  }

  @Override
  public OmKeyInfo lookupKey(OmKeyArgs args) throws IOException {
    return denormalizeOmKeyInfo(omMetadataReader.lookupKey(
        normalizeOmKeyArgs(args)));
  }

  @Override
  public KeyInfoWithVolumeContext getKeyInfo(final OmKeyArgs args,
                                             boolean assumeS3Context)
      throws IOException {
    return denormalizeKeyInfoWithVolumeContext(
        omMetadataReader.getKeyInfo(normalizeOmKeyArgs(args),
        assumeS3Context));
  }

  @Override
  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
      String startKey, long numEntries, boolean allowPartialPrefixes)
      throws IOException {
    List<OzoneFileStatus> l = omMetadataReader
        .listStatus(normalizeOmKeyArgs(args),
        recursive, normalizeKeyName(startKey), numEntries,
        allowPartialPrefixes);
    return l.stream().map(this::denormalizeOzoneFileStatus)
        .collect(Collectors.toList());
  }

  @Override
  public OzoneFileStatus getFileStatus(OmKeyArgs args) throws IOException {
    return denormalizeOzoneFileStatus(
        omMetadataReader.getFileStatus(normalizeOmKeyArgs(args)));
  }

  @Override
  public OmKeyInfo lookupFile(OmKeyArgs args) throws IOException {
    return denormalizeOmKeyInfo(omMetadataReader
        .lookupFile(normalizeOmKeyArgs(args)));
  }

  @Override
  public List<OmKeyInfo> listKeys(String vname, String bname,
      String startKey, String keyPrefix, int maxKeys) throws IOException {
    List<OmKeyInfo> l = omMetadataReader.listKeys(vname, bname,
        normalizeKeyName(startKey), normalizeKeyName(keyPrefix), maxKeys);
    return l.stream().map(this::denormalizeOmKeyInfo)
        .collect(Collectors.toList());
  }

  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    // TODO: handle denormalization
    return omMetadataReader.getAcl(normalizeOzoneObj(obj));
  }

  private OzoneObj normalizeOzoneObj(OzoneObj o) {
    if (o == null) {
      return null;
    }

    return OzoneObjInfo.Builder.getBuilder(o.getResourceType(),
        o.getStoreType(), o.getVolumeName(), o.getBucketName(),
        normalizeKeyName(o.getKeyName()))
        // OzonePrefixPath field appears to only used by fso
        //  delete/rename requests which are not applicable to
        //  snapshots
        .setOzonePrefixPath(o.getOzonePrefixPathViewer()).build();

  }


  // Remove snapshot indicator from keyname
  private String normalizeKeyName(String keyname) {
    if (keyname == null) {
      return null;
    }
    String[] keyParts = keyname.split("/");
    if (OmSnapshotManager.isSnapshotKey(keyParts)) {
      // ".snapshot/name/" becomes ""
      if (keyParts.length == 2) {
        return "";
      }
      // ".snapshot/name/key/" becomes "key/"
      String normalizedKeyName = String.join("/",
          Arrays.copyOfRange(keyParts, 2, keyParts.length));
      if (keyname.endsWith("/")) {
        normalizedKeyName = normalizedKeyName + "/";
      }
      return normalizedKeyName;
    }
    return keyname;
  }

  // Restore snapshot indicator to keyanme
  private String denormalizeKeyName(String keyname) {
    if (keyname == null) {
      return null;
    }
    return OmSnapshotManager.getSnapshotPrefix(snapshotName) + keyname;
  }

  private OmKeyInfo denormalizeOmKeyInfo(OmKeyInfo keyInfo) {
    if (keyInfo == null) {
      return null;
    }
    OmKeyInfo denormalized = keyInfo.copyObject();
    denormalized.setKeyName(denormalizeKeyName(keyInfo.getKeyName()));
    return denormalized;
  }

  private OmKeyArgs normalizeOmKeyArgs(OmKeyArgs args) {
    if (args == null) {
      return null;
    }
    return args.toBuilder().setKeyName(normalizeKeyName(
        args.getKeyName())).build();
  }

  private  OzoneFileStatus denormalizeOzoneFileStatus(
      OzoneFileStatus fileStatus) {
    if (fileStatus == null) {
      return null;
    }
    OmKeyInfo omKeyInfo;
    // if this is the filestatus for the whole bucket
    if (fileStatus.getKeyInfo() == null) {
      // denormalization requires that the keyname in the filestatus
      // keyInfo struct be updated to include the snapshot indicator.
      // But the bucket filestatus has a null keyInfo struct.

      //  so this code adds a dummy keyinfo struct just for
      //  denormalization.
      //  See KeyManagerImpl.getOzoneFileStatus()
      omKeyInfo = createDenormalizedBucketKeyInfo();
    } else {
      omKeyInfo = denormalizeOmKeyInfo(fileStatus.getKeyInfo());
    }
    return new OzoneFileStatus(
        omKeyInfo, fileStatus.getBlockSize(), fileStatus.isDirectory());
  }

  private KeyInfoWithVolumeContext denormalizeKeyInfoWithVolumeContext(
      KeyInfoWithVolumeContext k) {
    return new KeyInfoWithVolumeContext.Builder()
        .setKeyInfo(denormalizeOmKeyInfo(k.getKeyInfo()))
        .setVolumeArgs(k.getVolumeArgs().orElse(null))
        .setUserPrincipal(k.getUserPrincipal().orElse(null))
        .build();
  }

  private OmKeyInfo createDenormalizedBucketKeyInfo() {
    return new OmKeyInfo.Builder()
      .setVolumeName(volumeName)
      .setBucketName(bucketName)
      .setKeyName(denormalizeKeyName(""))
      .setOmKeyLocationInfos(Collections.singletonList(
          new OmKeyLocationInfoGroup(0, new ArrayList<>())))
      .setCreationTime(Time.now())
      .setModificationTime(Time.now())
      .setDataSize(0)
      .setReplicationConfig(RatisReplicationConfig
          .getInstance(HddsProtos.ReplicationFactor.ZERO))
      .build();
  }

  public String getName() {
    return snapshotName;
  }

  @Override
  public void close() throws IOException {
    omMetadataManager.getStore().close();
  }

  @VisibleForTesting
  public OMMetadataManager getMetadataManager() {
    return omMetadataManager;
  }
}
