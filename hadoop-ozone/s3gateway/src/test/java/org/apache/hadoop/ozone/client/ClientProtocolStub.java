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

package org.apache.hadoop.ozone.client;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneFsServerDefaults;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.DeleteTenantState;
import org.apache.hadoop.ozone.om.helpers.ErrorInfo;
import org.apache.hadoop.ozone.om.helpers.LeaseKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatusLight;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.helpers.S3VolumeContext;
import org.apache.hadoop.ozone.om.helpers.TenantStateList;
import org.apache.hadoop.ozone.om.helpers.TenantUserInfoValue;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse;
import org.apache.hadoop.ozone.snapshot.ListSnapshotDiffJobResponse;
import org.apache.hadoop.ozone.snapshot.ListSnapshotResponse;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.hadoop.security.token.Token;

/**
 * ClientProtocol implementation with in-memory state.
 */
public class ClientProtocolStub implements ClientProtocol {
  private static final String STUB_KERBEROS_ID = "stub_kerberos_id";
  private static final String STUB_SECRET = "stub_secret";
  private final ObjectStoreStub objectStoreStub;

  public ClientProtocolStub(ObjectStoreStub objectStoreStub) {
    this.objectStoreStub = objectStoreStub;
  }

  @Override
  public List<OzoneManagerProtocolProtos.OMRoleInfo> getOmRoleInfos()
      throws IOException {
    return null;
  }

  @Override
  public void createVolume(String volumeName) throws IOException {
  }

  @Override
  public void createVolume(String volumeName, VolumeArgs args)
      throws IOException {
  }

  @Override
  public boolean setVolumeOwner(String volumeName, String owner)
      throws IOException {
    return false;
  }

  @Override
  public void setVolumeQuota(String volumeName, long quotaInNamespace,
                             long quotaInBytes) throws IOException {
  }

  @Override
  public OzoneVolume getVolumeDetails(String volumeName) throws IOException {
    return objectStoreStub.getVolume(volumeName);
  }

  @Override
  public S3VolumeContext getS3VolumeContext() throws IOException {
    return null;
  }

  @Override
  public OzoneKey headS3Object(String bucketName, String keyName)
      throws IOException {
    return objectStoreStub.getS3Volume().getBucket(bucketName)
        .headObject(keyName);
  }

  @Override
  public OzoneKeyDetails getS3KeyDetails(String bucketName, String keyName)
      throws IOException {
    return objectStoreStub.getS3Volume().getBucket(bucketName).getKey(keyName);
  }

  @Override
  public OzoneKeyDetails getS3KeyDetails(String bucketName, String keyName,
                                         int partNumber)
      throws IOException {
    return objectStoreStub.getS3Volume().getBucket(bucketName).getKey(keyName);
  }

  @Override
  public OzoneVolume buildOzoneVolume(OmVolumeArgs volume) {
    return null;
  }

  @Override
  public boolean checkVolumeAccess(String volumeName, OzoneAcl acl)
      throws IOException {
    return false;
  }

  @Override
  public void deleteVolume(String volumeName) throws IOException {

  }

  @Override
  public List<OzoneVolume> listVolumes(String volumePrefix, String prevVolume,
                                       int maxListResult) throws IOException {
    return null;
  }

  @Override
  public List<OzoneVolume> listVolumes(String user, String volumePrefix,
                                       String prevVolume, int maxListResult)
      throws IOException {
    return null;
  }

  @Override
  public void createBucket(String volumeName, String bucketName)
      throws IOException {

  }

  @Override
  public void createBucket(String volumeName, String bucketName,
                           BucketArgs bucketArgs) throws IOException {

  }

  @Override
  public void setBucketVersioning(String volumeName, String bucketName,
                                  Boolean versioning) throws IOException {

  }

  @Override
  public void setBucketStorageType(String volumeName, String bucketName,
                                   StorageType storageType) throws IOException {

  }

  @Override
  public void deleteBucket(String volumeName, String bucketName)
      throws IOException {

  }

  @Override
  public void checkBucketAccess(String volumeName, String bucketName)
      throws IOException {

  }

  @Override
  public OzoneBucket getBucketDetails(String volumeName, String bucketName)
      throws IOException {
    return null;
  }

  @Override
  public List<OzoneBucket> listBuckets(String volumeName, String bucketPrefix,
                                       String prevBucket, int maxListResult,
                                       boolean hasSnapshot)
      throws IOException {
    return null;
  }

  @Override
  public OzoneOutputStream createKey(String volumeName, String bucketName,
                                     String keyName, long size,
                                     ReplicationType type,
                                     ReplicationFactor factor,
                                     Map<String, String> metadata)
      throws IOException {
    return getBucket(volumeName, bucketName).createKey(keyName, size);
  }

  @Override
  public OzoneOutputStream createKey(String volumeName, String bucketName,
                                     String keyName, long size,
                                     ReplicationConfig replicationConfig,
                                     Map<String, String> metadata)
      throws IOException {
    return getBucket(volumeName, bucketName)
        .createKey(keyName, size, replicationConfig, metadata);
  }

  @Override
  public OzoneOutputStream createKey(String volumeName, String bucketName,
                                     String keyName, long size,
                                     ReplicationConfig replicationConfig,
                                     Map<String, String> metadata,
                                     Map<String, String> tags) throws IOException {
    return getBucket(volumeName, bucketName)
        .createKey(keyName, size, replicationConfig, metadata, tags);
  }

  @Override
  public OzoneOutputStream rewriteKey(String volumeName, String bucketName, String keyName,
      long size, long existingKeyGeneration, ReplicationConfig replicationConfig,
      Map<String, String> metadata) throws IOException {
    return getBucket(volumeName, bucketName)
        .rewriteKey(keyName, size, existingKeyGeneration, replicationConfig, metadata);
  }

  @Override
  public OzoneInputStream getKey(String volumeName, String bucketName,
                                 String keyName) throws IOException {
    return getBucket(volumeName, bucketName).readKey(keyName);
  }

  @Override
  public OmKeyInfo getKeyInfo(String volumeName, String bucketName, String keyName,
      boolean forceUpdateContainerCache) throws IOException {
    return objectStoreStub.getClientProxy().getKeyInfo(
        volumeName, bucketName, keyName, forceUpdateContainerCache);
  }

  private OzoneBucket getBucket(String volumeName, String bucketName)
      throws IOException {
    return objectStoreStub.getVolume(volumeName).getBucket(bucketName);
  }

  @Override
  public void deleteKey(String volumeName, String bucketName, String keyName,
                        boolean recursive) throws IOException {
    getBucket(volumeName, bucketName).deleteKey(keyName);
  }

  @Override
  public void deleteKeys(String volumeName, String bucketName,
                         List<String> keyNameList) throws IOException {

  }

  @Override
  public Map<String, ErrorInfo> deleteKeys(String volumeName, String bucketName,
                                           List<String> keyNameList, boolean quiet)
      throws IOException {
    return new HashMap<>();
  }

  @Override
  public void renameKey(String volumeName, String bucketName,
                        String fromKeyName, String toKeyName)
      throws IOException {

  }

  @Override
  public void renameKeys(String volumeName, String bucketName,
                         Map<String, String> keyMap) throws IOException {

  }

  @Override
  public List<OzoneKey> listKeys(String volumeName, String bucketName,
                                 String keyPrefix, String prevKey,
                                 int maxListResult) throws IOException {
    return null;
  }

  @Override
  public OzoneKeyDetails getKeyDetails(String volumeName, String bucketName,
                                       String keyName) throws IOException {
    return getBucket(volumeName, bucketName).getKey(keyName);
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String volumeName,
                                                 String bucketName,
                                                 String keyName,
                                                 ReplicationType type,
                                                 ReplicationFactor factor)
      throws IOException {
    return null;
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String volumeName,
         String bucketName, String keyName, ReplicationConfig replicationConfig)
      throws IOException {
    return initiateMultipartUpload(volumeName, bucketName, keyName, replicationConfig, Collections.emptyMap());
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String volumeName,
         String bucketName, String keyName, ReplicationConfig replicationConfig,
         Map<String, String> metadata)
      throws IOException {
    return getBucket(volumeName, bucketName)
        .initiateMultipartUpload(keyName, replicationConfig, metadata);
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String volumeName,
         String bucketName, String keyName, ReplicationConfig replicationConfig,
         Map<String, String> metadata, Map<String, String> tags) throws IOException {
    return getBucket(volumeName, bucketName)
        .initiateMultipartUpload(keyName, replicationConfig, metadata, tags);
  }

  @Override
  public OzoneOutputStream createMultipartKey(String volumeName,
                                              String bucketName, String keyName,
                                              long size, int partNumber,
                                              String uploadID)
      throws IOException {
    return getBucket(volumeName, bucketName).createMultipartKey(keyName, size,
        partNumber, uploadID);
  }

  @Override
  public OmMultipartUploadCompleteInfo completeMultipartUpload(
      String volumeName, String bucketName, String keyName, String uploadID,
      Map<Integer, String> partsMap) throws IOException {
    return getBucket(volumeName, bucketName)
        .completeMultipartUpload(keyName, uploadID, partsMap);
  }

  @Override
  public void abortMultipartUpload(String volumeName, String bucketName,
                                   String keyName, String uploadID)
      throws IOException {
    getBucket(volumeName, bucketName).abortMultipartUpload(keyName, uploadID);
  }

  @Override
  public OzoneMultipartUploadPartListParts listParts(String volumeName,
                                                     String bucketName,
                                                     String keyName,
                                                     String uploadID,
                                                     int partNumberMarker,
                                                     int maxParts)
      throws IOException {
    return getBucket(volumeName, bucketName)
        .listParts(keyName, uploadID, partNumberMarker, maxParts);
  }

  @Override
  public OzoneMultipartUploadList listMultipartUploads(String volumename,
                                                       String bucketName,
                                                       String prefix,
                                                       String keyMarker,
                                                       String uploadIdMarker,
                                                       int maxUploads)
      throws IOException {
    return null;
  }

  @Override
  public Token<OzoneTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    return null;
  }

  @Override
  public long renewDelegationToken(Token<OzoneTokenIdentifier> token)
      throws IOException {
    return 0;
  }

  @Override
  public void cancelDelegationToken(Token<OzoneTokenIdentifier> token)
      throws IOException {

  }

  @Override
  @Nonnull
  public S3SecretValue getS3Secret(String kerberosID) throws IOException {
    return S3SecretValue.of(STUB_KERBEROS_ID, STUB_SECRET);
  }

  @Override
  public S3SecretValue getS3Secret(String kerberosID, boolean createIfNotExist)
      throws IOException {
    return null;
  }

  @Override
  public S3SecretValue setS3Secret(String accessId, String secretKey)
      throws IOException {
    return null;
  }

  @Override
  public void revokeS3Secret(String kerberosID) throws IOException {

  }

  @Override
  public void createTenant(String tenantId) throws IOException {

  }

  @Override
  public void createTenant(String tenantId, TenantArgs tenantArgs)
      throws IOException {

  }

  @Override
  public DeleteTenantState deleteTenant(String tenantId) throws IOException {
    return null;
  }

  @Override
  public S3SecretValue tenantAssignUserAccessId(String username,
                                                String tenantId,
                                                String accessId)
      throws IOException {
    return null;
  }

  @Override
  public void tenantRevokeUserAccessId(String accessId) throws IOException {

  }

  @Override
  public void tenantAssignAdmin(String accessId, String tenantId,
                                boolean delegated) throws IOException {

  }

  @Override
  public void tenantRevokeAdmin(String accessId, String tenantId)
      throws IOException {

  }

  @Override
  public TenantUserInfoValue tenantGetUserInfo(String userPrincipal)
      throws IOException {
    return null;
  }

  @Override
  public TenantUserList listUsersInTenant(String tenantId, String prefix)
      throws IOException {
    return null;
  }

  @Override
  public TenantStateList listTenant() throws IOException {
    return null;
  }

  @Override
  public OzoneFsServerDefaults getServerDefaults() throws IOException {
    return null;
  }

  @Override
  public KeyProvider getKeyProvider() throws IOException {
    return null;
  }

  @Override
  public URI getKeyProviderUri() throws IOException {
    return null;
  }

  @Override
  public String getCanonicalServiceName() {
    return null;
  }

  @Override
  public OzoneFileStatus getOzoneFileStatus(String volumeName,
                                            String bucketName, String keyName)
      throws IOException {
    return null;
  }

  @Override
  public void createDirectory(String volumeName, String bucketName,
                              String keyName) throws IOException {
    getBucket(volumeName, bucketName).createDirectory(keyName);
  }

  @Override
  public OzoneInputStream readFile(String volumeName, String bucketName,
                                   String keyName) throws IOException {
    return null;
  }

  @Override
  public OzoneOutputStream createFile(String volumeName, String bucketName,
                                      String keyName, long size,
                                      ReplicationType type,
                                      ReplicationFactor factor,
                                      boolean overWrite, boolean recursive)
      throws IOException {
    return null;
  }

  @Override
  public OzoneOutputStream createFile(String volumeName, String bucketName,
                                      String keyName, long size,
                                      ReplicationConfig replicationConfig,
                                      boolean overWrite, boolean recursive)
      throws IOException {
    return null;
  }

  @Override
  public List<OzoneFileStatus> listStatus(String volumeName, String bucketName,
                                          String keyName, boolean recursive,
                                          String startKey, long numEntries)
      throws IOException {
    return null;
  }

  @Override
  public List<OzoneFileStatus> listStatus(String volumeName, String bucketName,
                                          String keyName, boolean recursive,
                                          String startKey, long numEntries,
                                          boolean allowPartialPrefixes)
      throws IOException {
    return null;
  }

  @Override
  public List<OzoneFileStatusLight> listStatusLight(String volumeName,
      String bucketName, String keyName, boolean recursive, String startKey,
      long numEntries, boolean allowPartialPrefixes) throws IOException {
    return null;
  }

  @Override
  public boolean addAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    return false;
  }

  @Override
  public boolean removeAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    return false;
  }

  @Override
  public boolean setAcl(OzoneObj obj, List<OzoneAcl> acls) throws IOException {
    return false;
  }

  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    return null;
  }

  @Override
  public OzoneManagerProtocol getOzoneManagerClient() {
    return null;
  }

  @Override
  public void setBucketQuota(String volumeName, String bucketName,
                             long quotaInNamespace, long quotaInBytes)
      throws IOException {

  }

  @Override
  public void setReplicationConfig(String volumeName, String bucketName,
                                   ReplicationConfig replicationConfig)
      throws IOException {

  }

  @Deprecated
  @Override
  public void setEncryptionKey(String volumeName, String bucketName,
                               String bekName)
      throws IOException {

  }

  @Override
  public OzoneKey headObject(String volumeName, String bucketName,
                             String keyName) throws IOException {
    return getBucket(volumeName, bucketName).headObject(keyName);
  }

  @Override
  public void setThreadLocalS3Auth(S3Auth s3Auth) {

  }

  @Override
  public void setIsS3Request(boolean isS3Request) {

  }

  @Override
  public S3Auth getThreadLocalS3Auth() {
    return null;
  }

  @Override
  public void clearThreadLocalS3Auth() {

  }

  @Override
  public boolean setBucketOwner(String volumeName, String bucketName,
                                String owner) throws IOException {
    return false;
  }

  @Override
  public Map<OmKeyLocationInfo,
      Map<DatanodeDetails, OzoneInputStream>> getKeysEveryReplicas(
      String volumeName, String bucketName, String keyName) throws IOException {
    return null;
  }

  @Override
  public OzoneDataStreamOutput createStreamKey(
      String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig, Map<String, String> metadata)
      throws IOException {
    return null;
  }

  @Override
  public OzoneDataStreamOutput createStreamKey(
      String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig, Map<String, String> metadata,
      Map<String, String> tags) throws IOException {
    return null;
  }

  @Override
  public OzoneDataStreamOutput createMultipartStreamKey(
      String volumeName, String bucketName, String keyName, long size,
      int partNumber, String uploadID) throws IOException {
    return null;
  }

  @Override
  public OzoneDataStreamOutput createStreamFile(
      String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConf, boolean overWrite, boolean recursive)
      throws IOException {
    return null;
  }

  @Override
  public String createSnapshot(String volumeName,
      String bucketName, String snapshotName)
      throws IOException {
    return "";
  }

  @Override
  public void renameSnapshot(String volumeName, String bucketName,
      String snapshotOldName, String snapshotNewName)
      throws IOException {

  }

  @Override
  public ListSnapshotResponse listSnapshot(
      String volumeName, String bucketName, String snapshotPrefix,
      String prevSnapshot, int maxListResult) throws IOException {
    return null;
  }
  
  @Override
  public void deleteSnapshot(String volumeName,
      String bucketName, String snapshotName)
      throws IOException {

  }

  @Override
  public OzoneSnapshot getSnapshotInfo(String volumeName, String bucketName,
                                       String snapshotName) throws IOException {
    return null;
  }

  @Override
  @Deprecated
  public String printCompactionLogDag(String fileNamePrefix,
                                      String graphType) throws IOException {
    return null;
  }

  @Override
  public SnapshotDiffResponse snapshotDiff(String volumeName,
                                           String bucketName,
                                           String fromSnapshot,
                                           String toSnapshot,
                                           String token,
                                           int pageSize,
                                           boolean forceFullDiff,
                                           boolean disableNativeDiff)
      throws IOException {
    return null;
  }

  @Override
  public CancelSnapshotDiffResponse cancelSnapshotDiff(String volumeName,
                                                       String bucketName,
                                                       String fromSnapshot,
                                                       String toSnapshot)
      throws IOException {
    return null;
  }

  @Override
  public ListSnapshotDiffJobResponse listSnapshotDiffJobs(
      String volumeName,
      String bucketName,
      String jobStatus,
      boolean listAllStatus,
      String prevSnapshotDiffJob,
      int maxListResult) {
    return null;
  }

  @Override
  public void setTimes(OzoneObj obj, String keyName, long mtime, long atime)
      throws IOException {
  }

  @Override
  public LeaseKeyInfo recoverLease(String volumeName, String bucketName,
      String keyName, boolean force) throws IOException {
    return null;
  }

  @Override
  public void recoverKey(OmKeyArgs args, long clientID) throws IOException {

  }

  @Override
  public Map<String, String> getObjectTagging(String volumeName, String bucketName, String keyName) throws IOException {
    return getBucket(volumeName, bucketName).getObjectTagging(keyName);
  }

  @Override
  public void putObjectTagging(String volumeName, String bucketName, String keyName, Map<String, String> tags)
      throws IOException {
    getBucket(volumeName, bucketName).putObjectTagging(keyName, tags);
  }

  @Override
  public void deleteObjectTagging(String volumeName, String bucketName, String keyName) throws IOException {
    getBucket(volumeName, bucketName).deleteObjectTagging(keyName);
  }

}
