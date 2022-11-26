/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.client;

import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.DeleteTenantState;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
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
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * ClientProtocol implementation with in-memory state.
 */
public class ClientProtocolStub implements ClientProtocol {
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
                                       String prevBucket, int maxListResult)
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
  public OzoneInputStream getKey(String volumeName, String bucketName,
                                 String keyName) throws IOException {
    return getBucket(volumeName, bucketName).readKey(keyName);
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
  public List<RepeatedOmKeyInfo> listTrash(String volumeName, String bucketName,
                                           String startKeyName,
                                           String keyPrefix, int maxKeys)
      throws IOException {
    return null;
  }

  @Override
  public boolean recoverTrash(String volumeName, String bucketName,
                              String keyName, String destinationBucket)
      throws IOException {
    return false;
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
    return getBucket(volumeName, bucketName)
        .initiateMultipartUpload(keyName, replicationConfig);
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
                                                       String prefix)
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
  public S3SecretValue getS3Secret(String kerberosID) throws IOException {
    return null;
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

  @Override
  public OzoneKey headObject(String volumeName, String bucketName,
                             String keyName) throws IOException {
    return getBucket(volumeName, bucketName).headObject(keyName);
  }

  @Override
  public void setThreadLocalS3Auth(S3Auth s3Auth) {

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
}
