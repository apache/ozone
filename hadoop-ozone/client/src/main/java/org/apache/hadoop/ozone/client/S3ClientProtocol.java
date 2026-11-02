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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneFsServerDefaults;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.protocol.ListStatusLightOptions;
import org.apache.hadoop.ozone.om.helpers.AssumeRoleResponseInfo;
import org.apache.hadoop.ozone.om.helpers.CallerIdentityInfo;
import org.apache.hadoop.ozone.om.helpers.DeleteTenantState;
import org.apache.hadoop.ozone.om.helpers.ErrorInfo;
import org.apache.hadoop.ozone.om.helpers.LeaseKeyInfo;
import org.apache.hadoop.ozone.om.helpers.ListStatusResult;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleConfiguration;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatusLight;
import org.apache.hadoop.ozone.om.helpers.ReadConsistency;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.helpers.S3VolumeContext;
import org.apache.hadoop.ozone.om.helpers.TenantStateList;
import org.apache.hadoop.ozone.om.helpers.TenantUserInfoValue;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRoleInfo;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse;
import org.apache.hadoop.ozone.snapshot.ListSnapshotDiffJobResponse;
import org.apache.hadoop.ozone.snapshot.ListSnapshotResponse;
import org.apache.hadoop.ozone.snapshot.SnapshotCountResponse;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.hadoop.ozone.snapshot.SubmitSnapshotDiffResponse;
import org.apache.hadoop.security.token.Token;

/** Delegates ClientProtocol calls for S3 Gateway clients. */
final class S3ClientProtocol implements ClientProtocol {
  private final ClientProtocol delegate;

  S3ClientProtocol(ClientProtocol delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate == null");
  }

  @Override
  public List<OMRoleInfo> getOmRoleInfos() throws IOException {
    return delegate.getOmRoleInfos();
  }

  @Override
  public void createVolume(String volumeName) throws IOException {
    delegate.createVolume(volumeName);
  }

  @Override
  public void createVolume(String volumeName, VolumeArgs args) throws IOException {
    delegate.createVolume(volumeName, args);
  }

  @Override
  public boolean setVolumeOwner(String volumeName, String owner) throws IOException {
    return delegate.setVolumeOwner(volumeName, owner);
  }

  @Override
  public void setVolumeQuota(String volumeName, long quotaInNamespace, long quotaInBytes) throws IOException {
    delegate.setVolumeQuota(volumeName, quotaInNamespace, quotaInBytes);
  }

  @Override
  public OzoneVolume getVolumeDetails(String volumeName) throws IOException {
    return delegate.getVolumeDetails(volumeName);
  }

  @Override
  public S3VolumeContext getS3VolumeContext() throws IOException {
    return delegate.getS3VolumeContext();
  }

  @Override
  public OzoneKey headS3Object(String bucketName, String keyName) throws IOException {
    return delegate.headS3Object(bucketName, keyName);
  }

  @Override
  public OzoneKey headS3Object(String bucketName, String keyName, int partNumber) throws IOException {
    return delegate.headS3Object(bucketName, keyName, partNumber);
  }

  @Override
  public S3HeadObjectAttributes headS3ObjectAttributes(String bucketName, String keyName) throws IOException {
    return delegate.headS3ObjectAttributes(bucketName, keyName);
  }

  @Override
  public OzoneKeyDetails getS3KeyDetails(String bucketName, String keyName) throws IOException {
    return delegate.getS3KeyDetails(bucketName, keyName);
  }

  @Override
  public OzoneKeyDetails getS3KeyDetails(String bucketName, String keyName, int partNumber) throws IOException {
    return delegate.getS3KeyDetails(bucketName, keyName, partNumber);
  }

  @Override
  public OzoneVolume buildOzoneVolume(OmVolumeArgs volume) {
    return delegate.buildOzoneVolume(volume);
  }

  @Override
  @Deprecated
  public boolean checkVolumeAccess(String volumeName, OzoneAcl acl) throws IOException {
    return delegate.checkVolumeAccess(volumeName, acl);
  }

  @Override
  public void deleteVolume(String volumeName) throws IOException {
    delegate.deleteVolume(volumeName);
  }

  @Override
  public List<OzoneVolume> listVolumes(String volumePrefix, String prevVolume, int maxListResult) throws IOException {
    return delegate.listVolumes(volumePrefix, prevVolume, maxListResult);
  }

  @Override
  public List<OzoneVolume> listVolumes(String user, String volumePrefix, String prevVolume, int maxListResult)
      throws IOException {
    return delegate.listVolumes(user, volumePrefix, prevVolume, maxListResult);
  }

  @Override
  public void createBucket(String volumeName, String bucketName) throws IOException {
    delegate.createBucket(volumeName, bucketName);
  }

  @Override
  public void createBucket(String volumeName, String bucketName, BucketArgs bucketArgs) throws IOException {
    delegate.createBucket(volumeName, bucketName, bucketArgs);
  }

  @Override
  public void setBucketVersioning(String volumeName, String bucketName, Boolean versioning) throws IOException {
    delegate.setBucketVersioning(volumeName, bucketName, versioning);
  }

  @Override
  public void setBucketStorageType(String volumeName, String bucketName, StorageType storageType) throws IOException {
    delegate.setBucketStorageType(volumeName, bucketName, storageType);
  }

  @Override
  public void deleteBucket(String volumeName, String bucketName) throws IOException {
    delegate.deleteBucket(volumeName, bucketName);
  }

  @Override
  @Deprecated
  public void checkBucketAccess(String volumeName, String bucketName) throws IOException {
    delegate.checkBucketAccess(volumeName, bucketName);
  }

  @Override
  public OzoneBucket getBucketDetails(String volumeName, String bucketName) throws IOException {
    return delegate.getBucketDetails(volumeName, bucketName);
  }

  @Override
  public List<OzoneBucket> listBuckets(String volumeName, String bucketPrefix, String prevBucket, int maxListResult,
      boolean hasSnapshot) throws IOException {
    return delegate.listBuckets(volumeName, bucketPrefix, prevBucket, maxListResult, hasSnapshot);
  }

  @Override
  public OzoneOutputStream createKeyIfNoneMatch(String volumeName, String bucketName, String keyName, long size,
      String excludedETag, ReplicationConfig replicationConfig, Map<String, String> metadata, Map<String, String> tags,
      boolean derivedKeyPiggyBacking) throws IOException {
    return delegate.createKeyIfNoneMatch(volumeName, bucketName, keyName, size, excludedETag, replicationConfig,
        metadata, tags, derivedKeyPiggyBacking);
  }

  @Override
  public OzoneDataStreamOutput createStreamKeyIfNoneMatch(String volumeName, String bucketName, String keyName,
      long size, String excludedETag, ReplicationConfig replicationConfig, Map<String, String> metadata,
      Map<String, String> tags, boolean derivedKeyPiggyBacking) throws IOException {
    return delegate.createStreamKeyIfNoneMatch(volumeName, bucketName, keyName, size, excludedETag, replicationConfig,
        metadata, tags, derivedKeyPiggyBacking);
  }

  @Override
  @Deprecated
  public OzoneOutputStream createKey(String volumeName, String bucketName, String keyName, long size,
      ReplicationType type, ReplicationFactor factor, Map<String, String> metadata) throws IOException {
    return delegate.createKey(volumeName, bucketName, keyName, size, type, factor, metadata);
  }

  @Override
  public OzoneOutputStream createKey(String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig, Map<String, String> metadata) throws IOException {
    return delegate.createKey(volumeName, bucketName, keyName, size, replicationConfig, metadata);
  }

  @Override
  public OzoneOutputStream rewriteKey(String volumeName, String bucketName, String keyName, long size,
      long existingKeyGeneration, ReplicationConfig replicationConfig, Map<String, String> metadata)
      throws IOException {
    return delegate.rewriteKey(volumeName, bucketName, keyName, size, existingKeyGeneration, replicationConfig,
        metadata);
  }

  @Override
  public OzoneOutputStream createKeyIfNotExists(String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig, Map<String, String> metadata, Map<String, String> tags) throws IOException {
    return delegate.createKeyIfNotExists(volumeName, bucketName, keyName, size, replicationConfig, metadata, tags);
  }

  @Override
  public OzoneOutputStream rewriteKeyIfMatch(String volumeName, String bucketName, String keyName, long size,
      String expectedETag, ReplicationConfig replicationConfig, Map<String, String> metadata, Map<String, String> tags)
      throws IOException {
    return delegate.rewriteKeyIfMatch(volumeName, bucketName, keyName, size, expectedETag, replicationConfig, metadata,
        tags);
  }

  @Override
  public OzoneOutputStream createKey(String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig, Map<String, String> metadata, Map<String, String> tags) throws IOException {
    return delegate.createKey(volumeName, bucketName, keyName, size, replicationConfig, metadata, tags);
  }

  @Override
  public OzoneDataStreamOutput createStreamKey(String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig, Map<String, String> metadata) throws IOException {
    return delegate.createStreamKey(volumeName, bucketName, keyName, size, replicationConfig, metadata);
  }

  @Override
  public OzoneDataStreamOutput createStreamKey(String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig, Map<String, String> metadata, Map<String, String> tags) throws IOException {
    return delegate.createStreamKey(volumeName, bucketName, keyName, size, replicationConfig, metadata, tags);
  }

  @Override
  public OzoneDataStreamOutput createStreamKeyIfNotExists(String volumeName, String bucketName, String keyName,
      long size, ReplicationConfig replicationConfig, Map<String, String> metadata, Map<String, String> tags)
      throws IOException {
    return delegate.createStreamKeyIfNotExists(volumeName, bucketName, keyName, size, replicationConfig, metadata,
        tags);
  }

  @Override
  public OzoneDataStreamOutput rewriteStreamKeyIfMatch(String volumeName, String bucketName, String keyName, long size,
      String expectedETag, ReplicationConfig replicationConfig, Map<String, String> metadata, Map<String, String> tags)
      throws IOException {
    return delegate.rewriteStreamKeyIfMatch(volumeName, bucketName, keyName, size, expectedETag, replicationConfig,
        metadata, tags);
  }

  @Override
  public OzoneOutputStream createKey(String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig, Map<String, String> metadata, Map<String, String> tags,
      boolean derivedKeyPiggyBacking) throws IOException {
    return delegate.createKey(volumeName, bucketName, keyName, size, replicationConfig, metadata, tags,
        derivedKeyPiggyBacking);
  }

  @Override
  public OzoneOutputStream createKeyIfNotExists(String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig, Map<String, String> metadata, Map<String, String> tags,
      boolean derivedKeyPiggyBacking) throws IOException {
    return delegate.createKeyIfNotExists(volumeName, bucketName, keyName, size, replicationConfig, metadata, tags,
        derivedKeyPiggyBacking);
  }

  @Override
  public OzoneOutputStream rewriteKeyIfMatch(String volumeName, String bucketName, String keyName, long size,
      String expectedETag, ReplicationConfig replicationConfig, Map<String, String> metadata, Map<String, String> tags,
      boolean derivedKeyPiggyBacking) throws IOException {
    return delegate.rewriteKeyIfMatch(volumeName, bucketName, keyName, size, expectedETag, replicationConfig, metadata,
        tags, derivedKeyPiggyBacking);
  }

  @Override
  public OzoneOutputStream createMultipartKey(String volumeName, String bucketName, String keyName, long size,
      int partNumber, String uploadID, boolean derivedKeyPiggyBacking) throws IOException {
    return delegate.createMultipartKey(volumeName, bucketName, keyName, size, partNumber, uploadID,
        derivedKeyPiggyBacking);
  }

  @Override
  public OzoneDataStreamOutput createStreamKey(String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig, Map<String, String> metadata, Map<String, String> tags,
      boolean derivedKeyPiggyBacking) throws IOException {
    return delegate.createStreamKey(volumeName, bucketName, keyName, size, replicationConfig, metadata, tags,
        derivedKeyPiggyBacking);
  }

  @Override
  public OzoneDataStreamOutput createStreamKeyIfNotExists(String volumeName, String bucketName, String keyName,
      long size, ReplicationConfig replicationConfig, Map<String, String> metadata, Map<String, String> tags,
      boolean derivedKeyPiggyBacking) throws IOException {
    return delegate.createStreamKeyIfNotExists(volumeName, bucketName, keyName, size, replicationConfig, metadata, tags,
        derivedKeyPiggyBacking);
  }

  @Override
  public OzoneDataStreamOutput rewriteStreamKeyIfMatch(String volumeName, String bucketName, String keyName, long size,
      String expectedETag, ReplicationConfig replicationConfig, Map<String, String> metadata, Map<String, String> tags,
      boolean derivedKeyPiggyBacking) throws IOException {
    return delegate.rewriteStreamKeyIfMatch(volumeName, bucketName, keyName, size, expectedETag, replicationConfig,
        metadata, tags, derivedKeyPiggyBacking);
  }

  @Override
  public OzoneDataStreamOutput createMultipartStreamKey(String volumeName, String bucketName, String keyName, long size,
      int partNumber, String uploadID, boolean derivedKeyPiggyBacking) throws IOException {
    return delegate.createMultipartStreamKey(volumeName, bucketName, keyName, size, partNumber, uploadID,
        derivedKeyPiggyBacking);
  }

  @Override
  public OzoneInputStream getKey(String volumeName, String bucketName, String keyName) throws IOException {
    return delegate.getKey(volumeName, bucketName, keyName);
  }

  @Override
  public OmKeyInfo getKeyInfo(String volumeName, String bucketName, String keyName, boolean forceUpdateContainerCache)
      throws IOException {
    return delegate.getKeyInfo(volumeName, bucketName, keyName, forceUpdateContainerCache);
  }

  @Override
  public void deleteKey(String volumeName, String bucketName, String keyName, boolean recursive) throws IOException {
    delegate.deleteKey(volumeName, bucketName, keyName, recursive);
  }

  @Override
  public void deleteKey(String volumeName, String bucketName, String keyName, boolean recursive, String expectedETag)
      throws IOException {
    delegate.deleteKey(volumeName, bucketName, keyName, recursive, expectedETag);
  }

  @Override
  public void deleteKeys(String volumeName, String bucketName, List<String> keyNameList) throws IOException {
    delegate.deleteKeys(volumeName, bucketName, keyNameList);
  }

  @Override
  public Map<String, ErrorInfo> deleteKeys(String volumeName, String bucketName, List<String> keyNameList,
      boolean quiet) throws IOException {
    return delegate.deleteKeys(volumeName, bucketName, keyNameList, quiet);
  }

  @Override
  public void renameKey(String volumeName, String bucketName, String fromKeyName, String toKeyName) throws IOException {
    delegate.renameKey(volumeName, bucketName, fromKeyName, toKeyName);
  }

  @Override
  @Deprecated
  public void renameKeys(String volumeName, String bucketName, Map<String, String> keyMap) throws IOException {
    delegate.renameKeys(volumeName, bucketName, keyMap);
  }

  @Override
  public List<OzoneKey> listKeys(String volumeName, String bucketName, String keyPrefix, String prevKey,
      int maxListResult) throws IOException {
    return delegate.listKeys(volumeName, bucketName, keyPrefix, prevKey, maxListResult);
  }

  @Override
  public OzoneKeyDetails getKeyDetails(String volumeName, String bucketName, String keyName) throws IOException {
    return delegate.getKeyDetails(volumeName, bucketName, keyName);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  @Deprecated
  public OmMultipartInfo initiateMultipartUpload(String volumeName, String bucketName, String keyName,
      ReplicationType type, ReplicationFactor factor) throws IOException {
    return delegate.initiateMultipartUpload(volumeName, bucketName, keyName, type, factor);
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String volumeName, String bucketName, String keyName,
      ReplicationConfig replicationConfig) throws IOException {
    return delegate.initiateMultipartUpload(volumeName, bucketName, keyName, replicationConfig);
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String volumeName, String bucketName, String keyName,
      ReplicationConfig replicationConfig, Map<String, String> metadata) throws IOException {
    return delegate.initiateMultipartUpload(volumeName, bucketName, keyName, replicationConfig, metadata);
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(String volumeName, String bucketName, String keyName,
      ReplicationConfig replicationConfig, Map<String, String> metadata, Map<String, String> tags) throws IOException {
    return delegate.initiateMultipartUpload(volumeName, bucketName, keyName, replicationConfig, metadata, tags);
  }

  @Override
  public OzoneOutputStream createMultipartKey(String volumeName, String bucketName, String keyName, long size,
      int partNumber, String uploadID) throws IOException {
    return delegate.createMultipartKey(volumeName, bucketName, keyName, size, partNumber, uploadID);
  }

  @Override
  public OzoneDataStreamOutput createMultipartStreamKey(String volumeName, String bucketName, String keyName, long size,
      int partNumber, String uploadID) throws IOException {
    return delegate.createMultipartStreamKey(volumeName, bucketName, keyName, size, partNumber, uploadID);
  }

  @Override
  public OmMultipartUploadCompleteInfo completeMultipartUpload(String volumeName, String bucketName, String keyName,
      String uploadID, Map<Integer, String> partsMap) throws IOException {
    return delegate.completeMultipartUpload(volumeName, bucketName, keyName, uploadID, partsMap);
  }

  @Override
  public OmMultipartUploadCompleteInfo completeMultipartUpload(String volumeName, String bucketName, String keyName,
      String uploadID, Map<Integer, String> partsMap, Long expectedDataGeneration, String expectedETag)
      throws IOException {
    return delegate.completeMultipartUpload(volumeName, bucketName, keyName, uploadID, partsMap, expectedDataGeneration,
        expectedETag);
  }

  @Override
  public OmMultipartUploadCompleteInfo completeMultipartUpload(String volumeName, String bucketName, String keyName,
      String uploadID, Map<Integer, String> partsMap, Long expectedDataGeneration, String expectedETag,
      String excludedETag) throws IOException {
    return delegate.completeMultipartUpload(volumeName, bucketName, keyName, uploadID, partsMap, expectedDataGeneration,
        expectedETag, excludedETag);
  }

  @Override
  public void abortMultipartUpload(String volumeName, String bucketName, String keyName, String uploadID)
      throws IOException {
    delegate.abortMultipartUpload(volumeName, bucketName, keyName, uploadID);
  }

  @Override
  public void abortS3MultipartUpload(String bucketName, String keyName, String uploadID) throws IOException {
    delegate.abortS3MultipartUpload(bucketName, keyName, uploadID);
  }

  @Override
  public OzoneMultipartUploadPartListParts listParts(String volumeName, String bucketName, String keyName,
      String uploadID, int partNumberMarker, int maxParts) throws IOException {
    return delegate.listParts(volumeName, bucketName, keyName, uploadID, partNumberMarker, maxParts);
  }

  @Override
  public OzoneMultipartUploadList listMultipartUploads(String volumename, String bucketName, String prefix,
      String keyMarker, String uploadIdMarker, int maxUploads) throws IOException {
    return delegate.listMultipartUploads(volumename, bucketName, prefix, keyMarker, uploadIdMarker, maxUploads);
  }

  @Override
  public Token<OzoneTokenIdentifier> getDelegationToken(Text renewer) throws IOException {
    return delegate.getDelegationToken(renewer);
  }

  @Override
  public long renewDelegationToken(Token<OzoneTokenIdentifier> token) throws IOException {
    return delegate.renewDelegationToken(token);
  }

  @Override
  public void cancelDelegationToken(Token<OzoneTokenIdentifier> token) throws IOException {
    delegate.cancelDelegationToken(token);
  }

  @Override
  @Nonnull
  public S3SecretValue getS3Secret(String kerberosID) throws IOException {
    return delegate.getS3Secret(kerberosID);
  }

  @Override
  public S3SecretValue getS3Secret(String kerberosID, boolean createIfNotExist) throws IOException {
    return delegate.getS3Secret(kerberosID, createIfNotExist);
  }

  @Override
  public S3SecretValue setS3Secret(String accessId, String secretKey) throws IOException {
    return delegate.setS3Secret(accessId, secretKey);
  }

  @Override
  public void revokeS3Secret(String kerberosID) throws IOException {
    delegate.revokeS3Secret(kerberosID);
  }

  @Override
  public void createTenant(String tenantId) throws IOException {
    delegate.createTenant(tenantId);
  }

  @Override
  public void createTenant(String tenantId, TenantArgs tenantArgs) throws IOException {
    delegate.createTenant(tenantId, tenantArgs);
  }

  @Override
  public DeleteTenantState deleteTenant(String tenantId) throws IOException {
    return delegate.deleteTenant(tenantId);
  }

  @Override
  public S3SecretValue tenantAssignUserAccessId(String username, String tenantId, String accessId) throws IOException {
    return delegate.tenantAssignUserAccessId(username, tenantId, accessId);
  }

  @Override
  public void tenantRevokeUserAccessId(String accessId) throws IOException {
    delegate.tenantRevokeUserAccessId(accessId);
  }

  @Override
  public void tenantAssignAdmin(String accessId, String tenantId, boolean delegated) throws IOException {
    delegate.tenantAssignAdmin(accessId, tenantId, delegated);
  }

  @Override
  public void tenantRevokeAdmin(String accessId, String tenantId) throws IOException {
    delegate.tenantRevokeAdmin(accessId, tenantId);
  }

  @Override
  public TenantUserInfoValue tenantGetUserInfo(String userPrincipal) throws IOException {
    return delegate.tenantGetUserInfo(userPrincipal);
  }

  @Override
  public TenantUserList listUsersInTenant(String tenantId, String prefix) throws IOException {
    return delegate.listUsersInTenant(tenantId, prefix);
  }

  @Override
  public TenantStateList listTenant() throws IOException {
    return delegate.listTenant();
  }

  @Override
  public OzoneFsServerDefaults getServerDefaults() throws IOException {
    return delegate.getServerDefaults();
  }

  @Override
  public OzoneManagerVersion getOmVersion() {
    return delegate.getOmVersion();
  }

  @Override
  public KeyProvider getKeyProvider() throws IOException {
    return delegate.getKeyProvider();
  }

  @Override
  public URI getKeyProviderUri() throws IOException {
    return delegate.getKeyProviderUri();
  }

  @Override
  public String getCanonicalServiceName() {
    return delegate.getCanonicalServiceName();
  }

  @Override
  public OzoneFileStatus getOzoneFileStatus(String volumeName, String bucketName, String keyName) throws IOException {
    return delegate.getOzoneFileStatus(volumeName, bucketName, keyName);
  }

  @Override
  public OzoneFileStatus getOzoneFileStatus(String volumeName, String bucketName, String keyName, boolean headOp)
      throws IOException {
    return delegate.getOzoneFileStatus(volumeName, bucketName, keyName, headOp);
  }

  @Override
  public void createDirectory(String volumeName, String bucketName, String keyName) throws IOException {
    delegate.createDirectory(volumeName, bucketName, keyName);
  }

  @Override
  public OzoneInputStream readFile(String volumeName, String bucketName, String keyName) throws IOException {
    return delegate.readFile(volumeName, bucketName, keyName);
  }

  @Override
  public OzoneInputStream readFile(String volumeName, String bucketName, String keyName,
      boolean requireFileSystemLayout) throws IOException {
    return delegate.readFile(volumeName, bucketName, keyName, requireFileSystemLayout);
  }

  @Override
  @Deprecated
  public OzoneOutputStream createFile(String volumeName, String bucketName, String keyName, long size,
      ReplicationType type, ReplicationFactor factor, boolean overWrite, boolean recursive) throws IOException {
    return delegate.createFile(volumeName, bucketName, keyName, size, type, factor, overWrite, recursive);
  }

  @Override
  public OzoneOutputStream createFile(String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig, boolean overWrite, boolean recursive) throws IOException {
    return delegate.createFile(volumeName, bucketName, keyName, size, replicationConfig, overWrite, recursive);
  }

  @Override
  public OzoneDataStreamOutput createStreamFile(String volumeName, String bucketName, String keyName, long size,
      ReplicationConfig replicationConfig, boolean overWrite, boolean recursive) throws IOException {
    return delegate.createStreamFile(volumeName, bucketName, keyName, size, replicationConfig, overWrite, recursive);
  }

  @Override
  public List<OzoneFileStatus> listStatus(String volumeName, String bucketName, String keyName, boolean recursive,
      String startKey, long numEntries) throws IOException {
    return delegate.listStatus(volumeName, bucketName, keyName, recursive, startKey, numEntries);
  }

  @Override
  public List<OzoneFileStatus> listStatus(String volumeName, String bucketName, String keyName, boolean recursive,
      String startKey, long numEntries, boolean allowPartialPrefixes) throws IOException {
    return delegate.listStatus(volumeName, bucketName, keyName, recursive, startKey, numEntries, allowPartialPrefixes);
  }

  @Override
  public List<OzoneFileStatus> listStatus(ListStatusLightOptions options) throws IOException {
    return delegate.listStatus(options);
  }

  @Override
  public List<OzoneFileStatusLight> listStatusLight(ListStatusLightOptions options) throws IOException {
    return delegate.listStatusLight(options);
  }

  @Override
  public ListStatusResult<OzoneFileStatus> listStatusPage(ListStatusLightOptions options) throws IOException {
    return delegate.listStatusPage(options);
  }

  @Override
  public ListStatusResult<OzoneFileStatusLight> listStatusLightPage(ListStatusLightOptions options) throws IOException {
    return delegate.listStatusLightPage(options);
  }

  @Override
  public List<OzoneFileStatusLight> listStatusLight(String volumeName, String bucketName, String keyName,
      boolean recursive, String startKey, long numEntries, boolean allowPartialPrefixes) throws IOException {
    return delegate.listStatusLight(volumeName, bucketName, keyName, recursive, startKey, numEntries,
        allowPartialPrefixes);
  }

  @Override
  public boolean addAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    return delegate.addAcl(obj, acl);
  }

  @Override
  public boolean removeAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    return delegate.removeAcl(obj, acl);
  }

  @Override
  public boolean setAcl(OzoneObj obj, List<OzoneAcl> acls) throws IOException {
    return delegate.setAcl(obj, acls);
  }

  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    return delegate.getAcl(obj);
  }

  @Override
  public OzoneManagerProtocol getOzoneManagerClient() {
    return delegate.getOzoneManagerClient();
  }

  @Override
  public void setBucketQuota(String volumeName, String bucketName, long quotaInNamespace, long quotaInBytes)
      throws IOException {
    delegate.setBucketQuota(volumeName, bucketName, quotaInNamespace, quotaInBytes);
  }

  @Override
  public void setReplicationConfig(String volumeName, String bucketName, ReplicationConfig replicationConfig)
      throws IOException {
    delegate.setReplicationConfig(volumeName, bucketName, replicationConfig);
  }

  @Override
  @Deprecated
  public void setEncryptionKey(String volumeName, String bucketName, String bekName) throws IOException {
    delegate.setEncryptionKey(volumeName, bucketName, bekName);
  }

  @Override
  public OzoneKey headObject(String volumeName, String bucketName, String keyName) throws IOException {
    return delegate.headObject(volumeName, bucketName, keyName);
  }

  @Override
  public void setThreadLocalS3Auth(S3Auth s3Auth) {
    delegate.setThreadLocalS3Auth(s3Auth);
  }

  @Override
  public void setThreadLocalReadConsistency(ReadConsistency readConsistency) {
    delegate.setThreadLocalReadConsistency(readConsistency);
  }

  @Override
  public void setThreadLocalReadConsistency(ReadConsistency readConsistency, Long localLeaseLogLimit,
      Long localLeaseTimeMs) {
    delegate.setThreadLocalReadConsistency(readConsistency, localLeaseLogLimit, localLeaseTimeMs);
  }

  @Override
  public S3Auth getThreadLocalS3Auth() {
    return delegate.getThreadLocalS3Auth();
  }

  @Override
  public ReadConsistency getThreadLocalReadConsistency() {
    return delegate.getThreadLocalReadConsistency();
  }

  @Override
  public void clearThreadLocalS3Auth() {
    delegate.clearThreadLocalS3Auth();
  }

  @Override
  public void clearThreadLocalReadConsistency() {
    delegate.clearThreadLocalReadConsistency();
  }

  @Override
  public ThreadLocal<S3Auth> getS3CredentialsProvider() {
    return delegate.getS3CredentialsProvider();
  }

  @Override
  public boolean setBucketOwner(String volumeName, String bucketName, String owner) throws IOException {
    return delegate.setBucketOwner(volumeName, bucketName, owner);
  }

  @Override
  public Map<OmKeyLocationInfo, Map<DatanodeDetails, OzoneInputStream>> getKeysEveryReplicas(String volumeName,
      String bucketName, String keyName) throws IOException {
    return delegate.getKeysEveryReplicas(volumeName, bucketName, keyName);
  }

  @Override
  public String createSnapshot(String volumeName, String bucketName, String snapshotName) throws IOException {
    return delegate.createSnapshot(volumeName, bucketName, snapshotName);
  }

  @Override
  public void renameSnapshot(String volumeName, String bucketName, String snapshotOldName, String snapshotNewName)
      throws IOException {
    delegate.renameSnapshot(volumeName, bucketName, snapshotOldName, snapshotNewName);
  }

  @Override
  public void deleteSnapshot(String volumeName, String bucketName, String snapshotName) throws IOException {
    delegate.deleteSnapshot(volumeName, bucketName, snapshotName);
  }

  @Override
  public OzoneSnapshot getSnapshotInfo(String volumeName, String bucketName, String snapshotName) throws IOException {
    return delegate.getSnapshotInfo(volumeName, bucketName, snapshotName);
  }

  @Override
  @Deprecated
  public String printCompactionLogDag(String fileNamePrefix, String graphType) throws IOException {
    return delegate.printCompactionLogDag(fileNamePrefix, graphType);
  }

  @Override
  public ListSnapshotResponse listSnapshot(String volumeName, String bucketName, String snapshotPrefix,
      String prevSnapshot, int maxListResult) throws IOException {
    return delegate.listSnapshot(volumeName, bucketName, snapshotPrefix, prevSnapshot, maxListResult);
  }

  @Override
  public SnapshotCountResponse snapshotCount(String bucketFilter) throws IOException {
    return delegate.snapshotCount(bucketFilter);
  }

  @Override
  @Deprecated
  public SnapshotDiffResponse snapshotDiff(String volumeName, String bucketName, String fromSnapshot, String toSnapshot,
      String token, int pageSize, boolean forceFullDiff, boolean disableNativeDiff) throws IOException {
    return delegate.snapshotDiff(volumeName, bucketName, fromSnapshot, toSnapshot, token, pageSize, forceFullDiff,
        disableNativeDiff);
  }

  @Override
  public SnapshotDiffResponse snapshotDiff(String volumeName, String bucketName, String fromSnapshot, String toSnapshot,
      String token, int pageSize) throws IOException {
    return delegate.snapshotDiff(volumeName, bucketName, fromSnapshot, toSnapshot, token, pageSize);
  }

  @Override
  public SubmitSnapshotDiffResponse submitSnapshotDiff(String volumeName, String bucketName, String fromSnapshot,
      String toSnapshot, boolean forceFullDiff, boolean disableNativeDiff, boolean orderedReport)
      throws IOException {
    return delegate.submitSnapshotDiff(volumeName, bucketName, fromSnapshot, toSnapshot, forceFullDiff,
        disableNativeDiff, orderedReport);
  }

  @Override
  public CancelSnapshotDiffResponse cancelSnapshotDiff(String volumeName, String bucketName, String fromSnapshot,
      String toSnapshot) throws IOException {
    return delegate.cancelSnapshotDiff(volumeName, bucketName, fromSnapshot, toSnapshot);
  }

  @Override
  public ListSnapshotDiffJobResponse listSnapshotDiffJobs(String volumeName, String bucketName, String jobStatus,
      boolean listAllStatus, String prevSnapshotDiffJob, int maxListResult) throws IOException {
    return delegate.listSnapshotDiffJobs(volumeName, bucketName, jobStatus, listAllStatus, prevSnapshotDiffJob,
        maxListResult);
  }

  @Override
  public void setTimes(OzoneObj obj, String keyName, long mtime, long atime) throws IOException {
    delegate.setTimes(obj, keyName, mtime, atime);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    delegate.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public LeaseKeyInfo recoverLease(String volumeName, String bucketName, String keyName, boolean force)
      throws IOException {
    return delegate.recoverLease(volumeName, bucketName, keyName, force);
  }

  @Override
  public void recoverKey(OmKeyArgs args, long clientID) throws IOException {
    delegate.recoverKey(args, clientID);
  }

  @Override
  public Map<String, String> getObjectTagging(
      String volumeName, String bucketName, String keyName) throws IOException {
    return delegate.getObjectTagging(
        resolveVolumeName(volumeName), bucketName, keyName);
  }

  private String resolveVolumeName(String volumeName) throws IOException {
    if (volumeName != null || OzoneManagerVersion.S3_VOLUME_CONTEXT_RESOLUTION
        .isSupportedBy(delegate.getOmVersion())) {
      return volumeName;
    }
    return delegate.getS3VolumeContext().getOmVolumeArgs().getVolume();
  }

  @Override
  public void putObjectTagging(String volumeName, String bucketName, String keyName, Map<String, String> tags)
      throws IOException {
    delegate.putObjectTagging(volumeName, bucketName, keyName, tags);
  }

  @Override
  public void deleteObjectTagging(String volumeName, String bucketName, String keyName) throws IOException {
    delegate.deleteObjectTagging(volumeName, bucketName, keyName);
  }

  @Override
  public AssumeRoleResponseInfo assumeRole(String roleArn, String roleSessionName, int durationSeconds,
      String awsIamSessionPolicy, String requestId) throws IOException {
    return delegate.assumeRole(roleArn, roleSessionName, durationSeconds, awsIamSessionPolicy, requestId);
  }

  @Override
  public CallerIdentityInfo getCallerIdentity() throws IOException {
    return delegate.getCallerIdentity();
  }

  @Override
  public void revokeSTSToken(String originalAccessKeyId) throws IOException {
    delegate.revokeSTSToken(originalAccessKeyId);
  }

  @Override
  public OzoneLifecycleConfiguration getLifecycleConfiguration(String volumeName, String bucketName)
      throws IOException {
    return delegate.getLifecycleConfiguration(volumeName, bucketName);
  }

  @Override
  public void setLifecycleConfiguration(OmLifecycleConfiguration lifecycleConfiguration) throws IOException {
    delegate.setLifecycleConfiguration(lifecycleConfiguration);
  }

  @Override
  public void deleteLifecycleConfiguration(String volumeName, String bucketName) throws IOException {
    delegate.deleteLifecycleConfiguration(volumeName, bucketName);
  }

  @Override
  public Map<String, String> getBucketTagging(String volumeName, String bucketName) throws IOException {
    return delegate.getBucketTagging(volumeName, bucketName);
  }

  @Override
  public void putBucketTagging(String volumeName, String bucketName, Map<String, String> tags) throws IOException {
    delegate.putBucketTagging(volumeName, bucketName, tags);
  }

  @Override
  public void deleteBucketTagging(String volumeName, String bucketName) throws IOException {
    delegate.deleteBucketTagging(volumeName, bucketName);
  }
}
