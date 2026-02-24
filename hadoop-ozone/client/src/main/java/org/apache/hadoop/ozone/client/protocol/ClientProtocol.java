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

package org.apache.hadoop.ozone.client.protocol;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.net.URI;
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
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadList;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneSnapshot;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.TenantArgs;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRoleInfo;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.snapshot.CancelSnapshotDiffResponse;
import org.apache.hadoop.ozone.snapshot.ListSnapshotDiffJobResponse;
import org.apache.hadoop.ozone.snapshot.ListSnapshotResponse;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.Token;

/**
 * An implementer of this interface is capable of connecting to Ozone Cluster
 * and perform client operations. The protocol used for communication is
 * determined by the implementation class specified by
 * property <code>ozone.client.protocol</code>. The build-in implementation
 * includes: {@link org.apache.hadoop.ozone.client.rpc.RpcClient} for RPC.
 */
@KerberosInfo(serverPrincipal = OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY)
public interface ClientProtocol {

  /**
   * List of OM node Ids and their Ratis server roles.
   * @return List of OM server roles
   * @throws IOException
   */
  List<OMRoleInfo> getOmRoleInfos() throws IOException;

  /**
   * Creates a new Volume.
   * @param volumeName Name of the Volume
   * @throws IOException
   */
  void createVolume(String volumeName)
      throws IOException;

  /**
   * Creates a new Volume with properties set in VolumeArgs.
   * @param volumeName Name of the Volume
   * @param args Properties to be set for the Volume
   * @throws IOException
   */
  void createVolume(String volumeName, VolumeArgs args)
      throws IOException;

  /**
   * Sets the owner of volume.
   * @param volumeName Name of the Volume
   * @param owner to be set for the Volume
   * @return true if operation succeeded, false if specified user is
   *         already the owner.
   * @throws IOException
   */
  boolean setVolumeOwner(String volumeName, String owner) throws IOException;

  /**
   * Set Volume Quota.
   * @param volumeName Name of the Volume
   * @param quotaInNamespace The maximum number of buckets in this volume.
   * @param quotaInBytes The maximum size this volume can be used.
   * @throws IOException
   */
  void setVolumeQuota(String volumeName, long quotaInNamespace,
      long quotaInBytes) throws IOException;

  /**
   * Returns {@link OzoneVolume}.
   * @param volumeName Name of the Volume
   * @return {@link OzoneVolume}
   * @throws IOException
   * */
  OzoneVolume getVolumeDetails(String volumeName)
      throws IOException;

  /**
   * @return Raw GetS3VolumeContextResponse.
   * S3Auth won't be updated with actual userPrincipal by this call.
   * @throws IOException
   */
  S3VolumeContext getS3VolumeContext() throws IOException;

  /**
   * Returns OzoneKey that contains the application generated/visible
   * metadata for an Ozone Object in S3 context.
   *
   * If Key exists, return returns OzoneKey.
   * If Key does not exist, throws an exception with error code KEY_NOT_FOUND
   *
   * @return OzoneKey which gives basic information about the key.
   */
  OzoneKey headS3Object(String bucketName, String keyName) throws IOException;

  /**
   * Get OzoneKey in S3 context.
   * @param bucketName Name of the Bucket
   * @param keyName Key name
   * @return {@link OzoneKey}
   * @throws IOException
   */
  OzoneKeyDetails getS3KeyDetails(String bucketName, String keyName)
      throws IOException;

  /**
   * Get OzoneKey in S3 context.
   * @param bucketName Name of the Bucket
   * @param keyName Key name
   * @param partNumber Multipart-upload part number
   * @return {@link OzoneKey}
   * @throws IOException
   */
  OzoneKeyDetails getS3KeyDetails(String bucketName, String keyName,
                                  int partNumber)
      throws IOException;

  OzoneVolume buildOzoneVolume(OmVolumeArgs volume);

  /**
   * Checks if a Volume exists and the user with a role specified has access
   * to the Volume.
   * @param volumeName Name of the Volume
   * @param acl requested acls which needs to be checked for access
   * @return Boolean - True if the user with a role can access the volume.
   * This is possible for owners of the volume and admin users
   * @throws IOException
   */
  @Deprecated
  boolean checkVolumeAccess(String volumeName, OzoneAcl acl)
      throws IOException;

  /**
   * Deletes an empty Volume.
   * @param volumeName Name of the Volume
   * @throws IOException
   */
  void deleteVolume(String volumeName) throws IOException;

  /**
   * Lists all volumes in the cluster that matches the volumePrefix,
   * size of the returned list depends on maxListResult. If volume prefix
   * is null, returns all the volumes. The caller has to make multiple calls
   * to read all volumes.
   *
   * @param volumePrefix Volume prefix to match
   * @param prevVolume Starting point of the list, this volume is excluded
   * @param maxListResult Max number of volumes to return.
   * @return {@code List<OzoneVolume>}
   * @throws IOException
   */
  List<OzoneVolume> listVolumes(String volumePrefix, String prevVolume,
                                int maxListResult)
      throws IOException;

  /**
   * Lists all volumes in the cluster that are owned by the specified
   * user and matches the volumePrefix, size of the returned list depends on
   * maxListResult. If the user is null, return volumes owned by current user.
   * If volume prefix is null, returns all the volumes. The caller has to make
   * multiple calls to read all volumes.
   *
   * @param user User Name
   * @param volumePrefix Volume prefix to match
   * @param prevVolume Starting point of the list, this volume is excluded
   * @param maxListResult Max number of volumes to return.
   * @return {@code List<OzoneVolume>}
   * @throws IOException
   */
  List<OzoneVolume> listVolumes(String user, String volumePrefix,
                                    String prevVolume, int maxListResult)
      throws IOException;

  /**
   * Creates a new Bucket in the Volume.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @throws IOException
   */
  void createBucket(String volumeName, String bucketName)
      throws IOException;

  /**
   * Creates a new Bucket in the Volume, with properties set in BucketArgs.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param bucketArgs Bucket Arguments
   * @throws IOException
   */
  void createBucket(String volumeName, String bucketName,
                    BucketArgs bucketArgs)
      throws IOException;


  /**
   * Enables or disables Bucket Versioning.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param versioning True to enable Versioning, False to disable.
   * @throws IOException
   */
  void setBucketVersioning(String volumeName, String bucketName,
                           Boolean versioning)
      throws IOException;

  /**
   * Sets the Storage Class of a Bucket.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param storageType StorageType to be set
   * @throws IOException
   */
  void setBucketStorageType(String volumeName, String bucketName,
                            StorageType storageType)
      throws IOException;

  /**
   * Deletes a bucket if it is empty.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @throws IOException
   */
  void deleteBucket(String volumeName, String bucketName)
      throws IOException;

  /**
   * True if the bucket exists and user has read access
   * to the bucket else throws Exception.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @throws IOException
   */
  @Deprecated
  void checkBucketAccess(String volumeName, String bucketName)
      throws IOException;

  /**
   * Returns {@link OzoneBucket}.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @return {@link OzoneBucket}
   * @throws IOException
   */
  OzoneBucket getBucketDetails(String volumeName, String bucketName)
      throws IOException;

  /**
   * Returns the List of Buckets in the Volume that matches the bucketPrefix,
   * size of the returned list depends on maxListResult. The caller has to make
   * multiple calls to read all volumes.
   *
   * @param volumeName    Name of the Volume
   * @param bucketPrefix  Bucket prefix to match
   * @param prevBucket    Starting point of the list, this bucket is excluded
   * @param maxListResult Max number of buckets to return.
   * @param hasSnapshot   flag to list the buckets which have snapshot.
   * @return {@code List<OzoneBucket>}
   * @throws IOException
   */
  List<OzoneBucket> listBuckets(String volumeName, String bucketPrefix,
                                String prevBucket, int maxListResult,
                                boolean hasSnapshot)
      throws IOException;

  /**
   * Writes a key in an existing bucket.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Name of the Key
   * @param size Size of the data
   * @param metadata Custom key value metadata
   * @return {@link OzoneOutputStream}
   * @deprecated Use {@link ClientProtocol#createKey(String, String, String, long, ReplicationConfig, Map)} instead.
   */
  @Deprecated
  OzoneOutputStream createKey(String volumeName, String bucketName,
                              String keyName, long size, ReplicationType type,
                              ReplicationFactor factor,
                              Map<String, String> metadata)
      throws IOException;

  /**
   * Writes a key in an existing bucket.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Name of the Key
   * @param size Size of the data
   * @param metadata Custom key value metadata
   * @return {@link OzoneOutputStream}
   *
   */
  OzoneOutputStream createKey(String volumeName, String bucketName,
      String keyName, long size, ReplicationConfig replicationConfig,
      Map<String, String> metadata)
      throws IOException;

  /**
   * This API allows to atomically update an existing key. The key read before invoking this API
   * should remain unchanged for this key to be written. This is controlled by the generation
   * field in the existing Key param. If the key is replaced or updated the generation will change. If the
   * generation has changed since the existing Key was read, either the initial key create will fail,
   * or the key will fail to commit after the data has been written as the checks are carried out
   * both at key open and commit time.
   *
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Existing key to rewrite. This must exist in the bucket.
   * @param size The size of the new key
   * @param existingKeyGeneration The generation of the existing key which is checked for changes at key create
   *                              and commit time.
   * @param replicationConfig The replication configuration for the key to be rewritten.
   * @param metadata custom key value metadata
   * @return {@link OzoneOutputStream}
   * @throws IOException
   */
  OzoneOutputStream rewriteKey(String volumeName, String bucketName, String keyName,
      long size, long existingKeyGeneration, ReplicationConfig replicationConfig,
       Map<String, String> metadata) throws IOException;

  /**
   * Creates a key only if it does not exist (S3 If-None-Match: * semantics).
   *
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Name of the Key
   * @param size Size of the data
   * @param replicationConfig The replication configuration
   * @param metadata custom key value metadata
   * @param tags Tags used for S3 object tags
   * @return {@link OzoneOutputStream}
   * @throws OMException with KEY_ALREADY_EXISTS if key exists
   */
  OzoneOutputStream createKeyIfNotExists(String volumeName, String bucketName,
      String keyName, long size, ReplicationConfig replicationConfig,
      Map<String, String> metadata, Map<String, String> tags)
      throws IOException;

  /**
   * Rewrites a key only if its ETag matches (S3 If-Match semantics).
   *
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Name of the Key
   * @param size Size of the data
   * @param expectedETag The ETag value the existing key must have
   * @param replicationConfig The replication configuration
   * @param metadata custom key value metadata
   * @param tags Tags used for S3 object tags
   * @return {@link OzoneOutputStream}
   * @throws OMException with ETAG_MISMATCH, ETAG_NOT_AVAILABLE, or KEY_NOT_FOUND
   */
  @SuppressWarnings("checkstyle:parameternumber")
  OzoneOutputStream rewriteKeyIfMatch(String volumeName, String bucketName,
      String keyName, long size, String expectedETag,
      ReplicationConfig replicationConfig, Map<String, String> metadata,
      Map<String, String> tags) throws IOException;

  /**
   * Writes a key in an existing bucket.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Name of the Key
   * @param size Size of the data
   * @param metadata Custom key value metadata
   * @param tags Tags used for S3 object tags
   * @return {@link OzoneOutputStream}
   *
   */
  OzoneOutputStream createKey(String volumeName, String bucketName,
      String keyName, long size, ReplicationConfig replicationConfig,
      Map<String, String> metadata, Map<String, String> tags)
      throws IOException;

  /**
   * Writes a key in an existing bucket.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Name of the Key
   * @param size Size of the data
   * @param metadata custom key value metadata
   * @return {@link OzoneDataStreamOutput}
   *
   */
  OzoneDataStreamOutput createStreamKey(String volumeName, String bucketName,
      String keyName, long size, ReplicationConfig replicationConfig,
      Map<String, String> metadata)
      throws IOException;

  /**
   * Writes a key in an existing bucket.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Name of the Key
   * @param size Size of the data
   * @param metadata custom key value metadata
   * @param tags Tags used for S3 object tags
   * @return {@link OzoneDataStreamOutput}
   *
   */
  OzoneDataStreamOutput createStreamKey(String volumeName, String bucketName,
      String keyName, long size, ReplicationConfig replicationConfig,
      Map<String, String> metadata, Map<String, String> tags)
      throws IOException;

  /**
   * Reads a key from an existing bucket.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Name of the Key
   * @return {@link OzoneInputStream}
   * @throws IOException
   */
  OzoneInputStream getKey(String volumeName, String bucketName, String keyName)
      throws IOException;

  /**
   * Reads key info from an existing bucket.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Name of the Key
   * @param forceUpdateContainerCache if true force OM to update container cache location from SCM
   * @return {@link OmKeyInfo}
   * @throws IOException
   */
  OmKeyInfo getKeyInfo(String volumeName, String bucketName, String keyName,
      boolean forceUpdateContainerCache) throws IOException;

  /**
   * Deletes an existing key.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Name of the Key
   * @param recursive recursive deletion of all sub path keys if true,
   *                  otherwise non-recursive
   * @throws IOException
   */
  void deleteKey(String volumeName, String bucketName, String keyName,
                 boolean recursive)
      throws IOException;

  /**
   * Deletes keys through the list.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyNameList List of the Key
   * @throws IOException
   */
  void deleteKeys(String volumeName, String bucketName,
                  List<String> keyNameList)
      throws IOException;

  /**
   * Deletes keys through the list.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyNameList List of the Key
   * @param quiet flag to not throw exception if delete fails
   * @throws IOException
   */
  Map<String, ErrorInfo> deleteKeys(String volumeName, String bucketName,
                                    List<String> keyNameList, boolean quiet)
      throws IOException;

  /**
   * Renames an existing key within a bucket.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param fromKeyName Name of the Key to be renamed
   * @param toKeyName New name to be used for the Key
   * @throws IOException
   */
  void renameKey(String volumeName, String bucketName, String fromKeyName,
                 String toKeyName) throws IOException;

  /**
   * Renames existing keys within a bucket.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyMap The key is original key name nad value is new key name.
   * @throws IOException
   */
  @Deprecated
  void renameKeys(String volumeName, String bucketName,
                  Map<String, String> keyMap) throws IOException;

  /**
   * Returns list of Keys in {Volume/Bucket} that matches the keyPrefix,
   * size of the returned list depends on maxListResult. The caller has
   * to make multiple calls to read all keys.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyPrefix Bucket prefix to match
   * @param prevKey Starting point of the list, this key is excluded
   * @param maxListResult Max number of buckets to return.
   * @return {@code List<OzoneKey>}
   * @throws IOException
   */
  List<OzoneKey> listKeys(String volumeName, String bucketName,
                          String keyPrefix, String prevKey, int maxListResult)
      throws IOException;

  /**
   * Get OzoneKey.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Key name
   * @return {@link OzoneKey}
   * @throws IOException
   */
  OzoneKeyDetails getKeyDetails(String volumeName, String bucketName,
                                String keyName)
      throws IOException;

  /**
   * Close and release the resources.
   */
  void close() throws IOException;

  /**
   * Initiate Multipart upload.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param type
   * @param factor
   * @return {@link OmMultipartInfo}
   * @throws IOException
   */
  @Deprecated
  OmMultipartInfo initiateMultipartUpload(String volumeName, String
      bucketName, String keyName, ReplicationType type, ReplicationFactor
      factor) throws IOException;

  /**
   * Initiate Multipart upload.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Name of the Key
   * @param replicationConfig Replication Config
   * @return {@link OmMultipartInfo}
   * @throws IOException
   */
  OmMultipartInfo initiateMultipartUpload(String volumeName, String
      bucketName, String keyName, ReplicationConfig replicationConfig)
      throws IOException;

  /**
   * Initiate Multipart upload.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Name of the Key
   * @param replicationConfig Replication config
   * @param metadata Custom key value metadata
   * @return {@link OmMultipartInfo}
   * @throws IOException
   */
  OmMultipartInfo initiateMultipartUpload(String volumeName, String
      bucketName, String keyName, ReplicationConfig replicationConfig,
      Map<String, String> metadata)
      throws IOException;

  /**
   * Initiate Multipart upload.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Name of the Key
   * @param replicationConfig Replication config
   * @param metadata Custom key value metadata
   * @param tags Tags used for S3 object tags
   * @return {@link OmMultipartInfo}
   * @throws IOException
   */
  OmMultipartInfo initiateMultipartUpload(String volumeName, String
      bucketName, String keyName, ReplicationConfig replicationConfig,
      Map<String, String> metadata, Map<String, String> tags)
      throws IOException;

  /**
   * Create a part key for a multipart upload key.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param size
   * @param partNumber
   * @param uploadID
   * @return OzoneOutputStream
   * @throws IOException
   */
  OzoneOutputStream createMultipartKey(String volumeName, String bucketName,
                                       String keyName, long size,
                                       int partNumber, String uploadID)
      throws IOException;

  /**
   * Create a part key for a multipart upload key.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param size
   * @param partNumber
   * @param uploadID
   * @return OzoneDataStreamOutput
   * @throws IOException
   */
  OzoneDataStreamOutput createMultipartStreamKey(String volumeName,
                                                 String bucketName,
                                                 String keyName, long size,
                                                 int partNumber,
                                                 String uploadID)
      throws IOException;

  /**
   * Complete Multipart upload. This will combine all the parts and make the
   * key visible in ozone.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param uploadID
   * @param partsMap
   * @return OmMultipartUploadCompleteInfo
   * @throws IOException
   */
  OmMultipartUploadCompleteInfo completeMultipartUpload(String volumeName,
      String bucketName, String keyName, String uploadID,
      Map<Integer, String> partsMap) throws IOException;

  /**
   * Abort Multipart upload request for the given key with given uploadID.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param uploadID
   * @throws IOException
   */
  void abortMultipartUpload(String volumeName,
      String bucketName, String keyName, String uploadID) throws IOException;

  /**
   * Returns list of parts of a multipart upload key.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param uploadID
   * @param partNumberMarker - returns parts with part number which are greater
   * than this partNumberMarker.
   * @param maxParts
   * @return OmMultipartUploadListParts
   */
  OzoneMultipartUploadPartListParts listParts(String volumeName,
      String bucketName, String keyName, String uploadID, int partNumberMarker,
      int maxParts)  throws IOException;

  /**
   * Return with the inflight multipart uploads.
   */
  OzoneMultipartUploadList listMultipartUploads(String volumename,
      String bucketName, String prefix, String keyMarker, String uploadIdMarker, int maxUploads) throws IOException;

  /**
   * Get a valid Delegation Token.
   *
   * @param renewer the designated renewer for the token
   * @return Token<OzoneDelegationTokenSelector>
   * @throws IOException
   */
  Token<OzoneTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException;

  /**
   * Renew an existing delegation token.
   *
   * @param token delegation token obtained earlier
   * @return the new expiration time
   * @throws IOException
   */
  long renewDelegationToken(Token<OzoneTokenIdentifier> token)
      throws IOException;

  /**
   * Cancel an existing delegation token.
   *
   * @param token delegation token
   * @throws IOException
   */
  void cancelDelegationToken(Token<OzoneTokenIdentifier> token)
      throws IOException;

  /**
   * Returns S3 Secret given kerberos user.
   * Will generate a secret access key for the accessId (=kerberosID)
   * if it doesn't exist.
   * @param kerberosID Access ID
   * @return S3SecretValue
   * @throws IOException
   */
  @Nonnull S3SecretValue getS3Secret(String kerberosID) throws IOException;

  /**
   * Returns S3 Secret given kerberos user.
   * Optionally generate a secret access key for the accessId (=kerberosID)
   * if it doesn't exist if createIfNotExist is true.
   * When createIfNotExist is false and accessId (=kerberosID) doesn't
   * exist, OM throws OMException with ACCESSID_NOT_FOUND to the client.
   * @param kerberosID
   * @param createIfNotExist
   * @return S3SecretValue
   * @throws IOException
   */
  S3SecretValue getS3Secret(String kerberosID, boolean createIfNotExist)
          throws IOException;

  /**
   * Set secret key of a given accessId.
   * @param accessId
   * @param secretKey
   * @return S3SecretValue
   * @throws IOException
   */
  S3SecretValue setS3Secret(String accessId, String secretKey)
      throws IOException;

  /**
   * Revoke S3 Secret of given kerberos user.
   * @param kerberosID
   * @throws IOException
   */
  void revokeS3Secret(String kerberosID) throws IOException;

  /**
   * Create a tenant.
   * @param tenantId tenant name.
   * @throws IOException
   */
  void createTenant(String tenantId) throws IOException;

  /**
   * Create a tenant with args.
   *
   * @param tenantId
   * @param tenantArgs extra arguments e.g. volume name
   * @throws IOException
   */
  void createTenant(String tenantId, TenantArgs tenantArgs) throws IOException;

  /**
   * Delete a tenant.
   * @param tenantId tenant name.
   * @throws IOException
   * @return DeleteTenantState
   */
  DeleteTenantState deleteTenant(String tenantId) throws IOException;

  /**
   * Assign a user to a tenant.
   * @param username user name to be assigned.
   * @param tenantId tenant name.
   * @param accessId access ID.
   * @throws IOException
   */
  S3SecretValue tenantAssignUserAccessId(String username, String tenantId,
      String accessId) throws IOException;

  /**
   * Revoke a user accessId previously assign to a tenant.
   * @param accessId accessId to be revoked.
   * @throws IOException
   */
  void tenantRevokeUserAccessId(String accessId) throws IOException;

  /**
   * Assign admin role to an accessId in a tenant.
   * @param accessId access ID.
   * @param tenantId tenant name.
   * @param delegated true if making delegated admin.
   * @throws IOException
   */
  void tenantAssignAdmin(String accessId, String tenantId, boolean delegated)
      throws IOException;

  /**
   * Revoke admin role of an accessId from a tenant.
   * @param accessId access ID.
   * @param tenantId tenant name.
   * @throws IOException
   */
  void tenantRevokeAdmin(String accessId, String tenantId) throws IOException;

  /**
   * Get tenant info for a user.
   * @param userPrincipal Kerberos principal of a user.
   * @return TenantUserInfo
   * @throws IOException
   */
  TenantUserInfoValue tenantGetUserInfo(String userPrincipal)
      throws IOException;

  /**
   * Get List of users in a tenant.
   * @param tenantId tenant name
   * @param prefix optional prefix
   * @return List of username, accessIds in tenant.
   * @throws IOException on server error.
   */
  TenantUserList listUsersInTenant(String tenantId, String prefix)
      throws IOException;

  /**
   * List tenants.
   * @return TenantStateList
   * @throws IOException
   */
  TenantStateList listTenant() throws IOException;

  /**
   * Get server default values for a number of configuration params.
   * @return Default configuration from the server.
   * @throws IOException
   */
  OzoneFsServerDefaults getServerDefaults() throws IOException;

  /**
   * Get KMS client provider.
   * @return KMS client provider.
   * @throws IOException
   */
  KeyProvider getKeyProvider() throws IOException;

  /**
   * Get KMS client provider uri.
   * @return KMS client provider uri.
   * @throws IOException
   */
  URI getKeyProviderUri() throws IOException;

  /**
   * Get CanonicalServiceName for ozone delegation token.
   * @return Canonical Service Name of ozone delegation token.
   */
  String getCanonicalServiceName();

  /**
   * Get the Ozone File Status for a particular Ozone key.
   *
   * @param volumeName volume name.
   * @param bucketName bucket name.
   * @param keyName    key name.
   * @return OzoneFileStatus for the key.
   * @throws OMException if file does not exist
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  OzoneFileStatus getOzoneFileStatus(String volumeName, String bucketName,
      String keyName) throws IOException;

  /**
   * Creates directory with keyName as the absolute path for the directory.
   *
   * @param volumeName Volume name
   * @param bucketName Bucket name
   * @param keyName    Absolute path for the directory
   * @throws OMException if any entry in the path exists as a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  void createDirectory(String volumeName, String bucketName, String keyName)
      throws IOException;

  /**
   * Creates an input stream for reading file contents.
   *
   * @param volumeName Volume name
   * @param bucketName Bucket name
   * @param keyName    Absolute path of the file to be read
   * @return Input stream for reading the file
   * @throws OMException if any entry in the path exists as a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  OzoneInputStream readFile(String volumeName, String bucketName,
      String keyName) throws IOException;

  /**
   * Creates an output stream for writing to a file.
   *
   * @param volumeName Volume name
   * @param bucketName Bucket name
   * @param keyName    Absolute path of the file to be written
   * @param size       Size of data to be written
   * @param type       Replication Type
   * @param factor     Replication Factor
   * @param overWrite  if true existing file at the location will be overwritten
   * @param recursive  if true file would be created even if parent directories
   *                   do not exist
   * @return Output stream for writing to the file
   * @throws OMException if given key is a directory
   *                     if file exists and isOverwrite flag is false
   *                     if an ancestor exists as a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  @SuppressWarnings("checkstyle:parameternumber")
  @Deprecated
  OzoneOutputStream createFile(String volumeName, String bucketName,
      String keyName, long size, ReplicationType type, ReplicationFactor factor,
      boolean overWrite, boolean recursive) throws IOException;


  /**
   * Creates an output stream for writing to a file.
   *
   * @param volumeName Volume name
   * @param bucketName Bucket name
   * @param keyName    Absolute path of the file to be written
   * @param size       Size of data to be written
   * @param replicationConfig Replication config
   * @param overWrite  if true existing file at the location will be overwritten
   * @param recursive  if true file would be created even if parent directories
   *                   do not exist
   * @return Output stream for writing to the file
   * @throws OMException if given key is a directory
   *                     if file exists and isOverwrite flag is false
   *                     if an ancestor exists as a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  @SuppressWarnings("checkstyle:parameternumber")
  OzoneOutputStream createFile(String volumeName, String bucketName,
      String keyName, long size, ReplicationConfig replicationConfig,
      boolean overWrite, boolean recursive) throws IOException;

  @SuppressWarnings("checkstyle:parameternumber")
  OzoneDataStreamOutput createStreamFile(String volumeName, String bucketName,
      String keyName, long size, ReplicationConfig replicationConfig,
      boolean overWrite, boolean recursive) throws IOException;


  /**
   * List the status for a file or a directory and its contents.
   *
   * @param volumeName Volume name
   * @param bucketName Bucket name
   * @param keyName    Absolute path of the entry to be listed
   * @param recursive  For a directory if true all the descendants of a
   *                   particular directory are listed
   * @param startKey   Key from which listing needs to start. If startKey exists
   *                   its status is included in the final list.
   * @param numEntries Number of entries to list from the start key
   * @return list of file status
   */
  List<OzoneFileStatus> listStatus(String volumeName, String bucketName,
      String keyName, boolean recursive, String startKey, long numEntries)
      throws IOException;

  /**
   * List the status for a file or a directory and its contents.
   *
   * @param volumeName Volume name
   * @param bucketName Bucket name
   * @param keyName    Absolute path of the entry to be listed
   * @param recursive  For a directory if true all the descendants of a
   *                   particular directory are listed
   * @param startKey   Key from which listing needs to start. If startKey exists
   *                   its status is included in the final list.
   * @param numEntries Number of entries to list from the start key
   * @param allowPartialPrefixes if partial prefixes should be allowed,
   *                             this is needed in context of ListKeys
   * @return list of file status
   */
  List<OzoneFileStatus> listStatus(String volumeName, String bucketName,
      String keyName, boolean recursive, String startKey,
      long numEntries, boolean allowPartialPrefixes) throws IOException;

  /**
   * Lightweight listStatus API.
   *
   * @param volumeName Volume name
   * @param bucketName Bucket name
   * @param keyName    Absolute path of the entry to be listed
   * @param recursive  For a directory if true all the descendants of a
   *                   particular directory are listed
   * @param startKey   Key from which listing needs to start. If startKey exists
   *                   its status is included in the final list.
   * @param numEntries Number of entries to list from the start key
   * @param allowPartialPrefixes if partial prefixes should be allowed,
   *                             this is needed in context of ListKeys
   * @return list of file status
   */
  List<OzoneFileStatusLight> listStatusLight(String volumeName,
      String bucketName, String keyName, boolean recursive, String startKey,
      long numEntries, boolean allowPartialPrefixes) throws IOException;

  /**
   * Add acl for Ozone object. Return true if acl is added successfully else
   * false.
   * @param obj Ozone object for which acl should be added.
   * @param acl ozone acl to be added.
   *
   * @throws IOException if there is error.
   * */
  boolean addAcl(OzoneObj obj, OzoneAcl acl) throws IOException;

  /**
   * Remove acl for Ozone object. Return true if acl is removed successfully
   * else false.
   * @param obj Ozone object.
   * @param acl Ozone acl to be removed.
   *
   * @throws IOException if there is error.
   * */
  boolean removeAcl(OzoneObj obj, OzoneAcl acl) throws IOException;

  /**
   * Acls to be set for given Ozone object. This operations reset ACL for
   * given object to list of ACLs provided in argument.
   * @param obj Ozone object.
   * @param acls List of acls.
   *
   * @throws IOException if there is error.
   * */
  boolean setAcl(OzoneObj obj, List<OzoneAcl> acls) throws IOException;

  /**
   * Returns list of ACLs for given Ozone object.
   * @param obj Ozone object.
   *
   * @throws IOException if there is error.
   * */
  List<OzoneAcl> getAcl(OzoneObj obj) throws IOException;

  /**
   * Getter for OzoneManagerClient.
   */
  OzoneManagerProtocol getOzoneManagerClient();

  /**
   * Set Bucket Quota.
   * @param volumeName Name of the Volume.
   * @param bucketName Name of the Bucket.
   * @param quotaInBytes The maximum size this buckets can be used.
   * @param quotaInNamespace The maximum number of keys in this bucket.
   * @throws IOException
   */
  void setBucketQuota(String volumeName, String bucketName,
      long quotaInNamespace, long quotaInBytes) throws IOException;

  /**
   * Set Bucket replication configuration.
   *
   * @param volumeName        Name of the Volume.
   * @param bucketName        Name of the Bucket.
   * @param replicationConfig The replication config to set on bucket.
   * @throws IOException
   */
  void setReplicationConfig(String volumeName, String bucketName,
      ReplicationConfig replicationConfig) throws IOException;

  /**
   * Set Bucket Encryption Key (BEK).
   *
   * @param volumeName
   * @param bucketName
   * @param bekName
   * @throws IOException
   * @deprecated This functionality is deprecated as it is not intended for
   * users to reset bucket encryption under normal circumstances and may be
   * removed in the future. Users are advised to exercise caution and consider
   * alternative approaches for managing bucket encryption unless HDDS-7449 or
   * HDDS-7526 is encountered. As a result, the setter methods for this
   * functionality have been marked as deprecated.
   */
  @Deprecated
  void setEncryptionKey(String volumeName, String bucketName,
                        String bekName) throws IOException;

  /**
   * Returns OzoneKey that contains the application generated/visible
   * metadata for an Ozone Object.
   *
   * If Key exists, return returns OzoneKey.
   * If Key does not exist, throws an exception with error code KEY_NOT_FOUND
   *
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @return OzoneKey which gives basic information about the key.
   * @throws IOException
   */
  OzoneKey headObject(String volumeName, String bucketName,
      String keyName) throws IOException;

  /**
   * Sets the S3 Authentication information for the requests executed on behalf
   * of the S3 API implementation within Ozone.
   * @param s3Auth authentication information for each S3 API call.
   */
  void setThreadLocalS3Auth(S3Auth s3Auth);

  void setIsS3Request(boolean isS3Request);

  /**
   * Gets the S3 Authentication information that is attached to the thread.
   * @return S3 Authentication information.
   */
  S3Auth getThreadLocalS3Auth();

  /**
   * Clears the S3 Authentication information attached to the thread.
   */
  void clearThreadLocalS3Auth();

  default ThreadLocal<S3Auth> getS3CredentialsProvider() {
    return null;
  }

  /**
   * Sets the owner of bucket.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param owner to be set for the bucket
   * @throws IOException
   */
  boolean setBucketOwner(String volumeName, String bucketName,
      String owner) throws IOException;

  /**
   * Reads every replica for all the blocks associated with a given key.
   * @param volumeName Volume name.
   * @param bucketName Bucket name.
   * @param keyName Key name.
   * @return For every OmKeyLocationInfo (represents a block) it is mapped
   * every replica, which is constructed by the DatanodeDetails and an
   * inputstream made from the block.
   * @throws IOException
   */
  Map<OmKeyLocationInfo,
      Map<DatanodeDetails, OzoneInputStream>> getKeysEveryReplicas(
          String volumeName, String bucketName, String keyName)
      throws IOException;

  /**
   * Create snapshot.
   * @param volumeName vol to be used
   * @param bucketName bucket to be used
   * @param snapshotName name to be used
   * @return name used
   * @throws IOException
   */
  String createSnapshot(String volumeName,
      String bucketName, String snapshotName) throws IOException;

  /**
   * Rename snapshot.
   *
   * @param volumeName Vol to be used
   * @param bucketName Bucket to be used
   * @param snapshotOldName Old name of the snapshot
   * @param snapshotNewName New name of the snapshot
   *
   * @throws IOException
   */
  void renameSnapshot(String volumeName,
      String bucketName, String snapshotOldName, String snapshotNewName) throws IOException;

  /**
   * Delete snapshot.
   * @param volumeName vol to be used
   * @param bucketName bucket to be used
   * @param snapshotName name of the snapshot to be deleted
   * @throws IOException
   */
  void deleteSnapshot(String volumeName,
      String bucketName, String snapshotName) throws IOException;

  /**
   * Returns snapshot info for volume/bucket snapshot path.
   * @param volumeName volume name
   * @param bucketName bucket name
   * @param snapshotName snapshot name
   * @return snapshot info for volume/bucket snapshot path.
   * @throws IOException
   */
  OzoneSnapshot getSnapshotInfo(String volumeName,
                                String bucketName,
                                String snapshotName) throws IOException;

  /**
   * Create an image of the current compaction log DAG in the OM.
   * @param fileNamePrefix  file name prefix of the image file.
   * @param graphType       type of node name to use in the graph image.
   * @return message which tells the image name, parent dir and OM leader
   * node information.
   */
  @Deprecated
  String printCompactionLogDag(String fileNamePrefix, String graphType)
      throws IOException;

  /**
   * List snapshots in a volume/bucket.
   * @param volumeName     volume name
   * @param bucketName     bucket name
   * @param snapshotPrefix snapshot prefix to match
   * @param prevSnapshot   snapshots will be listed after this snapshot name
   * @param maxListResult  max number of snapshots to return
   * @return list of snapshots for volume/bucket path.
   * @throws IOException
   */
  ListSnapshotResponse listSnapshot(
      String volumeName, String bucketName, String snapshotPrefix,
      String prevSnapshot, int maxListResult) throws IOException;

  /**
   * Get the differences between two snapshots.
   * @param volumeName Name of the volume to which the snapshotted bucket belong
   * @param bucketName Name of the bucket to which the snapshots belong
   * @param fromSnapshot The name of the starting snapshot
   * @param toSnapshot The name of the ending snapshot
   * @param token to get the index to return diff report from.
   * @param pageSize maximum entries returned to the report.
   * @param forceFullDiff request to force full diff, skipping DAG optimization
   * @return the difference report between two snapshots
   * @throws IOException in case of any exception while generating snapshot diff
   */
  @SuppressWarnings("parameternumber")
  SnapshotDiffResponse snapshotDiff(String volumeName, String bucketName,
                                    String fromSnapshot, String toSnapshot,
                                    String token, int pageSize,
                                    boolean forceFullDiff,
                                    boolean disableNativeDiff)
      throws IOException;

  /**
   * Cancel snapshot diff job.
   * @param volumeName Name of the volume to which the snapshotted bucket belong
   * @param bucketName Name of the bucket to which the snapshots belong
   * @param fromSnapshot The name of the starting snapshot
   * @param toSnapshot The name of the ending snapshot
   * @return the success if cancel succeeds.
   * @throws IOException in case of any exception while cancelling snap diff job
   */
  CancelSnapshotDiffResponse cancelSnapshotDiff(String volumeName,
                                                String bucketName,
                                                String fromSnapshot,
                                                String toSnapshot)
      throws IOException;

  /**
   * Get a list of the SnapshotDiff jobs for a bucket based on the JobStatus.
   * @param volumeName Name of the volume to which the snapshotted bucket belong
   * @param bucketName Name of the bucket to which the snapshots belong
   * @param jobStatus JobStatus to be used to filter the snapshot diff jobs
   * @param listAllStatus Option to specify whether to list all jobs regardless of status
   * @param prevSnapshotDiffJob list snapshot diff jobs after this snapshot diff job.
   * @param maxListResult maximum entries to be returned from the startSnapshotDiffJob.
   * @return a list of SnapshotDiffJob objects
   * @throws IOException in case there is a failure while getting a response.
   */
  ListSnapshotDiffJobResponse listSnapshotDiffJobs(
      String volumeName,
      String bucketName,
      String jobStatus,
      boolean listAllStatus,
      String prevSnapshotDiffJob,
      int maxListResult) throws IOException;

  /**
   * Time to be set for given Ozone object. This operations updates modification
   * time and access time for the given key.
   * @param obj Ozone object.
   * @param keyName Full path name to the key in the bucket.
   * @param mtime Modification time. Unchanged if -1.
   * @param atime Access time. Unchanged if -1.
   *
   * @throws IOException if there is error.
   * */
  void setTimes(OzoneObj obj, String keyName, long mtime, long atime)
      throws IOException;

  /**
   * Start the lease recovery of a file.
   *
   * @param volumeName - The volume name.
   * @param bucketName - The bucket name.
   * @param keyName - The key user want to recover.
   * @param force - force recover the file.
   * @return LeaseKeyInfo KeyInfo of file under recovery
   * @throws IOException if an error occurs
   */
  LeaseKeyInfo recoverLease(String volumeName, String bucketName, String keyName, boolean force) throws IOException;

  /**
   * Recovery and commit a key. This will make the change from the client visible. The client
   * is identified by the clientID.
   *
   * @param args the key to commit
   * @param clientID the client identification
   * @throws IOException
   */
  void recoverKey(OmKeyArgs args, long clientID) throws IOException;

  /**
   * Gets the tags for an existing key.
   * @param volumeName Volume name.
   * @param bucketName Bucket name.
   * @param keyName Key name.
   * @return tags for the specified key.
   * @throws IOException
   */
  Map<String, String> getObjectTagging(String volumeName, String bucketName, String keyName)
      throws IOException;

  /**
   * Sets the tags to an existing key.
   * @param volumeName Volume name.
   * @param bucketName Bucket name.
   * @param keyName Key name.
   * @param tags Tags to set on the key.
   * @throws IOException
   */
  void putObjectTagging(String volumeName, String bucketName, String keyName,
                        Map<String, String> tags) throws IOException;


  /**
   * Removes all the tags from the specified key.
   * @param volumeName Volume name.
   * @param bucketName Bucket name.
   * @param keyName Key name.
   * @throws IOException
   */
  void deleteObjectTagging(String volumeName, String bucketName, String keyName)
      throws IOException;

}
