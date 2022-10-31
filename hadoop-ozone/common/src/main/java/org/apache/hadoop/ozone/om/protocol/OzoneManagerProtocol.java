/**
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

package org.apache.hadoop.ozone.om.protocol;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.IOmMetadataReader;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.DBUpdates;
import org.apache.hadoop.ozone.om.helpers.DeleteTenantState;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDeleteKeys;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmRenameKeys;
import org.apache.hadoop.ozone.om.helpers.OmTenantArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.helpers.S3VolumeContext;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.helpers.TenantStateList;
import org.apache.hadoop.ozone.om.helpers.TenantUserInfoValue;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CancelPrepareResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.EchoRPCResponse;
import org.apache.hadoop.ozone.security.OzoneDelegationTokenSelector;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.StatusAndMessages;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.TokenInfo;

/**
 * Protocol to talk to OM.
 */
@KerberosInfo(
    serverPrincipal = OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY)
@TokenInfo(OzoneDelegationTokenSelector.class)
public interface OzoneManagerProtocol
    extends IOmMetadataReader, OzoneManagerSecurityProtocol, Closeable {

  @SuppressWarnings("checkstyle:ConstantName")
  /**
   * Version 1: Initial version.
   */
  long versionID = 1L;

  /**
   * Creates a volume.
   * @param args - Arguments to create Volume.
   * @throws IOException
   */
  default void createVolume(OmVolumeArgs args) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }


  /**
   * Changes the owner of a volume.
   * @param volume  - Name of the volume.
   * @param owner - Name of the owner.
   * @return true if operation succeeded, false if specified user is
   *         already the owner.
   * @throws IOException
   */
  default boolean setOwner(String volume, String owner) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }


  /**
   * Changes the Quota on a volume.
   * @param volume - Name of the volume.
   * @param quotaInNamespace - Volume quota in counts.
   * @param quotaInBytes - Volume quota in bytes.
   * @throws IOException
   */
  default void setQuota(String volume, long quotaInNamespace, long quotaInBytes)
      throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }


  /**
   * Checks if the specified user can access this volume.
   * @param volume - volume
   * @param userAcl - user acls which needs to be checked for access
   * @return true if the user has required access for the volume,
   *         false otherwise
   * @throws IOException
   */
  default boolean checkVolumeAccess(String volume, OzoneAclInfo userAcl)
      throws IOException {
    throw new UnsupportedOperationException("This operation is not supported.");
  }


  /**
   * Gets the volume information.
   * @param volume - Volume name.
   * @return VolumeArgs or exception is thrown.
   * @throws IOException
   */
  OmVolumeArgs getVolumeInfo(String volume) throws IOException;

  /**
   * Deletes an existing empty volume.
   * @param volume - Name of the volume.
   * @throws IOException
   */
  default void deleteVolume(String volume) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }


  /**
   * Lists volumes accessible by a specific user.
   * @param userName - user name
   * @param prefix  - Filter prefix -- Return only entries that match this.
   * @param prevKey - Previous key -- List starts from the next from the prevkey
   * @param maxKeys - Max number of keys to return.
   * @return List of Volumes.
   * @throws IOException
   */
  List<OmVolumeArgs> listVolumeByUser(String userName, String prefix, String
      prevKey, int maxKeys) throws IOException;

  /**
   * Lists volume all volumes in the cluster.
   * @param prefix  - Filter prefix -- Return only entries that match this.
   * @param prevKey - Previous key -- List starts from the next from the prevkey
   * @param maxKeys - Max number of keys to return.
   * @return List of Volumes.
   * @throws IOException
   */
  List<OmVolumeArgs> listAllVolumes(String prefix, String
      prevKey, int maxKeys) throws IOException;

  /**
   * Creates a bucket.
   * @param bucketInfo - BucketInfo to create Bucket.
   * @throws IOException
   */
  default void createBucket(OmBucketInfo bucketInfo) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }


  /**
   * Gets the bucket information.
   * @param volumeName - Volume name.
   * @param bucketName - Bucket name.
   * @return OmBucketInfo or exception is thrown.
   * @throws IOException
   */
  OmBucketInfo getBucketInfo(String volumeName, String bucketName)
      throws IOException;

  /**
   * Sets bucket property from args.
   * @param args - BucketArgs.
   * @throws IOException
   */
  default void setBucketProperty(OmBucketArgs args) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }

  /**
   * Changes the owner of a bucket.
   * @param args  - OMBucketArgs
   * @return true if operation succeeded, false if specified user is
   *         already the owner.
   * @throws IOException
   */
  default boolean setBucketOwner(OmBucketArgs args) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }


  /**
   * Open the given key and return an open key session.
   *
   * @param args the args of the key.
   * @return OpenKeySession instance that client uses to talk to container.
   * @throws IOException
   */
  default OpenKeySession openKey(OmKeyArgs args) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }

  /**
   * Commit a key. This will make the change from the client visible. The client
   * is identified by the clientID.
   *
   * @param args the key to commit
   * @param clientID the client identification
   * @throws IOException
   */
  default void commitKey(OmKeyArgs args, long clientID)
      throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }


  /**
   * Allocate a new block, it is assumed that the client is having an open key
   * session going on. This block will be appended to this open key session.
   *
   * @param args the key to append
   * @param clientID the client identification
   * @param excludeList List of datanodes/containers to exclude during block
   *                    allocation
   * @return an allocated block
   * @throws IOException
   */
  default OmKeyLocationInfo allocateBlock(OmKeyArgs args, long clientID,
      ExcludeList excludeList) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }

  /**
   * Rename an existing key within a bucket.
   * @param args the args of the key.
   * @param toKeyName New name to be used for the Key
   * @throws IOException
   */
  default void renameKey(OmKeyArgs args, String toKeyName) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }


  /**
   * Rename existing keys within a bucket.
   * @param omRenameKeys Includes volume, bucket, and fromKey toKey name map
   *                     and fromKey name toKey info Map.
   * @throws IOException
   */
  default void renameKeys(OmRenameKeys omRenameKeys) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }


  /**
   * Deletes an existing key.
   *
   * @param args the args of the key.
   * @throws IOException
   */
  default void deleteKey(OmKeyArgs args) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }


  /**
   * Deletes existing key/keys. This interface supports delete
   * multiple keys and a single key. Used by deleting files
   * through OzoneFileSystem.
   *
   * @param deleteKeys
   * @throws IOException
   */
  default void deleteKeys(OmDeleteKeys deleteKeys) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }


  /**
   * Deletes an existing empty bucket from volume.
   * @param volume - Name of the volume.
   * @param bucket - Name of the bucket.
   * @throws IOException
   */
  default void deleteBucket(String volume, String bucket) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }


  /**
   * Returns a list of buckets represented by {@link OmBucketInfo}
   * in the given volume. Argument volumeName is required, others
   * are optional.
   *
   * @param volumeName
   *   the name of the volume.
   * @param startBucketName
   *   the start bucket name, only the buckets whose name is
   *   after this value will be included in the result.
   * @param bucketPrefix
   *   bucket name prefix, only the buckets whose name has
   *   this prefix will be included in the result.
   * @param maxNumOfBuckets
   *   the maximum number of buckets to return. It ensures
   *   the size of the result will not exceed this limit.
   * @return a list of buckets.
   * @throws IOException
   */
  List<OmBucketInfo> listBuckets(String volumeName,
      String startBucketName, String bucketPrefix, int maxNumOfBuckets)
      throws IOException;

  /**
   * Returns list of Ozone services with its configuration details.
   *
   * @return list of Ozone services
   * @throws IOException
   */
  List<ServiceInfo> getServiceList() throws IOException;

  ServiceInfoEx getServiceInfo() throws IOException;

  /**
   * Triggers Ranger background sync task immediately.
   *
   * Requires Ozone administrator privilege.
   *
   * @param noWait set to true if client won't wait for the result.
   * @return true if noWait is true or when task completed successfully,
   *         false otherwise.
   * @throws IOException OMException (e.g. PERMISSION_DENIED)
   */
  boolean triggerRangerBGSync(boolean noWait) throws IOException;

  /**
   * Initiate metadata upgrade finalization.
   * This method when called, initiates finalization of Ozone Manager metadata
   * during an upgrade. The status returned contains the status
   * - ALREADY_FINALIZED with empty message list when the software layout
   *    version and the metadata layout version are equal
   * - STARTING_FINALIZATION with empty message list when the finalization
   *    has been started successfully
   * - If a finalization is already in progress, then the method throws an
   *    {@link OMException} with a result code INVALID_REQUEST
   *
   *
   * The leader Ozone Manager initiates finalization of the followers via
   * the Raft protocol in other Ozone Managers, and reports progress to the
   * client via the
   * {@link #queryUpgradeFinalizationProgress(String, boolean, boolean)}
   * call.
   *
   * The follower Ozone Managers reject this request and directs the client to
   * the leader.
   *
   * @param upgradeClientID String identifier of the upgrade finalizer client
   * @return the finalization status.
   * @throws IOException
   *            when finalization is failed, or this Ozone Manager is not the
   *                leader.
   * @throws OMException
   *            when finalization is already in progress.
   */
  StatusAndMessages finalizeUpgrade(String upgradeClientID) throws IOException;

  /**
   * Queries the current status of finalization.
   * This method when called, returns the status messages from the finalization
   * progress, if any. The status returned is
   * - FINALIZATION_IN_PROGRESS, and the messages since the last query if the
   *    finalization is still running
   * - FINALIZATION_DONE with a message list containing the messages since
   *    the last query, if the finalization ended but the messages were not
   *    yet emitted to the client.
   * - ALREADY_FINALIZED with an empty message list otherwise
   * - If finalization is not in progress, but software layout version and
   *    metadata layout version are different, the method will throw an
   *    {@link OMException} with a result code INVALID_REQUEST
   * - If during finalization an other client with different ID than the one
   *    initiated finalization is calling the method, then an
   *    {@link OMException} with a result code INVALID_REQUEST is thrown,
   *    unless the request is forced by a new client, in which case the new
   *    client takes over the old client and the old client should exit.
   *
   * @param takeover set force takeover of output monitoring
   * @param readonly set readonly of output
   * @param upgradeClientID String identifier of the upgrade finalizer client
   * @return the finalization status and status messages.
   * @throws IOException
   *            if there was a problem during the query
   * @throws OMException
   *            if finalization is needed but not yet started
   */
  StatusAndMessages queryUpgradeFinalizationProgress(
      String upgradeClientID, boolean takeover, boolean readonly
  ) throws IOException;

  /*
   * S3 Specific functionality that is supported by Ozone Manager.
   */

  /**
   * Initiate multipart upload for the specified key.
   * @param keyArgs
   * @return MultipartInfo
   * @throws IOException
   */
  default OmMultipartInfo initiateMultipartUpload(OmKeyArgs keyArgs)
      throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }



  /**
   * Commit Multipart upload part file.
   * @param omKeyArgs
   * @param clientID
   * @return OmMultipartCommitUploadPartInfo
   * @throws IOException
   */
  default OmMultipartCommitUploadPartInfo commitMultipartUploadPart(
      OmKeyArgs omKeyArgs, long clientID) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }


  /**
   * Complete Multipart upload Request.
   * @param omKeyArgs
   * @param multipartUploadList
   * @return OmMultipartUploadCompleteInfo
   * @throws IOException
   */
  default OmMultipartUploadCompleteInfo completeMultipartUpload(
      OmKeyArgs omKeyArgs, OmMultipartUploadCompleteList multipartUploadList)
      throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }


  /**
   * Abort multipart upload.
   * @param omKeyArgs
   * @throws IOException
   */
  default void abortMultipartUpload(OmKeyArgs omKeyArgs) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }


  /**
   * Returns list of parts of a multipart upload key.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param uploadID
   * @param partNumberMarker
   * @param maxParts
   * @return OmMultipartUploadListParts
   */
  OmMultipartUploadListParts listParts(String volumeName, String bucketName,
      String keyName, String uploadID, int partNumberMarker,
      int maxParts)  throws IOException;

  /**
   * List in-flight uploads.
   */
  OmMultipartUploadList listMultipartUploads(String volumeName,
      String bucketName, String prefix) throws IOException;

  /**
   * Gets s3Secret for given kerberos user.
   * @param kerberosID
   * @return S3SecretValue
   * @throws IOException
   */
  default S3SecretValue getS3Secret(String kerberosID) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }

  /**
   * Gets s3Secret for given kerberos user.
   * @param kerberosID
   * @param createIfNotExist
   * @return S3SecretValue
   * @throws IOException
   */
  default S3SecretValue getS3Secret(String kerberosID, boolean createIfNotExist)
      throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach");
  };

  /**
   * Set secret key for accessId.
   * @param accessId
   * @param secretKey
   * @return S3SecretValue
   * @throws IOException
   */
  default S3SecretValue setS3Secret(String accessId, String secretKey)
      throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach");
  }

  /**
   * Revokes s3Secret of given kerberos user.
   * @param kerberosID
   * @throws IOException
   */
  default void revokeS3Secret(String kerberosID) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }

  /**
   * Create a tenant.
   * @param omTenantArgs OmTenantArgs
   * @throws IOException
   */
  default void createTenant(OmTenantArgs omTenantArgs) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach");
  }

  /**
   * Delete a tenant.
   * @param tenantId tenant name.
   * @return DeleteTenantResponse
   * @throws IOException
   */
  default DeleteTenantState deleteTenant(String tenantId)
      throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach");
  }

  /**
   * Assign user to a tenant.
   * @param username user name to be assigned.
   * @param tenantId tenant name.
   * @param accessId access ID.
   * @return S3SecretValue
   * @throws IOException
   */
  default S3SecretValue tenantAssignUserAccessId(String username,
                                                 String tenantId,
                                                 String accessId)
      throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach");
  }

  S3VolumeContext getS3VolumeContext() throws IOException;

  /**
   * Revoke user accessId to a tenant.
   * @param accessId accessId to be revoked.
   * @throws IOException
   */
  default void tenantRevokeUserAccessId(String accessId) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach");
  }

  /**
   * Create snapshot.
   * @param volumeName vol to be used
   * @param bucketName bucket to be used
   * @param snapshotName name to be used
   * @return name used
   * @throws IOException
   */
  default String createSnapshot(String volumeName,
      String bucketName, String snapshotName) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented");
  }

  /**
   * Assign admin role to a user identified by an accessId in a tenant.
   * @param accessId access ID.
   * @param tenantId tenant name.
   * @param delegated true if making delegated admin.
   * @throws IOException
   */
  default void tenantAssignAdmin(String accessId,
                                 String tenantId,
                                 boolean delegated)
      throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach");
  }

  /**
   * Revoke admin role of an accessId in a tenant.
   * @param accessId access ID.
   * @param tenantId tenant name.
   * @throws IOException
   */
  default void tenantRevokeAdmin(String accessId,
                                 String tenantId) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach");
  }

  /**
   * Get tenant info for a user.
   * @param userPrincipal Kerberos principal of a user.
   * @return TenantUserInfo
   * @throws IOException
   */
  TenantUserInfoValue tenantGetUserInfo(String userPrincipal)
      throws IOException;

  TenantUserList listUsersInTenant(String tenantId, String prefix)
      throws IOException;

  /**
   * List tenants.
   * @return TenantStateList
   * @throws IOException
   */
  TenantStateList listTenant() throws IOException;

  /**
   * Ozone FS api to create a directory. Parent directories if do not exist
   * are created for the input directory.
   *
   * @param args Key args
   * @throws OMException if any entry in the path exists as a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  default void createDirectory(OmKeyArgs args) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }


  /**
   * OzoneFS api to creates an output stream for a file.
   *
   * @param keyArgs   Key args
   * @param overWrite if true existing file at the location will be overwritten
   * @param recursive if true file would be created even if parent directories
   *                  do not exist
   * @throws OMException if given key is a directory
   *                     if file exists and isOverwrite flag is false
   *                     if an ancestor exists as a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  default OpenKeySession createFile(OmKeyArgs keyArgs, boolean overWrite,
      boolean recursive) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }


  /**
   * Add acl for Ozone object. Return true if acl is added successfully else
   * false.
   * @param obj Ozone object for which acl should be added.
   * @param acl ozone acl to be added.
   *
   * @throws IOException if there is error.
   * */
  default boolean addAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }


  /**
   * Remove acl for Ozone object. Return true if acl is removed successfully
   * else false.
   * @param obj Ozone object.
   * @param acl Ozone acl to be removed.
   *
   * @throws IOException if there is error.
   * */
  default boolean removeAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }


  /**
   * Acls to be set for given Ozone object. This operations reset ACL for
   * given object to list of ACLs provided in argument.
   * @param obj Ozone object.
   * @param acls List of acls.
   *
   * @throws IOException if there is error.
   * */
  default boolean setAcl(OzoneObj obj, List<OzoneAcl> acls) throws IOException {
    throw new UnsupportedOperationException("OzoneManager does not require " +
        "this to be implemented, as write requests use a new approach.");
  }

  /**
   * Get DB updates since a specific sequence number.
   * @param dbUpdatesRequest request that encapsulates a sequence number.
   * @return Wrapper containing the updates.
   */
  DBUpdates getDBUpdates(
      OzoneManagerProtocolProtos.DBUpdatesRequest dbUpdatesRequest)
      throws IOException;

  /**
   * List trash allows the user to list the keys that were marked as deleted,
   * but not actually deleted by Ozone Manager. This allows a user to recover
   * keys within a configurable window.
   * @param volumeName - The volume name, which can also be a wild card
   *                   using '*'.
   * @param bucketName - The bucket name, which can also be a wild card
   *                   using '*'.
   * @param startKeyName - List keys from a specific key name.
   * @param keyPrefix - List keys using a specific prefix.
   * @param maxKeys - The number of keys to be returned. This must be below
   *                the cluster level set by admins.
   * @return The list of keys that are deleted from the deleted table.
   * @throws IOException
   */
  List<RepeatedOmKeyInfo> listTrash(String volumeName, String bucketName,
      String startKeyName, String keyPrefix, int maxKeys) throws IOException;

  /**
   * Recover trash allows the user to recover keys that were marked as deleted,
   * but not actually deleted by Ozone Manager.
   * @param volumeName - The volume name.
   * @param bucketName - The bucket name.
   * @param keyName - The key user want to recover.
   * @param destinationBucket - The bucket user want to recover to.
   * @return The result of recovering operation is success or not.
   * @throws IOException
   */
  default boolean recoverTrash(String volumeName, String bucketName,
      String keyName, String destinationBucket) throws IOException {
    return false;
  }

  /**
   *
   * @param txnApplyWaitTimeoutSeconds Max time in SECONDS to wait for all
   *                                   transactions before the prepare request
   *                                   to be applied to the OM DB.
   * @param txnApplyCheckIntervalSeconds Time in SECONDS to wait between
   *                                     successive checks for all transactions
   *                                     to be applied to the OM DB.
   * @return
   */
  default long prepareOzoneManager(
      long txnApplyWaitTimeoutSeconds, long txnApplyCheckIntervalSeconds)
      throws IOException {
    return -1;
  }

  /**
   * Check if Ozone Manager is 'prepared' at a specific Txn Id.
   * @param txnId passed in Txn Id
   * @return PrepareStatus response
   * @throws IOException on exception.
   */
  default PrepareStatusResponse getOzoneManagerPrepareStatus(long txnId)
      throws IOException {
    return PrepareStatusResponse.newBuilder()
        .setCurrentTxnIndex(-1)
        .setStatus(PrepareStatus.NOT_PREPARED)
        .build();
  }

  /**
   * Cancel the prepare state of the Ozone Manager. If ozone manager is not
   * prepared, has no effect.
   * @throws IOException on exception.
   */
  default CancelPrepareResponse cancelOzoneManagerPrepare() throws IOException {
    return CancelPrepareResponse.newBuilder().build();
  }

  /**
   * Send RPC request with or without payload to OM
   * to benchmark RPC communication performance.
   * @param payloadReq payload in request.
   * @param payloadSizeResp payload size of response.
   * @throws IOException if there is error in the RPC communication.
   * @return EchoRPCResponse.
   */
  EchoRPCResponse echoRPCReq(byte[] payloadReq,
                             int payloadSizeResp)
          throws IOException;

}
