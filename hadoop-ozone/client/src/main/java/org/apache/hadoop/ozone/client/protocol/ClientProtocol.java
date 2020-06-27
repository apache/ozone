/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client.protocol;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadList;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRoleInfo;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
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
   * @param quota Quota to be set for the Volume
   * @throws IOException
   */
  void setVolumeQuota(String volumeName, OzoneQuota quota)
      throws IOException;

  /**
   * Returns {@link OzoneVolume}.
   * @param volumeName Name of the Volume
   * @return {@link OzoneVolume}
   * @throws IOException
   * */
  OzoneVolume getVolumeDetails(String volumeName)
      throws IOException;

  /**
   * Checks if a Volume exists and the user with a role specified has access
   * to the Volume.
   * @param volumeName Name of the Volume
   * @param acl requested acls which needs to be checked for access
   * @return Boolean - True if the user with a role can access the volume.
   * This is possible for owners of the volume and admin users
   * @throws IOException
   */
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
   * @param volumeName Name of the Volume
   * @param bucketPrefix Bucket prefix to match
   * @param prevBucket Starting point of the list, this bucket is excluded
   * @param maxListResult Max number of buckets to return.
   * @return {@code List<OzoneBucket>}
   * @throws IOException
   */
  List<OzoneBucket> listBuckets(String volumeName, String bucketPrefix,
                                String prevBucket, int maxListResult)
      throws IOException;

  /**
   * Writes a key in an existing bucket.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Name of the Key
   * @param size Size of the data
   * @param metadata custom key value metadata
   * @return {@link OzoneOutputStream}
   *
   */
  OzoneOutputStream createKey(String volumeName, String bucketName,
                              String keyName, long size, ReplicationType type,
                              ReplicationFactor factor,
                              Map<String, String> metadata)
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
   * Deletes an existing key.
   * @param volumeName Name of the Volume
   * @param bucketName Name of the Bucket
   * @param keyName Name of the Key
   * @throws IOException
   */
  void deleteKey(String volumeName, String bucketName, String keyName)
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
                                    String startKeyName, String keyPrefix,
                                    int maxKeys)
      throws IOException;

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
  boolean recoverTrash(String volumeName, String bucketName, String keyName,
      String destinationBucket) throws IOException;

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
  OmMultipartInfo initiateMultipartUpload(String volumeName, String
      bucketName, String keyName, ReplicationType type, ReplicationFactor
      factor) throws IOException;

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
      String bucketName, String prefix) throws IOException;

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
   * returns S3 Secret given kerberos user.
   * @param kerberosID
   * @return S3SecretValue
   * @throws IOException
   */
  S3SecretValue getS3Secret(String kerberosID) throws IOException;

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
  OzoneOutputStream createFile(String volumeName, String bucketName,
      String keyName, long size, ReplicationType type, ReplicationFactor factor,
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
}
