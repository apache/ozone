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

import static org.apache.hadoop.ozone.OzoneConsts.ETAG;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.OzoneConsts.QUOTA_RESET;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Stack;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BasicOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.ErrorInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatusLight;
import org.apache.hadoop.ozone.om.helpers.WithMetadata;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;

/**
 * A class that encapsulates OzoneBucket.
 */
public class OzoneBucket extends WithMetadata {

  /**
   * The proxy used for connecting to the cluster and perform
   * client operations.
   */
  private final ClientProtocol proxy;
  /**
   * Name of the volume in which the bucket belongs to.
   */
  private final String volumeName;
  /**
   * Name of the bucket.
   */
  private final String name;
  /**
   * Default replication factor to be used while creating keys.
   */
  private ReplicationConfig defaultReplication;

  /**
   * Type of storage to be used for this bucket.
   * [RAM_DISK, SSD, DISK, ARCHIVE]
   */
  private StorageType storageType;

  /**
   * Bucket Version flag.
   */
  private Boolean versioning;

  /**
   * Cache size to be used for listKey calls.
   */
  private int listCacheSize;

  /**
   * Used bytes of the bucket.
   */
  private long usedBytes;

  /**
   * Used namespace of the bucket.
   */
  private long usedNamespace;

  /**
   * Creation time of the bucket.
   */
  private Instant creationTime;

  /**
   * Modification time of the bucket.
   */
  private Instant modificationTime;

  /**
   * Bucket Encryption key name if bucket encryption is enabled.
   */
  private String encryptionKeyName;

  private OzoneObj ozoneObj;

  private String sourceVolume;
  private String sourceBucket;
  private boolean sourcePathExist = true;

  /**
   * Quota of bytes allocated for the bucket.
   */
  private long quotaInBytes;
  /**
   * Quota of key count allocated for the bucket.
   */
  private long quotaInNamespace;
  /**
   * Bucket Layout.
   */
  private BucketLayout bucketLayout = BucketLayout.DEFAULT;
  /**
   * Bucket Owner.
   */
  private String owner;

  protected OzoneBucket(Builder builder) {
    super(builder);
    this.proxy = builder.proxy;
    this.volumeName = builder.volumeName;
    this.name = builder.name;  // bucket name
    // Bucket level replication is not configured by default.
    this.defaultReplication = builder.defaultReplicationConfig != null ?
        builder.defaultReplicationConfig.getReplicationConfig() : null;
    this.storageType = builder.storageType;
    this.versioning = builder.versioning;
    if (builder.conf != null) {
      this.listCacheSize = HddsClientUtils.getListCacheSize(builder.conf);
    }
    this.usedBytes = builder.usedBytes;
    this.usedNamespace = builder.usedNamespace;
    this.creationTime = Instant.ofEpochMilli(builder.creationTime);
    if (builder.modificationTime != 0) {
      this.modificationTime = Instant.ofEpochMilli(builder.modificationTime);
    } else {
      modificationTime = Instant.now();
      if (modificationTime.isBefore(this.creationTime)) {
        modificationTime = Instant.ofEpochSecond(
            this.creationTime.getEpochSecond(), this.creationTime.getNano());
      }
    }
    this.encryptionKeyName = builder.encryptionKeyName;
    this.ozoneObj = OzoneObjInfo.Builder.newBuilder()
        .setBucketName(name)
        .setVolumeName(volumeName)
        .setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OzoneObj.StoreType.OZONE).build();
    this.sourceVolume = builder.sourceVolume;
    this.sourceBucket = builder.sourceBucket;
    this.quotaInBytes = builder.quotaInBytes;
    this.quotaInNamespace = builder.quotaInNamespace;
    if (builder.bucketLayout != null) {
      this.bucketLayout = builder.bucketLayout;
    }
    this.owner = builder.owner;
  }

  /**
   * Returns Volume Name.
   *
   * @return volumeName
   */
  public String getVolumeName() {
    return volumeName;
  }

  /**
   * Returns Bucket Name.
   *
   * @return bucketName
   */
  public String getName() {
    return name;
  }

  /**
   * Returns ACL's associated with the Bucket.
   *
   * @return acls
   */
  @JsonIgnore
  public List<OzoneAcl> getAcls() throws IOException {
    return proxy.getAcl(ozoneObj);
  }

  /**
   * Returns StorageType of the Bucket.
   *
   * @return storageType
   */
  public StorageType getStorageType() {
    return storageType;
  }

  /**
   * Returns Versioning associated with the Bucket.
   *
   * @return versioning
   */
  public Boolean getVersioning() {
    return versioning;
  }

  /**
   * Returns creation time of the Bucket.
   *
   * @return creation time of the bucket
   */
  public Instant getCreationTime() {
    return creationTime;
  }

  /**
   * Returns modification time of the Bucket.
   *
   * @return modification time of the bucket
   */
  public Instant getModificationTime() {
    return modificationTime;
  }

  /**
   * Return the bucket encryption key name.
   * @return the bucket encryption key name
   */
  public String getEncryptionKeyName() {
    return encryptionKeyName;
  }

  public String getSourceVolume() {
    return sourceVolume;
  }

  public String getSourceBucket() {
    return sourceBucket;
  }

  /**
   * Returns Quota allocated for the Bucket in bytes.
   *
   * @return quotaInBytes
   */
  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  /**
   * Returns quota of key counts allocated for the Bucket.
   *
   * @return quotaInNamespace
   */
  public long getQuotaInNamespace() {
    return quotaInNamespace;
  }

  /**
   * Returns the owner of the Bucket.
   *
   * @return owner
   */
  public String getOwner() {
    return owner;
  }

  public int getListCacheSize() {
    return listCacheSize;
  }

  /**
   * Builder for OmBucketInfo.
  /**
   * Adds ACLs to the Bucket.
   * @param addAcl ACL to be added
   * @return true - if acl is successfully added, false if acl already exists
   * for the bucket.
   * @throws IOException
   */
  public boolean addAcl(OzoneAcl addAcl) throws IOException {
    return proxy.addAcl(ozoneObj, addAcl);
  }

  /**
   * Removes ACLs from the bucket.
   * @return true - if acl is successfully removed, false if acl to be
   * removed does not exist for the bucket.
   * @throws IOException
   */
  public boolean removeAcl(OzoneAcl removeAcl) throws IOException {
    return proxy.removeAcl(ozoneObj, removeAcl);
  }

  /**
   * Acls to be set for given Ozone object. This operations reset ACL for
   * given object to list of ACLs provided in argument.
   * @param acls List of acls.
   *
   * @throws IOException if there is error.
   * */
  public boolean setAcl(List<OzoneAcl> acls) throws IOException {
    return proxy.setAcl(ozoneObj, acls);
  }

  /**
   * Sets/Changes the storage type of the bucket.
   * @param newStorageType Storage type to be set
   * @throws IOException
   */
  public void setStorageType(StorageType newStorageType) throws IOException {
    proxy.setBucketStorageType(volumeName, name, newStorageType);
    storageType = newStorageType;
  }

  /**
   * Enable/Disable versioning of the bucket.
   * @param newVersioning
   * @throws IOException
   */
  public void setVersioning(Boolean newVersioning) throws IOException {
    proxy.setBucketVersioning(volumeName, name, newVersioning);
    versioning = newVersioning;
  }

  /**
   * Clean the space quota of the bucket.
   *
   * @throws IOException
   */
  public void clearSpaceQuota() throws IOException {
    OzoneBucket ozoneBucket = proxy.getBucketDetails(volumeName, name);
    proxy.setBucketQuota(volumeName, name, ozoneBucket.getQuotaInNamespace(),
        QUOTA_RESET);
    quotaInBytes = QUOTA_RESET;
    quotaInNamespace = ozoneBucket.getQuotaInNamespace();
  }

  /**
   * Clean the namespace quota of the bucket.
   *
   * @throws IOException
   */
  public void clearNamespaceQuota() throws IOException {
    OzoneBucket ozoneBucket = proxy.getBucketDetails(volumeName, name);
    proxy.setBucketQuota(volumeName, name, QUOTA_RESET,
        ozoneBucket.getQuotaInBytes());
    quotaInBytes = ozoneBucket.getQuotaInBytes();
    quotaInNamespace = QUOTA_RESET;
  }

  /**
   * Sets/Changes the quota of this Bucket.
   *
   * @param quota OzoneQuota Object that can be applied to storage bucket.
   * @throws IOException
   */
  public void setQuota(OzoneQuota quota) throws IOException {
    proxy.setBucketQuota(volumeName, name, quota.getQuotaInNamespace(),
        quota.getQuotaInBytes());
    quotaInBytes = quota.getQuotaInBytes();
    quotaInNamespace = quota.getQuotaInNamespace();
  }

  /**
   * Sets/Changes the default replication config of this Bucket.
   *
   * @param replicationConfig Replication config that can be applied to bucket.
   * @throws IOException
   */
  public void setReplicationConfig(ReplicationConfig replicationConfig)
      throws IOException {
    proxy.setReplicationConfig(volumeName, name, replicationConfig);
  }

  public void setListCacheSize(int listCacheSize) {
    this.listCacheSize = listCacheSize;
  }

  @Deprecated
  public void setEncryptionKey(String bekName) throws IOException {
    proxy.setEncryptionKey(volumeName, name, bekName);
    encryptionKeyName = bekName;
  }

  /**
   * Creates a new key in the bucket, with default replication type RATIS and
   * with replication factor THREE.
   * @param key Name of the key to be created.
   * @param size Size of the data the key will point to.
   * @return OzoneOutputStream to which the data has to be written.
   * @throws IOException
   */
  public OzoneOutputStream createKey(String key, long size)
      throws IOException {
    return createKey(key, size, defaultReplication,
        Collections.emptyMap());
  }

  /**
   * Creates a new key in the bucket.
   * @param key Name of the key to be created.
   * @param size Size of the data the key will point to.
   * @param type Replication type to be used.
   * @param factor Replication factor of the key.
   * @return OzoneOutputStream to which the data has to be written.
   * @throws IOException
   */
  @Deprecated
  public OzoneOutputStream createKey(String key, long size,
      ReplicationType type,
      ReplicationFactor factor,
      Map<String, String> keyMetadata)
      throws IOException {
    return proxy
        .createKey(volumeName, name, key, size, type, factor, keyMetadata);
  }

  /**
   * Creates a new key in the bucket.
   *
   * @param key               Name of the key to be created.
   * @param size              Size of the data the key will point to.
   * @param replicationConfig Replication configuration.
   * @param keyMetadata       Custom key metadata.
   * @return OzoneOutputStream to which the data has to be written.
   * @throws IOException
   */
  public OzoneOutputStream createKey(String key, long size,
      ReplicationConfig replicationConfig,
      Map<String, String> keyMetadata)
      throws IOException {
    return this.createKey(key, size, replicationConfig, keyMetadata, Collections.emptyMap());
  }

  /**
   * Creates a new key in the bucket.
   *
   * @param key               Name of the key to be created.
   * @param size              Size of the data the key will point to.
   * @param replicationConfig Replication configuration.
   * @param keyMetadata       Custom key metadata.
   * @param tags              Tags used for S3 object tags
   * @return OzoneOutputStream to which the data has to be written.
   * @throws IOException
   */
  public OzoneOutputStream createKey(String key, long size,
      ReplicationConfig replicationConfig,
      Map<String, String> keyMetadata,
      Map<String, String> tags)
      throws IOException {
    return proxy
        .createKey(volumeName, name, key, size, replicationConfig, keyMetadata, tags);
  }

  /**
   * This API allows to atomically update an existing key. The key read before invoking this API
   * should remain unchanged for this key to be written. This is controlled by the generation
   * field in the existing Key param. If the key is replaced or updated the generation will change. If the
   * generation has changed since the existing Key was read, either the initial key create will fail,
   * or the key will fail to commit after the data has been written as the checks are carried out
   * both at key open and commit time.
   *
   * @param keyName Existing key to rewrite. This must exist in the bucket.
   * @param size The size of the new key
   * @param existingKeyGeneration The generation of the existing key which is checked for changes at key create
   *                              and commit time.
   * @param replicationConfig The replication configuration for the key to be rewritten.
   * @param metadata custom key value metadata
   * @return OzoneOutputStream to which the data has to be written.
   * @throws IOException
   */
  public OzoneOutputStream rewriteKey(String keyName, long size, long existingKeyGeneration,
      ReplicationConfig replicationConfig, Map<String, String> metadata) throws IOException {
    return proxy.rewriteKey(volumeName, name, keyName, size, existingKeyGeneration, replicationConfig, metadata);
  }

  /**
   * Creates a key only if it does not exist (S3 If-None-Match: * semantics).
   *
   * @param keyName Name of the key
   * @param size Size of the data
   * @param replicationConfig Replication configuration
   * @param metadata custom key value metadata
   * @param tags Tags used for S3 object tags
   * @return OzoneOutputStream to which the data has to be written.
   * @throws IOException
   */
  public OzoneOutputStream createKeyIfNotExists(String keyName, long size,
      ReplicationConfig replicationConfig, Map<String, String> metadata,
      Map<String, String> tags) throws IOException {
    return proxy.createKeyIfNotExists(volumeName, name, keyName, size,
        replicationConfig, metadata, tags);
  }

  /**
   * Rewrites a key only if its ETag matches (S3 If-Match semantics).
   *
   * @param keyName Name of the key
   * @param size Size of the data
   * @param expectedETag The ETag value the existing key must have
   * @param replicationConfig Replication configuration
   * @param metadata custom key value metadata
   * @param tags Tags used for S3 object tags
   * @return OzoneOutputStream to which the data has to be written.
   * @throws IOException
   */
  public OzoneOutputStream rewriteKeyIfMatch(String keyName, long size,
      String expectedETag, ReplicationConfig replicationConfig,
      Map<String, String> metadata, Map<String, String> tags)
      throws IOException {
    return proxy.rewriteKeyIfMatch(volumeName, name, keyName, size,
        expectedETag, replicationConfig, metadata, tags);
  }

  /**
   * Creates a new key in the bucket, with default replication type RATIS and
   * with replication factor THREE.
   *
   * @param key  Name of the key to be created.
   * @param size Size of the data the key will point to.
   * @return OzoneOutputStream to which the data has to be written.
   * @throws IOException
   */
  public OzoneDataStreamOutput createStreamKey(String key, long size)
      throws IOException {
    return createStreamKey(key, size, defaultReplication,
        Collections.emptyMap());
  }

  /**
   * Creates a new key in the bucket.
   *
   * @param key               Name of the key to be created.
   * @param size              Size of the data the key will point to.
   * @param replicationConfig Replication configuration.
   * @param keyMetadata       Custom key metadata.
   * @return OzoneDataStreamOutput to which the data has to be written.
   * @throws IOException
   */
  public OzoneDataStreamOutput createStreamKey(String key, long size,
      ReplicationConfig replicationConfig, Map<String, String> keyMetadata)
      throws IOException {
    if (replicationConfig == null) {
      replicationConfig = defaultReplication;
    }
    return this.createStreamKey(key, size, replicationConfig, keyMetadata,
        Collections.emptyMap());
  }

  /**
   * Creates a new key in the bucket.
   *
   * @param key               Name of the key to be created.
   * @param size              Size of the data the key will point to.
   * @param replicationConfig Replication configuration.
   * @param keyMetadata       Custom key metadata.
   * @return OzoneDataStreamOutput to which the data has to be written.
   * @throws IOException
   */
  public OzoneDataStreamOutput createStreamKey(String key, long size,
      ReplicationConfig replicationConfig, Map<String, String> keyMetadata,
      Map<String, String> tags) throws IOException {
    if (replicationConfig == null) {
      replicationConfig = defaultReplication;
    }
    return proxy.createStreamKey(volumeName, name, key, size,
        replicationConfig, keyMetadata, tags);
  }

  /**
   * Reads an existing key from the bucket.
   *
   * @param key Name of the key to be read.
   * @return OzoneInputStream the stream using which the data can be read.
   * @throws IOException
   */
  public OzoneInputStream readKey(String key) throws IOException {
    return proxy.getKey(volumeName, name, key);
  }

  /**
   * Returns information about the key.
   * @param key Name of the key.
   * @return OzoneKeyDetails Information about the key.
   * @throws IOException
   */
  public OzoneKeyDetails getKey(String key) throws IOException {
    return proxy.getKeyDetails(volumeName, name, key);
  }

  /**
   *
   * Returns OzoneKey that contains the application generated/visible
   * metadata for an Ozone Object.
   *
   * If Key exists, return returns OzoneKey.
   * If Key does not exist, throws an exception with error code KEY_NOT_FOUND
   *
   * @param key
   * @return OzoneKey which gives basic information about the key.
   * @throws IOException
   */
  public OzoneKey headObject(String key) throws IOException {
    return proxy.headObject(volumeName, name, key);
  }

  public long getUsedBytes() {
    return usedBytes;
  }

  public long getUsedNamespace() {
    return usedNamespace;
  }

  /**
   * Returns Iterator to iterate over all keys in the bucket.
   * The result can be restricted using key prefix, will return all
   * keys if key prefix is null.
   *
   * @param keyPrefix Bucket prefix to match
   * @return {@code Iterator<OzoneKey>}
   */
  public Iterator<? extends OzoneKey> listKeys(String keyPrefix)
      throws IOException {
    return listKeys(keyPrefix, null);
  }

  /**
   * Returns Iterator to iterate over all keys after prevKey in the bucket.
   * If prevKey is null it iterates from the first key in the bucket.
   * The result can be restricted using key prefix, will return all
   * keys if key prefix is null.
   *
   * @param keyPrefix Bucket prefix to match
   * @param prevKey Keys will be listed after this key name
   * @return {@code Iterator<OzoneKey>}
   */
  public Iterator<? extends OzoneKey> listKeys(String keyPrefix, String prevKey)
      throws IOException {
    return listKeys(keyPrefix, prevKey, false);
  }

  /**
   * Returns Iterator to iterate over all keys after prevKey in the bucket.
   * If shallow is true, iterator will only contain immediate children.
   * This applies to the aws s3 list with delimiter '/' scenario.
   * Note: When shallow is true, whether keyPrefix ends with slash or not
   * will affect the results, see {@code getNextShallowListOfKeys}.
   *
   * @param keyPrefix Bucket prefix to match
   * @param prevKey Keys will be listed after this key name
   * @param shallow If true, only list immediate children ozoneKeys
   * @return {@code Iterator<OzoneKey>}
   */
  public Iterator<? extends OzoneKey> listKeys(String keyPrefix, String prevKey,
      boolean shallow) throws IOException {
    return new KeyIteratorFactory()
        .getKeyIterator(keyPrefix, prevKey, bucketLayout, shallow);
  }

  /**
   * Checks if the bucket is a Link Bucket.
   * @return True if bucket is a link, False otherwise.
   */
  public boolean isLink() {
    return sourceVolume != null && sourceBucket != null;
  }

  /**
   * Deletes key from the bucket.
   * @param key Name of the key to be deleted.
   * @throws IOException
   */
  public void deleteKey(String key) throws IOException {
    proxy.deleteKey(volumeName, name, key, false);
  }

  /**
   * Ozone FS api to delete a directory. Sub directories will be deleted if
   * recursive flag is true, otherwise it will be non-recursive.
   *
   * @param key       Name of the key to be deleted.
   * @param recursive recursive deletion of all sub path keys if true,
   *                  otherwise non-recursive
   * @throws IOException
   */
  public void deleteDirectory(String key, boolean recursive)
      throws IOException {
    proxy.deleteKey(volumeName, name, key, recursive);
  }

  /**
   * Deletes the given list of keys from the bucket.
   * @param keyList List of the key name to be deleted.
   * @throws IOException
   */
  public void deleteKeys(List<String> keyList) throws IOException {
    proxy.deleteKeys(volumeName, name, keyList);
  }

  /**
   * Deletes the given list of keys from the bucket.
   * @param keyList List of the key name to be deleted.
   * @param quiet flag to not throw exception if delete fails
   * @throws IOException
   */
  public Map<String, ErrorInfo> deleteKeys(List<String> keyList, boolean quiet) throws IOException {
    return proxy.deleteKeys(volumeName, name, keyList, quiet);
  }

  /**
   * Rename the keyname from fromKeyName to toKeyName.
   * @param fromKeyName The original key name.
   * @param toKeyName New key name.
   * @throws IOException
   */
  public void renameKey(String fromKeyName, String toKeyName)
      throws IOException {
    proxy.renameKey(volumeName, name, fromKeyName, toKeyName);
  }

  /**
   * Rename the key by keyMap, The key is fromKeyName and value is toKeyName.
   * @param keyMap The key is original key name nad value is new key name.
   * @throws IOException
   */
  @Deprecated
  public void renameKeys(Map<String, String> keyMap)
      throws IOException {
    proxy.renameKeys(volumeName, name, keyMap);
  }

  /**
   * Initiate multipart upload for a specified key.
   * @param keyName Name of the key to be created when the multipart upload is completed.
   * @param type Replication type to be used.
   * @param factor Replication factor of the key.
   * @return OmMultipartInfo
   * @throws IOException
   * @deprecated Use {@link OzoneBucket#initiateMultipartUpload(String, ReplicationConfig)} instead.
   */
  @Deprecated
  public OmMultipartInfo initiateMultipartUpload(String keyName,
      ReplicationType type,
      ReplicationFactor factor)
      throws IOException {
    return proxy.initiateMultipartUpload(volumeName, name, keyName, type,
        factor);
  }

  /**
   * Initiate multipart upload for a specified key.
   * @param keyName Name of the key to be created when the multipart upload is completed.
   * @param config Replication config.
   * @return OmMultipartInfo
   * @throws IOException
   */
  public OmMultipartInfo initiateMultipartUpload(String keyName,
      ReplicationConfig config)
      throws IOException {
    return initiateMultipartUpload(keyName, config, Collections.emptyMap());
  }

  /**
   * Initiate multipart upload for a specified key.
   * @param keyName Name of the key to be created when the multipart upload is completed.
   * @param config Replication config.
   * @param metadata Custom key metadata.
   * @return OmMultipartInfo
   * @throws IOException
   */
  public OmMultipartInfo initiateMultipartUpload(String keyName,
      ReplicationConfig config, Map<String, String> metadata)
      throws IOException {
    return initiateMultipartUpload(keyName, config, metadata, Collections.emptyMap());
  }

  /**
   * Initiate multipart upload for a specified key.
   * @param keyName Name of the key to be created when the multipart upload is completed.
   * @param config Replication config.
   * @param metadata Custom key metadata.
   * @param tags Tags used for S3 object tags.
   * @return OmMultipartInfo
   * @throws IOException
   */
  public OmMultipartInfo initiateMultipartUpload(String keyName,
      ReplicationConfig config, Map<String, String> metadata,
      Map<String, String> tags)
      throws IOException {
    return proxy.initiateMultipartUpload(volumeName, name, keyName, config, metadata, tags);
  }

  /**
   * Initiate multipart upload for a specified key, with default replication
   * type RATIS and with replication factor THREE.
   *
   * @param key Name of the key to be created.
   * @return OmMultipartInfo.
   * @throws IOException
   */
  public OmMultipartInfo initiateMultipartUpload(String key)
      throws IOException {
    return initiateMultipartUpload(key, defaultReplication);
  }

  /**
   * Create a part key for a multipart upload key.
   * @param key
   * @param size
   * @param partNumber
   * @param uploadID
   * @return OzoneOutputStream
   * @throws IOException
   */
  public OzoneOutputStream createMultipartKey(String key, long size,
                                              int partNumber, String uploadID)
      throws IOException {
    return proxy.createMultipartKey(volumeName, name, key, size, partNumber,
        uploadID);
  }

  /**
   * Create a part key for a multipart upload key.
   * @param key
   * @param size
   * @param partNumber
   * @param uploadID
   * @return OzoneDataStreamOutput
   * @throws IOException
   */
  public OzoneDataStreamOutput createMultipartStreamKey(String key,
      long size, int partNumber, String uploadID) throws IOException {
    return proxy.createMultipartStreamKey(volumeName, name,
            key, size, partNumber, uploadID);
  }

  /**
   * Complete Multipart upload. This will combine all the parts and make the
   * key visible in ozone.
   * @param key
   * @param uploadID
   * @param partsMap
   * @return OmMultipartUploadCompleteInfo
   * @throws IOException
   */
  public OmMultipartUploadCompleteInfo completeMultipartUpload(String key,
      String uploadID, Map<Integer, String> partsMap) throws IOException {
    return proxy.completeMultipartUpload(volumeName, name, key, uploadID,
        partsMap);
  }

  /**
   * Abort multipart upload request.
   * @param keyName
   * @param uploadID
   * @throws IOException
   */
  public void abortMultipartUpload(String keyName, String uploadID) throws
      IOException {
    proxy.abortMultipartUpload(volumeName, name, keyName, uploadID);
  }

  /**
   * Returns list of parts of a multipart upload key.
   * @param keyName
   * @param uploadID
   * @param partNumberMarker
   * @param maxParts
   * @return OzoneMultipartUploadPartListParts
   */
  public OzoneMultipartUploadPartListParts listParts(String keyName,
      String uploadID, int partNumberMarker, int maxParts)  throws IOException {
    // As at most we  can have 10000 parts for a key, not using iterator. If
    // needed, it can be done later. So, if we send 10000 as max parts at
    // most in a single rpc call, we return 0.6 mb, by assuming each part
    // size as 60 bytes (ignored the replication type size during calculation)

    return proxy.listParts(volumeName, name, keyName, uploadID,
              partNumberMarker, maxParts);
  }

  /**
   * OzoneFS api to get file status for an entry.
   *
   * @param keyName Key name
   * @throws OMException if file does not exist
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  public OzoneFileStatus getFileStatus(String keyName) throws IOException {
    return proxy.getOzoneFileStatus(volumeName, name, keyName);
  }

  /**
   * Ozone FS api to create a directory. Parent directories if do not exist
   * are created for the input directory.
   *
   * @param keyName Key name
   * @throws OMException if any entry in the path exists as a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  public void createDirectory(String keyName) throws IOException {
    proxy.createDirectory(volumeName, name, keyName);
  }

  /**
   * OzoneFS api to creates an input stream for a file.
   *
   * @param keyName Key name
   * @throws OMException if given key is not found or it is not a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  public OzoneInputStream readFile(String keyName) throws IOException {
    return proxy.readFile(volumeName, name, keyName);
  }

  /**
   * OzoneFS api to creates an output stream for a file.
   *
   * @param keyName   Key name
   * @param overWrite if true existing file at the location will be overwritten
   * @param recursive if true file would be created even if parent directories
   *                    do not exist
   * @throws OMException if given key is a directory
   *                     if file exists and isOverwrite flag is false
   *                     if an ancestor exists as a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  @Deprecated
  public OzoneOutputStream createFile(String keyName, long size,
      ReplicationType type, ReplicationFactor factor, boolean overWrite,
      boolean recursive) throws IOException {
    return proxy
        .createFile(volumeName, name, keyName, size, type, factor, overWrite,
            recursive);
  }

  /**
   * OzoneFS api to creates an output stream for a file.
   *
   * @param keyName   Key name
   * @param overWrite if true existing file at the location will be overwritten
   * @param recursive if true file would be created even if parent directories
   *                    do not exist
   * @throws OMException if given key is a directory
   *                     if file exists and isOverwrite flag is false
   *                     if an ancestor exists as a file
   *                     if bucket does not exist
   * @throws IOException if there is error in the db
   *                     invalid arguments
   */
  public OzoneOutputStream createFile(String keyName, long size,
      ReplicationConfig replicationConfig, boolean overWrite,
      boolean recursive) throws IOException {
    return proxy
        .createFile(volumeName, name, keyName, size, replicationConfig,
            overWrite, recursive);
  }

  public OzoneDataStreamOutput createStreamFile(String keyName, long size,
      ReplicationConfig replicationConfig, boolean overWrite,
      boolean recursive) throws IOException {
    return proxy.createStreamFile(volumeName, name, keyName, size,
        replicationConfig, overWrite, recursive);
  }

  /**
   * List the status for a file or a directory and its contents.
   *
   * @param keyName    Absolute path of the entry to be listed
   * @param recursive  For a directory if true all the descendants of a
   *                   particular directory are listed
   * @param startKey   Key from which listing needs to start. If startKey exists
   *                   its status is included in the final list.
   * @param numEntries Number of entries to list from the start key
   * @return list of file status
   */
  public List<OzoneFileStatus> listStatus(String keyName, boolean recursive,
      String startKey, long numEntries) throws IOException {
    return proxy.listStatus(volumeName, name, keyName, recursive, startKey, numEntries);
  }

  /**
   * List the lightweight status for a file or a directory and its contents.
   *
   * @param keyName    Absolute path of the entry to be listed
   * @param recursive  For a directory if true all the descendants of a
   *                   particular directory are listed
   * @param startKey   Key from which listing needs to start. If startKey exists
   *                   its status is included in the final list.
   * @param numEntries Number of entries to list from the start key
   * @return list of file status
   */
  public List<OzoneFileStatusLight> listStatusLight(String keyName, boolean recursive,
      String startKey, long numEntries) throws IOException {
    return proxy.listStatusLight(volumeName, name, keyName, recursive, startKey, numEntries, false);
  }

  /**
   * List the status for a file or a directory and its contents.
   *
   * @param keyName    Absolute path of the entry to be listed
   * @param recursive  For a directory if true all the descendants of a
   *                   particular directory are listed
   * @param startKey   Key from which listing needs to start. If startKey exists
   *                   its status is included in the final list.
   * @param numEntries Number of entries to list from the start key
   * @param allowPartialPrefix allow partial prefixes during listStatus,
   *                           this is used in context of listKeys calling
   *                           listStatus
   * @return list of file status
   */
  public List<OzoneFileStatus> listStatus(String keyName, boolean recursive,
      String startKey, long numEntries, boolean allowPartialPrefix)
      throws IOException {
    return proxy
        .listStatus(volumeName, name, keyName, recursive, startKey,
            numEntries, allowPartialPrefix);
  }

  /**
   * Return with the list of the in-flight multipart uploads.
   *
   * @param prefix Optional string to filter for the selected keys.
   */
  public OzoneMultipartUploadList listMultipartUploads(String prefix,
      String keyMarker, String uploadIdMarker, int maxUploads)
      throws IOException {
    return proxy.listMultipartUploads(volumeName, getName(), prefix, keyMarker, uploadIdMarker, maxUploads);
  }

  /**
   * Sets/Changes the owner of this Bucket.
   * @param userName new owner
   * @throws IOException
   */
  public boolean setOwner(String userName) throws IOException {
    boolean result = proxy.setBucketOwner(volumeName, name, userName);
    this.owner = userName;
    return result;
  }

  /**
   * Builder for OmBucketInfo.
   /**
   * Set time to a key in this bucket.
   * @param keyName Full path name to the key in the bucket.
   * @param mtime Modification time. Unchanged if -1.
   * @param atime Access time. Unchanged if -1.
   * @throws IOException
   */
  public void setTimes(String keyName, long mtime, long atime)
      throws IOException {
    proxy.setTimes(ozoneObj, keyName, mtime, atime);
  }

  /**
   * Gets the tags for an existing key.
   * @param keyName Key name.
   * @return Tags for the specified key.
   * @throws IOException
   */
  public Map<String, String> getObjectTagging(String keyName)
      throws IOException {
    return proxy.getObjectTagging(volumeName, name, keyName);
  }

  /**
   * Sets the tags to an existing key.
   * @param keyName Key name.
   * @param tags Tags to set on the key.
   * @throws IOException
   */
  public void putObjectTagging(String keyName, Map<String, String> tags)
      throws IOException {
    proxy.putObjectTagging(volumeName, name, keyName, tags);
  }

  /**
   * Removes all the tags from an existing key.
   * @param keyName Key name
   * @throws IOException
   */
  public void deleteObjectTagging(String keyName) throws IOException {
    proxy.deleteObjectTagging(volumeName, name, keyName);
  }

  public void setSourcePathExist(boolean b) {
    this.sourcePathExist = b;
  }

  public boolean isSourcePathExist() {
    return this.sourcePathExist;
  }

  public static Builder newBuilder(ConfigurationSource conf,
      ClientProtocol proxy) {
    Objects.requireNonNull(proxy, "Client proxy is not set.");
    return new Builder(conf, proxy);
  }

  /**
   * Inner builder for OzoneBucket.
   */
  public static class Builder extends WithMetadata.Builder {
    private ConfigurationSource conf;
    private ClientProtocol proxy;
    private String volumeName;
    private String name;
    private DefaultReplicationConfig defaultReplicationConfig;
    private StorageType storageType;
    private Boolean versioning;
    private long usedBytes;
    private long usedNamespace;
    private long creationTime;
    private long modificationTime;
    private String encryptionKeyName;
    private String sourceVolume;
    private String sourceBucket;
    private long quotaInBytes;
    private long quotaInNamespace;
    private BucketLayout bucketLayout;
    private String owner;

    protected Builder() {
    }

    private Builder(ConfigurationSource conf, ClientProtocol proxy) {
      this.conf = conf;
      this.proxy = proxy;
    }

    @Override
    public Builder setMetadata(Map<String, String> metadata) {
      super.setMetadata(metadata);
      return this;
    }

    public Builder setVolumeName(String volumeName) {
      this.volumeName = volumeName;
      return this;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setDefaultReplicationConfig(
        DefaultReplicationConfig defaultReplicationConfig) {
      this.defaultReplicationConfig = defaultReplicationConfig;
      return this;
    }

    public Builder setStorageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
    }

    public Builder setVersioning(Boolean versioning) {
      this.versioning = versioning;
      return this;
    }

    public Builder setUsedBytes(long usedBytes) {
      this.usedBytes = usedBytes;
      return this;
    }

    public Builder setUsedNamespace(long usedNamespace) {
      this.usedNamespace = usedNamespace;
      return this;
    }

    public Builder setCreationTime(long creationTime) {
      this.creationTime = creationTime;
      return this;
    }

    public Builder setModificationTime(long modificationTime) {
      this.modificationTime = modificationTime;
      return this;
    }

    public Builder setEncryptionKeyName(String encryptionKeyName) {
      this.encryptionKeyName = encryptionKeyName;
      return this;
    }

    public Builder setSourceVolume(String sourceVolume) {
      this.sourceVolume = sourceVolume;
      return this;
    }

    public Builder setSourceBucket(String sourceBucket) {
      this.sourceBucket = sourceBucket;
      return this;
    }

    public Builder setQuotaInBytes(long quotaInBytes) {
      this.quotaInBytes = quotaInBytes;
      return this;
    }

    public Builder setQuotaInNamespace(long quotaInNamespace) {
      this.quotaInNamespace = quotaInNamespace;
      return this;
    }

    public Builder setBucketLayout(BucketLayout bucketLayout) {
      this.bucketLayout = bucketLayout;
      return this;
    }

    public Builder setOwner(String owner) {
      this.owner = owner;
      return this;
    }

    public OzoneBucket build() {
      return new OzoneBucket(this);
    }
  }

  /**
   * An Iterator to iterate over {@link OzoneKey} list.
   */
  private class KeyIterator implements Iterator<OzoneKey> {

    private String keyPrefix = null;
    private Iterator<OzoneKey> currentIterator;
    private OzoneKey currentValue;
    private final boolean shallow;
    private boolean addedKeyPrefix;
    private String delimiterKeyPrefix;

    boolean shallow() {
      return shallow;
    }

    String getKeyPrefix() {
      return keyPrefix;
    }

    void setKeyPrefix(String keyPrefixPath) {
      keyPrefix = keyPrefixPath;
    }

    boolean addedKeyPrefix() {
      return addedKeyPrefix;
    }

    void setAddedKeyPrefix(boolean addedKeyPrefix) {
      this.addedKeyPrefix = addedKeyPrefix;
    }

    String getDelimiterKeyPrefix() {
      return delimiterKeyPrefix;
    }

    void setDelimiterKeyPrefix(String delimiterKeyPrefix) {
      this.delimiterKeyPrefix = delimiterKeyPrefix;
    }

    /**
     * Creates an Iterator to iterate over all keys after prevKey in the bucket.
     * If prevKey is null it iterates from the first key in the bucket.
     * The returned keys match key prefix.
     * @param keyPrefix
     * @param prevKey
     * @param shallow
     */
    KeyIterator(String keyPrefix, String prevKey, boolean shallow)
        throws IOException {
      setKeyPrefix(keyPrefix);
      this.currentValue = null;
      this.shallow = shallow;
      this.currentIterator = getNextListOfKeys(prevKey).iterator();
    }

    @Override
    public boolean hasNext() {
      if (!currentIterator.hasNext() && currentValue != null) {
        try {
          currentIterator =
              getNextListOfKeys(currentValue.getName()).iterator();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return currentIterator.hasNext();
    }

    @Override
    public OzoneKey next() {
      if (hasNext()) {
        currentValue = currentIterator.next();
        return currentValue;
      }
      throw new NoSuchElementException();
    }

    /**
     * Gets the next set of key list using proxy.
     * @param prevKey
     * @return {@code List<OzoneKey>}
     */
    List<OzoneKey> getNextListOfKeys(String prevKey) throws
        IOException {
      // If shallow is true, only list immediate children
      if (shallow) {
        return getNextShallowListOfKeys(prevKey);
      }
      return proxy.listKeys(volumeName, name, keyPrefix, prevKey,
          listCacheSize);
    }

    /**
     * Using listStatusLight instead of listKeys avoiding listing all children
     * keys. Giving the structure of keys delimited by "/":
     *
     *                   buck-1
     *                     |
     *                     a
     *                     |
     *       -----------------------------------
     *      |           |                       |
     *      b1          b2                      b3
     *    -----       --------               ----------
     *    |    |      |    |   |             |    |     |
     *   c1   c2     d1   d2  d3             e1   e2   e3
     *                    |                  |
     *                --------               |
     *               |        |              |
     *            d21.txt   d22.txt        e11.txt
     *
     * For the above structure, the keys listed delimited "/" in order are
     * as follows:
     *      a/
     *      a/b1/
     *      a/b1/c1/
     *      a/b1/c2/
     *      a/b2/
     *      a/b2/d1/
     *      a/b2/d2/
     *      a/b2/d2/d21.txt
     *      a/b2/d2/d22.txt
     *      a/b2/d3/
     *      a/b3/
     *      a/b3/e1/
     *      a/b3/e1/e11.txt
     *      a/b3/e2/
     *      a/b3/e3/
     *
     * When keyPrefix ends without slash (/), the result as Example 1:
     * Example 1: keyPrefix="a/b2", prevKey=""
     *            result: [a/b2/]
     * Example 2: keyPrefix="a/b2/", prevKey=""
     *            result: [a/b2/d1/, a/b2/d2/, a/b2/d3/]
     * Example 3: keyPrefix="a/b2/", prevKey="a/b2/d2/d21.txt"
     *            result: [a/b2/d2/, a/b2/d3/]
     * Example 4: keyPrefix="a/b2/", prevKey="a/b2/d2/d22.txt"
     *            result: [a/b2/d3/]
     * Say, keyPrefix="a/b" and prevKey="", the results will be
     * [a/b1/, a/b2/, a/b3/]
     * In implementation, the keyPrefix "a/b" can be identified in listKeys,
     * but cannot be identified in listStatus. Therefore, keyPrefix "a/b"
     * needs to be split into keyPrefix "a" and call listKeys method to get
     * the next one key as the startKey in listStatusLight.
     */
    List<OzoneKey> getNextShallowListOfKeys(String prevKey)
        throws IOException {
      List<OzoneKey> resultList = new ArrayList<>();
      String startKey = prevKey;

      // 1. Get first element as startKey
      if (!addedKeyPrefix) {
        initDelimiterKeyPrefix();
        // prepare startKey
        List<OzoneKey> nextOneKeys =
            proxy.listKeys(volumeName, name, getKeyPrefix(), prevKey, 1);
        if (nextOneKeys.isEmpty()) {
          return nextOneKeys;
        }
        // Special case: ListKey expects keyPrefix element should present in
        // the resultList if startKey is blank or equals to keyPrefix.
        // The nextOneKey needs be added to the result because it will not be
        // present when using the 'listStatus' method.
        // Consider the case, keyPrefix="test/", prevKey="" or 'test1/',
        // then 'test/' will be added to the list result.
        startKey = nextOneKeys.get(0).getName();
        startKey = startKey == null ? "" : startKey;
        if (getKeyPrefix().endsWith(OZONE_URI_DELIMITER) &&
            startKey.equals(getKeyPrefix())) {
          resultList.add(nextOneKeys.get(0));
        }
      }

      // 2. Get immediate children by listStatusLight method
      List<OzoneFileStatusLight> statuses =
          proxy.listStatusLight(volumeName, name, delimiterKeyPrefix, false,
              startKey, listCacheSize, false);

      if (addedKeyPrefix && !statuses.isEmpty()) {
        // previous round already include the startKey, so remove it
        statuses.remove(0);
      } else {
        setAddedKeyPrefix(true);
      }

      List<OzoneKey> ozoneKeys = buildKeysWithKeyPrefix(statuses);

      resultList.addAll(ozoneKeys);
      return resultList;
    }

    protected void initDelimiterKeyPrefix() {
      setDelimiterKeyPrefix(getKeyPrefix());
      if (!getKeyPrefix().endsWith(OZONE_URI_DELIMITER)) {
        setDelimiterKeyPrefix(OzoneFSUtils.getParentDir(getKeyPrefix()));
      }
    }

    protected List<OzoneKey> buildKeysWithKeyPrefix(
        List<OzoneFileStatusLight> statuses) {
      return statuses.stream()
          .map(OzoneBucket::toOzoneKey)
          .filter(key -> StringUtils.startsWith(key.getName(), getKeyPrefix()))
          .collect(Collectors.toList());
    }

  }

  private static OzoneKey toOzoneKey(OzoneFileStatusLight status) {
    BasicOmKeyInfo keyInfo = status.getKeyInfo();
    String keyName = keyInfo.getKeyName();
    final Map<String, String> metadata;
    if (status.isDirectory()) {
      // add trailing slash to represent directory
      keyName = OzoneFSUtils.addTrailingSlashIfNeeded(keyName);
      metadata = Collections.emptyMap();
    } else {
      metadata = Collections.singletonMap(ETAG, keyInfo.getETag());
    }
    return new OzoneKey(keyInfo.getVolumeName(),
        keyInfo.getBucketName(), keyName,
        keyInfo.getDataSize(), keyInfo.getCreationTime(),
        keyInfo.getModificationTime(),
        keyInfo.getReplicationConfig(),
        metadata,
        status.isFile(),
        keyInfo.getOwnerName(),
        Collections.emptyMap());
  }

  /**
   * An Iterator to iterate over {@link OzoneKey} list.
   *
   *                  buck-1
   *                    |
   *                    a
   *                    |
   *      -----------------------------------
   *     |           |                       |
   *     b1          b2                      b3
   *   -----       --------               ----------
   *   |    |      |    |   |             |    |     |
   *  c1   c2     d1   d2  d3             e1   e2   e3
   *                   |                  |
   *               --------               |
   *              |        |              |
   *           d21.txt   d22.txt        e11.txt
   *
   * Say, keyPrefix="a" and prevKey="", then will do Depth-First-Traversal and
   * visit node to getChildren in below fashion:-
   * 1. getChildren("a/")  2. getChildren("a/b1")  3. getChildren("a/b1/c1")
   * 4. getChildren("a/b1/c2")  5. getChildren("a/b2/d1")
   * 6. getChildren("a/b2/d2")  7. getChildren("a/b2/d3")
   * 8. getChildren("a/b3/e1")  9. getChildren("a/b3/e2")
   * 10. getChildren("a/b3/e3")
   *
   * Note: Does not guarantee to return the list of keys in a sorted order.
   */
  private class KeyIteratorWithFSO extends KeyIterator {

    private Stack<Pair<String, String>> stack;
    private String removeStartKey = "";

    /**
     * Creates an Iterator to iterate over all keys after prevKey in the bucket.
     * If prevKey is null it iterates from the first key in the bucket.
     * The returned keys match key prefix.
     *
     * @param keyPrefix
     * @param prevKey
     * @param shallow
     */
    KeyIteratorWithFSO(String keyPrefix, String prevKey, boolean shallow)
        throws IOException {
      super(keyPrefix, prevKey, shallow);
    }

    /**
     * keyPrefix="a1" and startKey="a1/b2/d2/f3/f31.tx".
     * Now, this function will prepare and return a list :
     * <"a1/b2/d2", "a1/b2/d2/f3">
     * <"a1/b2", "a1/b2/d2">
     * <"a1", "a1/b2">
     *
     * @param keyPrefix keyPrefix
     * @param startKey  startKey
     * @param seekPaths list of seek paths between keyPrefix and startKey
     */
    private void getSeekPathsBetweenKeyPrefixAndStartKey(String keyPrefix,
        String startKey, List<Pair<String, String>> seekPaths) {

      String parentStartKeyPath = OzoneFSUtils.getParentDir(startKey);

      if (StringUtils.isNotBlank(startKey)) {
        if (StringUtils.compare(parentStartKeyPath, keyPrefix) >= 0) {
          seekPaths.add(new ImmutablePair<>(parentStartKeyPath, startKey));

          // recursively fetch all the sub-paths between keyPrefix and prevKey
          getSeekPathsBetweenKeyPrefixAndStartKey(keyPrefix, parentStartKeyPath,
              seekPaths);
        } else if (StringUtils.compare(startKey, keyPrefix) >= 0) {
          // Both keyPrefix and startKey reached at the same level.
          // Adds partial keyPrefix and startKey for seek.
          seekPaths.add(new ImmutablePair<>(keyPrefix, startKey));
        }
      }
    }

    @Override
    List<OzoneKey> getNextListOfKeys(String prevKey) throws IOException {
      if (stack == null) {
        stack = new Stack();
      }

      if (shallow()) {
        return getNextShallowListOfKeys(prevKey);
      }

      // normalize paths
      if (!addedKeyPrefix()) {
        if (!prepareStack(prevKey)) {
          return new ArrayList<>();
        }
      }

      // 1. Pop out top pair and get its immediate children
      List<OzoneKey> keysResultList = new ArrayList<>();
      if (stack.isEmpty()) {
        // case: startKey is empty
        if (getChildrenKeys(getKeyPrefix(), prevKey, keysResultList)) {
          return keysResultList;
        }
      }

      // 2. Pop element and seek for its sub-child path(s). Basically moving
      // seek pointer to next level(depth) in FS tree.
      // case: startKey is non-empty
      while (!stack.isEmpty()) {
        Pair<String, String> keyPrefixPath = stack.pop();
        if (getChildrenKeys(keyPrefixPath.getLeft(), keyPrefixPath.getRight(),
            keysResultList)) {
          // reached limit batch size.
          break;
        }
      }
      return keysResultList;
    }

    @Override
    List<OzoneKey> getNextShallowListOfKeys(String prevKey)
        throws IOException {
      List<OzoneKey> resultList = new ArrayList<>();
      String startKey = prevKey;
      boolean findFirstStartKey = false;

      if (!addedKeyPrefix()) {
        initDelimiterKeyPrefix();

        if (!prepareStack(prevKey)) {
          return new ArrayList<>();
        }

        // 1. Get first element as startKey.
        List<OzoneKey> firstKeyResult = new ArrayList<>();
        if (stack.isEmpty()) {
          // Case: startKey is empty
          getChildrenKeys(getKeyPrefix(), prevKey, firstKeyResult);
        } else {
          // Case: startKey is non-empty
          while (!stack.isEmpty()) {
            Pair<String, String> keyPrefixPath = stack.pop();
            getChildrenKeys(keyPrefixPath.getLeft(), keyPrefixPath.getRight(),
                firstKeyResult);
            if (!firstKeyResult.isEmpty()) {
              break;
            }
          }
        }
        if (!firstKeyResult.isEmpty()) {
          startKey = firstKeyResult.get(0).getName();
          findFirstStartKey = true;
        }

        // A specific case where findFirstStartKey is false does not mean that
        // the final result is empty because also need to determine whether
        // keyPrefix is an existing key. Consider the following structure:
        //     te/
        //     test1/
        //     test1/file1
        //     test1/file2
        //     test2/
        // when keyPrefix='te' and prevKey='test1/file2', findFirstStartKey
        // will be false because 'test1/file2' is the last key in dir
        // 'test1/'. In the correct result for this case, 'test2/' is expected
        // in the results and "test1/" should be excluded.
        //
        if (!findFirstStartKey) {
          if (StringUtils.isBlank(prevKey) || !keyPrefixExist()
              || !StringUtils.startsWith(prevKey, getKeyPrefix())) {
            return new ArrayList<>();
          }
        }
        // A special case where keyPrefix element should present in the
        // resultList. See the annotation of #addKeyPrefixInfoToResultList
        if (getKeyPrefix().equals(startKey) && findFirstStartKey
            && !firstKeyResult.get(0).isFile()) {
          resultList.add(firstKeyResult.get(0));
        }
        // Note that the startKey needs to be an immediate child of the
        // keyPrefix or black before calling listStatus.
        startKey = adjustStartKey(startKey);
        startKey = startKey == null ? "" : startKey;
      }

      // 2. Get immediate children by listStatus method.
      List<OzoneFileStatusLight> statuses =
          proxy.listStatusLight(volumeName, name, getDelimiterKeyPrefix(),
              false, startKey, listCacheSize, false);

      if (!statuses.isEmpty()) {
        // If findFirstStartKey is false, indicates that the keyPrefix is an
        // existing key and the prevKey is the last element within its
        // directory. In this case, the result should not include the
        // startKey itself.
        if (!findFirstStartKey && addedKeyPrefix()) {
          statuses.remove(0);
        }
        List<OzoneKey> ozoneKeys = buildKeysWithKeyPrefix(statuses);
        resultList.addAll(ozoneKeys);
      }
      return resultList;
    }

    private boolean prepareStack(String prevKey) throws IOException {
      prevKey = OmUtils.normalizeKey(prevKey, true);
      String keyPrefixName = "";
      if (StringUtils.isNotBlank(getKeyPrefix())) {
        keyPrefixName = OmUtils.normalizeKey(getKeyPrefix(), true);
      }
      setKeyPrefix(keyPrefixName);

      if (StringUtils.isNotBlank(prevKey)) {
        if (StringUtils.startsWith(prevKey, getKeyPrefix())) {
          // 1. Prepare all the seekKeys after the prefixKey.
          // Example case: prefixKey="a1", startKey="a1/b2/d2/f3/f31.tx"
          // Now, stack should be build with all the levels after prefixKey
          // Stack format => <keyPrefix and startKey>, startKey should be an
          // immediate child of keyPrefix.
          //             _______________________________________
          // Stack=> top | < a1/b2/d2/f3, a1/b2/d2/f3/f31.tx > |
          //             |-------------------------------------|
          //             | < a1/b2/d2, a1/b2/d2/f3 >           |
          //             |-------------------------------------|
          //             | < a1/b2, a1/b2/d2 >                 |
          //             |-------------------------------------|
          //      bottom | < a1, a1/b2 >                       |
          //             --------------------------------------|
          List<Pair<String, String>> seekPaths = new ArrayList<>();

          if (StringUtils.isNotBlank(getKeyPrefix())) {
            // If the prev key is a dir then seek its sub-paths
            // Say, prevKey="a1/b2/d2"
            addPrevDirectoryToSeekPath(prevKey, seekPaths);
          }

          // Key Prefix is Blank. The seek all the keys with startKey.
          removeStartKey = prevKey;
          getSeekPathsBetweenKeyPrefixAndStartKey(getKeyPrefix(), prevKey,
              seekPaths);

          // 2. Push elements in reverse order so that the FS tree traversal
          // will occur in left-to-right fashion[Depth-First Search]
          for (int index = seekPaths.size() - 1; index >= 0; index--) {
            Pair<String, String> seekDirPath = seekPaths.get(index);
            stack.push(seekDirPath);
          }
        } else if (StringUtils.isNotBlank(getKeyPrefix())) {
          if (!OzoneFSUtils.isAncestorPath(getKeyPrefix(), prevKey)) {
            // Case-1 - sibling: keyPrefix="a1/b2", startKey="a0/b123Invalid"
            // Skip traversing, if the startKey is not a sibling.
            // "a1/b", "a1/b1/e/"
            return false;
          } else if (StringUtils.compare(prevKey, getKeyPrefix()) < 0) {
            // Case-2 - compare: keyPrefix="a1/b2", startKey="a1/b123Invalid"
            // Since startKey is lexographically behind keyPrefix,
            // the seek precedence goes to keyPrefix.
            stack.push(new ImmutablePair<>(getKeyPrefix(), ""));
          }
        }
      }
      return true;
    }

    private String adjustStartKey(String startKey) {
      if (getKeyPrefix().endsWith(OZONE_URI_DELIMITER) &&
          getKeyPrefix().equals(startKey)) {
        return "";
      }
      return OzoneFSUtils.getImmediateChild(startKey, getDelimiterKeyPrefix());
    }

    private boolean keyPrefixExist() throws IOException {
      OzoneFileStatus keyPrefixStatus = null;
      try {
        keyPrefixStatus =
            proxy.getOzoneFileStatus(volumeName, name, getKeyPrefix());
      } catch (OMException ome) {
        // ignore exception
      }
      return keyPrefixStatus != null;
    }

    private void addPrevDirectoryToSeekPath(String prevKey,
        List<Pair<String, String>> seekPaths)
        throws IOException {
      try {
        OzoneFileStatus prevStatus =
            proxy.getOzoneFileStatus(volumeName, name, prevKey);
        if (prevStatus != null) {
          if (prevStatus.isDirectory()) {
            seekPaths.add(new ImmutablePair<>(prevKey, ""));
          }
        }
      } catch (OMException ome) {
        // ignore exception
      }
    }

    /**
     * List children under the given keyPrefix and startKey path. It does
     * recursive #listStatus calls to list all the sub-keys resultList.
     *
     *                  buck-1
     *                    |
     *                    a
     *                    |
     *      -----------------------------------
     *     |           |                       |
     *     b1          b2                      b3
     *   -----       --------               ----------
     *   |    |      |    |   |             |    |     |
     *  c1   c2     d1   d2  d3             e1   e2   e3
     *                   |                  |
     *               --------               |
     *              |        |              |
     *           d21.txt   d22.txt        e11.txt
     *
     * Say, KeyPrefix = "a" and startKey = null;
     *
     * Iteration-1) RPC call proxy#listStatus("a").
     *              Add b3, b2 and b1 to stack.
     * Iteration-2) pop b1 and do RPC call proxy#listStatus("b1")
     *              Add c2, c1 to stack.
     * Iteration-3) pop c1 and do RPC call proxy#listStatus("c1"). Empty list.
     * Iteration-4) pop c2 and do RPC call proxy#listStatus("c2"). Empty list.
     * Iteration-5) pop b2 and do RPC call proxy#listStatus("b2")
     *              Add d3, d2 and d1 to stack.
     *              ..........
     *              ..........
     * Iteration-n) pop e3 and do RPC call proxy#listStatus("e3")
     *              Reached end of the FS tree.
     *
     * @param keyPrefix
     * @param startKey
     * @param keysResultList
     * @return true represents it reached limit batch size, false otherwise.
     * @throws IOException
     */
    private boolean getChildrenKeys(String keyPrefix, String startKey,
        List<OzoneKey> keysResultList) throws IOException {

      // listStatus API expects a not null 'startKey' value
      startKey = startKey == null ? "" : startKey;

      // 1. Get immediate children of keyPrefix, starting with startKey
      List<OzoneFileStatusLight> statuses = proxy.listStatusLight(volumeName,
          name, keyPrefix, false, startKey, listCacheSize, true);

      // 2. Special case: ListKey expects keyPrefix element should present in
      // the resultList, only if startKey is blank. If startKey is not blank
      // then resultList shouldn't contain the startKey element.
      // Since proxy#listStatus API won't return keyPrefix element in the
      // resultList. So, this is to add user given keyPrefix to the return list.
      addKeyPrefixInfoToResultList(keyPrefix, startKey, keysResultList);

      // 3. Special case: ListKey expects startKey shouldn't present in the
      // resultList. Since proxy#listStatus API returns startKey element to
      // the returnList, this function is to remove the startKey element.
      removeStartKeyIfExistsInStatusList(startKey, statuses);

      // 4. Iterating over the resultStatuses list and add each key to the
      // resultList.
      for (int indx = 0; indx < statuses.size(); indx++) {
        OzoneFileStatusLight status = statuses.get(indx);
        BasicOmKeyInfo keyInfo = status.getKeyInfo();
        OzoneKey ozoneKey = toOzoneKey(status);
        keysResultList.add(ozoneKey);

        if (status.isDirectory()) {
          // Adding in-progress keyPath back to the stack to make sure
          // all the siblings will be fetched.
          stack.push(new ImmutablePair<>(keyPrefix, keyInfo.getKeyName()));
          // Adding current directory to the stack, so that this dir will be
          // the top element. Moving seek pointer to fetch sub-paths
          stack.push(new ImmutablePair<>(keyInfo.getKeyName(), ""));
          // Return it so that the next iteration will be
          // started using the stacked items.
          return true;
        } else if (indx == statuses.size() - 1) {
          // The last element is a FILE and reaches the listCacheSize.
          // Now, sets next seek key to this element
          stack.push(new ImmutablePair<>(keyPrefix, keyInfo.getKeyName()));
          // Return it so that the next iteration will be
          // started using the stacked items.
          return true;
        }
      }

      return false;
    }

    private void removeStartKeyIfExistsInStatusList(String startKey,
        List<OzoneFileStatusLight> statuses) {

      if (!statuses.isEmpty()) {
        String firstElement = statuses.get(0).getKeyInfo().getKeyName();
        String startKeyPath = startKey;
        if (StringUtils.isNotBlank(startKey)) {
          if (startKey.endsWith(OZONE_URI_DELIMITER)) {
            startKeyPath = OzoneFSUtils.removeTrailingSlashIfNeeded(startKey);
          }
        }

        // case-1) remove the startKey from the list as it should be skipped.
        // case-2) removeStartKey - as the startKey is a placeholder, which is
        //  managed internally to traverse leaf node's sub-paths.
        if (StringUtils.equals(firstElement, startKeyPath) ||
            StringUtils.equals(firstElement, removeStartKey)) {

          statuses.remove(0);
        }
      }
    }

    private void addKeyPrefixInfoToResultList(String keyPrefix,
        String startKey, List<OzoneKey> keysResultList) throws IOException {

      if (addedKeyPrefix()) {
        return;
      }

      // setting flag to true.
      setAddedKeyPrefix(true);

      // not required to addKeyPrefix
      // case-1) if keyPrefix is null/empty/just contains snapshot indicator
      // (The snapshot indicator is the null prefix equivalent for snapshot
      //  reads.)
      // case-2) if startKey is not null or empty
      if (StringUtils.isBlank(keyPrefix) || StringUtils.isNotBlank(startKey) ||
          OmUtils.isBucketSnapshotIndicator(keyPrefix)) {
        return;
      }

      OzoneFileStatus status = null;
      try {
        status = proxy.getOzoneFileStatus(volumeName, name,
            keyPrefix);
      } catch (OMException ome) {
        if (ome.getResult() == FILE_NOT_FOUND) {
          // keyPrefix path can't be found and skip adding it to result list
          return;
        }
      }

      if (status != null) {
        // not required to addKeyPrefix
        // case-3) if the keyPrefix corresponds to a file and not a dir,
        // prefix should not be added to avoid duplicate entry
        if (!status.isDirectory()) {
          return;
        }
        OmKeyInfo keyInfo = status.getKeyInfo();
        // add trailing slash to represent directory
        String keyName =
            OzoneFSUtils.addTrailingSlashIfNeeded(keyInfo.getKeyName());

        // removeStartKey - as the startKey is a placeholder, which is
        // managed internally to traverse leaf node's sub-paths.
        if (StringUtils.equals(keyName, removeStartKey)) {
          return;
        }

        OzoneKey ozoneKey = new OzoneKey(keyInfo.getVolumeName(),
            keyInfo.getBucketName(), keyName,
            keyInfo.getDataSize(), keyInfo.getCreationTime(),
            keyInfo.getModificationTime(),
            keyInfo.getReplicationConfig(),
            keyInfo.isFile(), keyInfo.getOwnerName());
        keysResultList.add(ozoneKey);
      }
    }

  }

  private class KeyIteratorFactory {
    KeyIterator getKeyIterator(String keyPrefix, String prevKey,
        BucketLayout bType, boolean shallow) throws IOException {
      if (bType.isFileSystemOptimized()) {
        return new KeyIteratorWithFSO(keyPrefix, prevKey, shallow);
      } else {
        return new KeyIterator(keyPrefix, prevKey, shallow);
      }
    }
  }

  public BucketLayout getBucketLayout() {
    return bucketLayout;
  }

  public ReplicationConfig getReplicationConfig() {
    return this.defaultReplication;
  }
}
