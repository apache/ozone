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

import static org.apache.hadoop.ozone.OzoneConsts.QUOTA_RESET;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.WithMetadata;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;

/**
 * A class that encapsulates OzoneVolume.
 */
public class OzoneVolume extends WithMetadata {

  /**
   * The proxy used for connecting to the cluster and perform
   * client operations.
   */
  private final ClientProtocol proxy;

  /**
   * Name of the Volume.
   */
  private final String name;

  /**
   * Admin Name of the Volume.
   */
  private String admin;
  /**
   * Owner of the Volume.
   */
  private String owner;
  /**
   * Quota of bytes allocated for the Volume.
   */
  private long quotaInBytes;
  /**
   * Quota of bucket count allocated for the Volume.
   */
  private long quotaInNamespace;
  /**
   * Bucket namespace quota usage.
   */
  private long usedNamespace;
  /**
   * Creation time of the volume.
   */
  private Instant creationTime;
  /**
   * Modification time of the volume.
   */
  private Instant modificationTime;
  /**
   * Volume ACLs.
   */
  private List<OzoneAcl> acls;

  private int listCacheSize;

  private OzoneObj ozoneObj;
  /**
   * Reference count on this Ozone volume.
   *
   * When reference count is larger than zero, it indicates that at least one
   * "lock" is held on the volume by some Ozone feature (e.g. multi-tenancy).
   * Volume delete operation will be denied in this case, and user should be
   * prompted to release the lock first via the interface provided by that
   * feature.
   *
   * Volumes created using CLI, ObjectStore API or upgraded from older OM DB
   * will have reference count set to zero by default.
   */
  private long refCount;

  protected OzoneVolume(Builder builder) {
    super(builder);
    this.proxy = builder.proxy;
    this.name = builder.name;
    this.admin = builder.admin;
    this.owner = builder.owner;
    this.quotaInBytes = builder.quotaInBytes;
    this.quotaInNamespace = builder.quotaInNamespace;
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
    this.acls = new ArrayList<>(builder.acls);
    if (builder.conf != null) {
      this.listCacheSize = HddsClientUtils.getListCacheSize(builder.conf);
    }
    this.ozoneObj = OzoneObjInfo.Builder.newBuilder()
        .setVolumeName(name)
        .setResType(OzoneObj.ResourceType.VOLUME)
        .setStoreType(OzoneObj.StoreType.OZONE).build();
    this.refCount = builder.refCount;
  }

  /**
   * Returns Volume name.
   *
   * @return volumeName
   */
  public String getName() {
    return name;
  }

  /**
   * Returns Volume's admin name.
   *
   * @return adminName
   */
  public String getAdmin() {
    return admin;
  }

  /**
   * Returns Volume's owner name.
   *
   * @return ownerName
   */
  public String getOwner() {
    return owner;
  }

  /**
   * Returns Quota allocated for the Volume in bytes.
   *
   * @return quotaInBytes
   */
  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  /**
   * Returns quota of bucket counts allocated for the Volume.
   *
   * @return quotaInNamespace
   */
  public long getQuotaInNamespace() {
    return quotaInNamespace;
  }

  /**
   * Returns creation time of the volume.
   *
   * @return creation time.
   */
  public Instant getCreationTime() {
    return creationTime;
  }

  /**
   * Returns modification time of the volume.
   *
   * @return modification time.
   */
  public Instant getModificationTime() {
    return modificationTime;
  }

  /**
   * Returns OzoneAcl list associated with the Volume.
   *
   * @return aclMap
   */
  public List<OzoneAcl> getAcls() {
    return Collections.unmodifiableList(acls);
  }

   /**
   * Adds ACLs to the volume.
   * @param addAcl ACL to be added
   * @return true - if acl is successfully added, false if acl already exists
   * for the bucket.
   * @throws IOException
   */
  public boolean addAcl(OzoneAcl addAcl) throws IOException {
    boolean added = proxy.addAcl(ozoneObj, addAcl);
    if (added) {
      acls.add(addAcl);
    }
    return added;
  }

  /**
   * Remove acl for Ozone object. Return true if acl is removed successfully
   * else false.
   * @param acl Ozone acl to be removed.
   *
   * @throws IOException if there is error.
   * */
  public boolean removeAcl(OzoneAcl acl) throws IOException {
    boolean removed = proxy.removeAcl(ozoneObj, acl);
    if (removed) {
      acls.remove(acl);
    }
    return removed;
  }

  /**
   * Acls to be set for given Ozone object. This operations reset ACL for
   * given object to list of ACLs provided in argument.
   * @param aclList List of acls.
   *
   * @throws IOException if there is error.
   * */
  public boolean setAcl(List<OzoneAcl> aclList) throws IOException {
    boolean reset = proxy.setAcl(ozoneObj, aclList);
    if (reset) {
      acls.clear();
      acls.addAll(aclList);
    }
    return reset;
  }

  /**
   * Returns used bucket namespace.
   * @return usedNamespace
   */
  public long getUsedNamespace() {
    return usedNamespace;
  }

  /**
   * Sets/Changes the owner of this Volume.
   * @param userName new owner
   * @throws IOException
   */
  public boolean setOwner(String userName) throws IOException {
    boolean result = proxy.setVolumeOwner(name, userName);
    this.owner = userName;
    return result;
  }

  /**
   * Clean the space quota of the volume.
   *
   * @throws IOException
   */
  public void clearSpaceQuota() throws IOException {
    OzoneVolume ozoneVolume = proxy.getVolumeDetails(name);
    proxy.setVolumeQuota(name, ozoneVolume.getQuotaInNamespace(), QUOTA_RESET);
    this.quotaInBytes = QUOTA_RESET;
    this.quotaInNamespace = ozoneVolume.getQuotaInNamespace();
  }

  /**
   * Clean the namespace quota of the volume.
   *
   * @throws IOException
   */
  public void clearNamespaceQuota() throws IOException {
    OzoneVolume ozoneVolume = proxy.getVolumeDetails(name);
    proxy.setVolumeQuota(name, QUOTA_RESET, ozoneVolume.getQuotaInBytes());
    this.quotaInBytes = ozoneVolume.getQuotaInBytes();
    this.quotaInNamespace = QUOTA_RESET;
  }

  /**
   * Sets/Changes the quota of this Volume.
   *
   * @param quota OzoneQuota Object that can be applied to storage volume.
   * @throws IOException
   */
  public void setQuota(OzoneQuota quota) throws IOException {
    proxy.setVolumeQuota(name, quota.getQuotaInNamespace(),
        quota.getQuotaInBytes());
    this.quotaInBytes = quota.getQuotaInBytes();
    this.quotaInNamespace = quota.getQuotaInNamespace();
  }

  /**
   * Creates a new Bucket in this Volume, with default values.
   * @param bucketName Name of the Bucket
   * @throws IOException
   */
  public void createBucket(String bucketName)
      throws IOException {
    proxy.createBucket(name, bucketName);
  }

  /**
   * Creates a new Bucket in this Volume, with properties set in bucketArgs.
   * @param bucketName Name of the Bucket
   * @param bucketArgs Properties to be set
   * @throws IOException
   */
  public void createBucket(String bucketName, BucketArgs bucketArgs)
      throws IOException {
    proxy.createBucket(name, bucketName, bucketArgs);
  }

  /**
   * Get the Bucket from this Volume.
   * @param bucketName Name of the Bucket
   * @return OzoneBucket
   * @throws IOException
   */
  public OzoneBucket getBucket(String bucketName) throws IOException {
    OzoneBucket bucket = proxy.getBucketDetails(name, bucketName);
    return bucket;
  }

  /**
   * Returns Iterator to iterate over all buckets in the volume.
   * The result can be restricted using bucket prefix, will return all
   * buckets if bucket prefix is null.
   *
   * @param bucketPrefix Bucket prefix to match
   * @return {@code Iterator<OzoneBucket>}
   */
  public Iterator<? extends OzoneBucket> listBuckets(String bucketPrefix) {
    return listBuckets(bucketPrefix, null);
  }

  /**
   * Returns Iterator to iterate over all buckets after prevBucket in the
   * volume.
   * If prevBucket is null it iterates from the first bucket in the volume.
   * The result can be restricted using bucket prefix, will return all
   * buckets if bucket prefix is null.
   *
   * @param bucketPrefix Bucket prefix to match
   * @param prevBucket Buckets are listed after this bucket
   * @return {@code Iterator<OzoneBucket>}
   */
  public Iterator<? extends OzoneBucket> listBuckets(String bucketPrefix,
      String prevBucket) {
    return listBuckets(bucketPrefix, prevBucket, false);
  }

  /**
   * Returns Iterator to iterate over all buckets after prevBucket in the
   * volume's snapshotted buckets.
   * volume.
   * If prevBucket is null it iterates from the first bucket in the volume.
   * The result can be restricted using bucket prefix, will return all
   * buckets if bucket prefix is null.
   *
   * @param bucketPrefix Bucket prefix to match
   * @param prevBucket   Buckets are listed after this bucket
   * @param hasSnapshot  Set the flag to list the buckets which have snapshot
   * @return {@code Iterator<OzoneBucket>}
   */
  public Iterator<? extends OzoneBucket> listBuckets(String bucketPrefix,
                                                     String prevBucket,
                                                     boolean hasSnapshot) {
    return new BucketIterator(bucketPrefix, prevBucket, hasSnapshot);
  }

  /**
   * Deletes the Bucket from this Volume.
   * @param bucketName Name of the Bucket
   * @throws IOException
   */
  public void deleteBucket(String bucketName) throws IOException {
    proxy.deleteBucket(name, bucketName);
  }

  public long getRefCount() {
    return refCount;
  }

  public static Builder newBuilder(ConfigurationSource conf,
      ClientProtocol proxy) {
    Objects.requireNonNull(proxy, "Client proxy is not set.");
    return new Builder(conf, proxy);
  }

  /**
   * Inner builder for OzoneVolume.
   */
  public static class Builder extends WithMetadata.Builder {
    private ConfigurationSource conf;
    private ClientProtocol proxy;
    private String name;
    private String admin;
    private String owner;
    private long quotaInBytes;
    private long quotaInNamespace;
    private long usedNamespace;
    private long creationTime;
    private long modificationTime;
    private List<OzoneAcl> acls;
    private long refCount;

    protected Builder() {
    }

    private Builder(ConfigurationSource conf, ClientProtocol proxy) {
      this.conf = conf;
      this.proxy = proxy;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setAdmin(String admin) {
      this.admin = admin;
      return this;
    }

    public Builder setOwner(String owner) {
      this.owner = owner;
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

    public Builder setAcls(List<OzoneAcl> acls) {
      this.acls = acls;
      return this;
    }

    public Builder setRefCount(long refCount) {
      this.refCount = refCount;
      return this;
    }

    @Override
    public Builder setMetadata(Map<String, String> metadata) {
      super.setMetadata(metadata);
      return this;
    }

    public OzoneVolume build() {
      return new OzoneVolume(this);
    }
  }

  /**
   * An Iterator to iterate over {@link OzoneBucket} list.
   */
  private class BucketIterator implements Iterator<OzoneBucket> {

    private String bucketPrefix = null;

    private Iterator<OzoneBucket> currentIterator;
    private OzoneBucket currentValue;

    private boolean hasSnapshot;

    /**
     * Creates an Iterator to iterate over all buckets after prevBucket in
     * the volume.
     * If prevBucket is null it iterates from the first bucket in the volume.
     * The returned buckets match bucket prefix.
     *
     * @param bucketPrefix
     * @param hasSnapshot
     */
    BucketIterator(String bucketPrefix, String prevBucket,
                   boolean hasSnapshot) {
      this.bucketPrefix = bucketPrefix;
      this.currentValue = null;
      this.hasSnapshot = hasSnapshot;
      this.currentIterator = getNextListOfBuckets(prevBucket).iterator();
    }

    @Override
    public boolean hasNext() {
      if (!currentIterator.hasNext() && currentValue != null) {
        currentIterator = getNextListOfBuckets(currentValue.getName())
            .iterator();
      }
      return currentIterator.hasNext();
    }

    @Override
    public OzoneBucket next() {
      if (hasNext()) {
        currentValue = currentIterator.next();
        return currentValue;
      }
      throw new NoSuchElementException();
    }

    /**
     * Gets the next set of bucket list using proxy.
     * @param prevBucket
     * @return {@code List<OzoneBucket>}
     */
    private List<OzoneBucket> getNextListOfBuckets(String prevBucket) {
      try {
        return proxy.listBuckets(name, bucketPrefix, prevBucket,
            listCacheSize, hasSnapshot);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
