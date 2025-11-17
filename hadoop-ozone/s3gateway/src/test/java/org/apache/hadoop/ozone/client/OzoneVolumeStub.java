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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.util.Time;

/**
 * Ozone volume with in-memory state for testing.
 */
public final class OzoneVolumeStub extends OzoneVolume {

  private final Map<String, OzoneBucket> buckets = new HashMap<>();

  private final ArrayList<OzoneAcl> aclList = new ArrayList<>();

  public static Builder newBuilder() {
    return new Builder();
  }

  private OzoneVolumeStub(Builder b) {
    super(b);
  }

  /**
   * Inner builder for OzoneVolumeStub.
   */
  public static final class Builder extends OzoneVolume.Builder {

    private Builder() {
    }

    @Override
    public Builder setName(String name) {
      super.setName(name);
      return this;
    }

    @Override
    public Builder setAdmin(String admin) {
      super.setAdmin(admin);
      return this;
    }

    @Override
    public Builder setOwner(String owner) {
      super.setOwner(owner);
      return this;
    }

    @Override
    public Builder setQuotaInBytes(long quotaInBytes) {
      super.setQuotaInBytes(quotaInBytes);
      return this;
    }

    @Override
    public Builder setQuotaInNamespace(long quotaInNamespace) {
      super.setQuotaInNamespace(quotaInNamespace);
      return this;
    }

    @Override
    public Builder setCreationTime(long creationTime) {
      super.setCreationTime(creationTime);
      return this;
    }

    @Override
    public Builder setAcls(List<OzoneAcl> acls) {
      super.setAcls(acls);
      return this;
    }

    @Override
    public OzoneVolumeStub build() {
      return new OzoneVolumeStub(this);
    }
  }

  @Override
  public void createBucket(String bucketName) {
    createBucket(bucketName, new BucketArgs.Builder()
        .setStorageType(StorageType.DEFAULT)
        .setVersioning(false)
        .build());
  }

  @Override
  public void createBucket(String bucketName, BucketArgs bucketArgs) {
    buckets.put(bucketName, OzoneBucketStub.newBuilder()
        .setVolumeName(getName())
        .setName(bucketName)
        .setDefaultReplicationConfig(new DefaultReplicationConfig(
            RatisReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.THREE)))
        .setBucketLayout(bucketArgs.getBucketLayout())
        .setStorageType(bucketArgs.getStorageType())
        .setVersioning(bucketArgs.getVersioning())
        .setCreationTime(Time.now())
        .build());
  }

  @Override
  public OzoneBucket getBucket(String bucketName) throws IOException {
    if (buckets.containsKey(bucketName)) {
      return buckets.get(bucketName);
    } else {
      throw new OMException("", OMException.ResultCodes.BUCKET_NOT_FOUND);
    }

  }

  @Override
  public Iterator<? extends OzoneBucket> listBuckets(String bucketPrefix) {
    return buckets.values()
        .stream()
        .filter(bucket -> {
          if (bucketPrefix != null) {
            return bucket.getName().startsWith(bucketPrefix);
          } else {
            return true;
          }
        })
        .collect(Collectors.toList())
        .iterator();
  }

  @Override
  public Iterator<? extends OzoneBucket> listBuckets(String bucketPrefix,
      String prevBucket) {
    return buckets.values()
        .stream()
        .filter(bucket -> bucket.getName().compareTo(prevBucket) > 0)
        .filter(bucket -> bucket.getName().startsWith(bucketPrefix))
        .collect(Collectors.toList())
        .iterator();
  }

  @Override
  public void deleteBucket(String bucketName) throws IOException {
    if (buckets.containsKey(bucketName)) {
      buckets.remove(bucketName);
    } else {
      throw new OMException("", OMException.ResultCodes.BUCKET_NOT_FOUND);
    }
  }

  @Override
  public List<OzoneAcl> getAcls() {
    return (List<OzoneAcl>)aclList.clone();
  }

  @Override
  public boolean addAcl(OzoneAcl addAcl) throws IOException {
    return aclList.add(addAcl);
  }

  @Override
  public boolean removeAcl(OzoneAcl acl) throws IOException {
    return aclList.remove(acl);
  }

  @Override
  public boolean setAcl(List<OzoneAcl> acls) throws IOException {
    aclList.clear();
    return aclList.addAll(acls);
  }
}
