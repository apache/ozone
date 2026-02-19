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

package org.apache.hadoop.ozone.om.helpers;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RepeatedKeyInfo;

/**
 * Args for deleted keys. This is written to om metadata deletedTable.
 * Once a key is deleted, it is moved to om metadata deletedTable. Having a
 * label: {@code List<OMKeyInfo>} ensures that if users create and delete keys with
 * exact same uri multiple times, all the delete instances are bundled under
 * the same key name. This is useful as part of GDPR compliance where an
 * admin wants to confirm if a given key is deleted from deletedTable metadata.
 */
public class RepeatedOmKeyInfo implements CopyObject<RepeatedOmKeyInfo> {
  private static final Codec<RepeatedOmKeyInfo> CODEC_TRUE = newCodec(true);
  private static final Codec<RepeatedOmKeyInfo> CODEC_FALSE = newCodec(false);

  private final List<OmKeyInfo> omKeyInfoList;
  /**
   * Represents the unique identifier for a bucket. This variable is used to
   * distinguish between different instances of a bucket, even if a bucket
   * with the same name is deleted and recreated.
   *
   * It is particularly useful for tracking and updating the quota usage
   * associated with a bucket.
   */
  private final long bucketId;

  private static Codec<RepeatedOmKeyInfo> newCodec(boolean ignorePipeline) {
    return new DelegatedCodec<>(
        Proto2Codec.get(RepeatedKeyInfo.getDefaultInstance()),
        RepeatedOmKeyInfo::getFromProto,
        k -> k.getProto(ignorePipeline, ClientVersion.CURRENT_VERSION),
        RepeatedOmKeyInfo.class);
  }

  public static Codec<RepeatedOmKeyInfo> getCodec(boolean ignorePipeline) {
    return ignorePipeline ? CODEC_TRUE : CODEC_FALSE;
  }

  public RepeatedOmKeyInfo(long bucketId) {
    this.omKeyInfoList = new ArrayList<>();
    this.bucketId = bucketId;
  }

  public RepeatedOmKeyInfo(List<OmKeyInfo> omKeyInfos, long bucketId) {
    this.omKeyInfoList = omKeyInfos;
    this.bucketId = bucketId;
  }

  public RepeatedOmKeyInfo(OmKeyInfo omKeyInfos, long bucketId) {
    this.omKeyInfoList = new ArrayList<>();
    this.omKeyInfoList.add(omKeyInfos);
    this.bucketId = bucketId;
  }

  public void addOmKeyInfo(OmKeyInfo info) {
    this.omKeyInfoList.add(info);
  }

  public List<OmKeyInfo> getOmKeyInfoList() {
    return omKeyInfoList;
  }

  /**
   * Returns a pair of long values representing the replicated size and
   * unreplicated size of all the keys in the list.
   */
  public ImmutablePair<Long, Long> getTotalSize() {
    long replicatedSize = 0;
    long unreplicatedSize = 0;

    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      if (omKeyInfo.getReplicatedSize() != 0) {
        replicatedSize += omKeyInfo.getReplicatedSize();
      }
      unreplicatedSize += omKeyInfo.getDataSize();
    }
    return new ImmutablePair<>(unreplicatedSize, replicatedSize);
  }

  // HDDS-7041. Return a new ArrayList to avoid ConcurrentModifyException
  public List<OmKeyInfo> cloneOmKeyInfoList() {
    return new ArrayList<>(omKeyInfoList);
  }

  public static Builder builderFromProto(RepeatedKeyInfo repeatedKeyInfo) {
    List<OmKeyInfo> list = new ArrayList<>();
    for (KeyInfo k : repeatedKeyInfo.getKeyInfoList()) {
      list.add(OmKeyInfo.getFromProtobuf(k));
    }
    RepeatedOmKeyInfo.Builder builder = new RepeatedOmKeyInfo.Builder().setOmKeyInfos(list);
    if (repeatedKeyInfo.hasBucketId()) {
      builder.setBucketId(repeatedKeyInfo.getBucketId());
    }
    return builder;
  }

  public static RepeatedOmKeyInfo getFromProto(RepeatedKeyInfo repeatedKeyInfo) {
    return builderFromProto(repeatedKeyInfo).build();
  }

  /**
   * @param compact true for persistence, false for network transmit
   */
  public RepeatedKeyInfo getProto(boolean compact, int clientVersion) {
    List<KeyInfo> list = new ArrayList<>();
    for (OmKeyInfo k : cloneOmKeyInfoList()) {
      list.add(k.getProtobuf(compact, clientVersion));
    }

    RepeatedKeyInfo.Builder builder = RepeatedKeyInfo.newBuilder()
        .addAllKeyInfo(list).setBucketId(bucketId);
    return builder.build();
  }

  public long getBucketId() {
    return bucketId;
  }

  @Override
  public String toString() {
    return "RepeatedOmKeyInfo{" +
        "omKeyInfoList=" + omKeyInfoList +
        '}';
  }

  /**
   * Builder of RepeatedOmKeyInfo.
   */
  public static class Builder {
    private List<OmKeyInfo> omKeyInfos;
    private long bucketId;

    public Builder() { }

    public Builder setOmKeyInfos(List<OmKeyInfo> infoList) {
      this.omKeyInfos = infoList;
      return this;
    }

    public Builder setBucketId(long bucketId) {
      this.bucketId = bucketId;
      return this;
    }

    public RepeatedOmKeyInfo build() {
      return new RepeatedOmKeyInfo(omKeyInfos, bucketId);
    }
  }

  @Override
  public RepeatedOmKeyInfo copyObject() {
    return new RepeatedOmKeyInfo(new ArrayList<>(omKeyInfoList), bucketId);
  }
}
