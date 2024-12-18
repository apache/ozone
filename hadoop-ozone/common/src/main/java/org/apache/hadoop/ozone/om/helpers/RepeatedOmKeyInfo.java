/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RepeatedKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyInfo;

/**
 * Args for deleted keys. This is written to om metadata deletedTable.
 * Once a key is deleted, it is moved to om metadata deletedTable. Having a
 * {label: List<OMKeyInfo>} ensures that if users create & delete keys with
 * exact same uri multiple times, all the delete instances are bundled under
 * the same key name. This is useful as part of GDPR compliance where an
 * admin wants to confirm if a given key is deleted from deletedTable metadata.
 */
public class RepeatedOmKeyInfo implements CopyObject<RepeatedOmKeyInfo> {
  private static final Codec<RepeatedOmKeyInfo> CODEC_TRUE = newCodec(true);
  private static final Codec<RepeatedOmKeyInfo> CODEC_FALSE = newCodec(false);

  private static Codec<RepeatedOmKeyInfo> newCodec(boolean ignorePipeline) {
    return new DelegatedCodec<>(
        Proto2Codec.get(RepeatedKeyInfo.getDefaultInstance()),
        RepeatedOmKeyInfo::getFromProto,
        k -> k.getProto(ignorePipeline, ClientVersion.CURRENT_VERSION));
  }

  public static Codec<RepeatedOmKeyInfo> getCodec(boolean ignorePipeline) {
    return ignorePipeline ? CODEC_TRUE : CODEC_FALSE;
  }

  private final List<OmKeyInfo> omKeyInfoList;

  public RepeatedOmKeyInfo() {
    this.omKeyInfoList = new ArrayList<>();
  }

  public RepeatedOmKeyInfo(List<OmKeyInfo> omKeyInfos) {
    this.omKeyInfoList = omKeyInfos;
  }

  public RepeatedOmKeyInfo(OmKeyInfo omKeyInfos) {
    this.omKeyInfoList = new ArrayList<>();
    this.omKeyInfoList.add(omKeyInfos);
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
    return new ImmutablePair<Long, Long>(unreplicatedSize, replicatedSize);
  }


  // HDDS-7041. Return a new ArrayList to avoid ConcurrentModifyException
  public List<OmKeyInfo> cloneOmKeyInfoList() {
    return new ArrayList<>(omKeyInfoList);
  }

  public static RepeatedOmKeyInfo getFromProto(RepeatedKeyInfo
      repeatedKeyInfo) throws IOException {
    List<OmKeyInfo> list = new ArrayList<>();
    for (KeyInfo k : repeatedKeyInfo.getKeyInfoList()) {
      list.add(OmKeyInfo.getFromProtobuf(k));
    }
    return new RepeatedOmKeyInfo.Builder().setOmKeyInfos(list).build();
  }

  /**
   *
   * @param compact, true for persistence, false for network transmit
   * @return
   */
  public RepeatedKeyInfo getProto(boolean compact, int clientVersion) {
    List<KeyInfo> list = new ArrayList<>();
    for (OmKeyInfo k : cloneOmKeyInfoList()) {
      list.add(k.getProtobuf(compact, clientVersion));
    }

    RepeatedKeyInfo.Builder builder = RepeatedKeyInfo.newBuilder()
        .addAllKeyInfo(list);
    return builder.build();
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

    public Builder() { }

    public Builder setOmKeyInfos(List<OmKeyInfo> infoList) {
      this.omKeyInfos = infoList;
      return this;
    }

    public RepeatedOmKeyInfo build() {
      return new RepeatedOmKeyInfo(omKeyInfos);
    }
  }

  @Override
  public RepeatedOmKeyInfo copyObject() {
    return new RepeatedOmKeyInfo(new ArrayList<>(omKeyInfoList));
  }
}
