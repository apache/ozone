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

import com.google.common.base.Objects;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocationList;

/**
 * A list of key locations. This class represents one single version of the
 * blocks of a key.
 */
public class OmKeyLocationInfoGroup {
  private final long version;
  // TODO: HDDS-5472 Store one version of locationInfo for each
  //   OmKeyLocationInfoGroup
  private final Map<Long, List<OmKeyLocationInfo>> locationVersionMap;
  private  boolean isMultipartKey;

  public OmKeyLocationInfoGroup(long version,
                                List<OmKeyLocationInfo> locations) {
    this(version, locations, false);
  }

  public OmKeyLocationInfoGroup(long version,
      List<OmKeyLocationInfo> locations, boolean isMultipartKey) {
    this.version = version;
    locationVersionMap = new HashMap<>();
    for (OmKeyLocationInfo info : locations) {
      locationVersionMap
          .computeIfAbsent(info.getCreateVersion(), v -> new ArrayList<>())
          .add(info);
    }
    //prevent NPE
    this.locationVersionMap.putIfAbsent(version, new ArrayList<>());
    this.isMultipartKey = isMultipartKey;

  }

  public OmKeyLocationInfoGroup(long version,
                                Map<Long, List<OmKeyLocationInfo>> locations) {
    this(version, locations, false);
  }

  public OmKeyLocationInfoGroup(long version,
      Map<Long, List<OmKeyLocationInfo>> locations, boolean isMultipartKey) {
    this.version = version;
    this.locationVersionMap = locations;
    //prevent NPE
    this.locationVersionMap.putIfAbsent(version, new ArrayList<>());
    this.isMultipartKey = isMultipartKey;
  }

  public void setMultipartKey(boolean isMpu) {
    this.isMultipartKey = isMpu;
  }

  public boolean isMultipartKey() {
    return isMultipartKey;
  }

  /**
   * @return Raw internal locationVersionMap.
   */
  public Map<Long, List<OmKeyLocationInfo>> getLocationVersionMap() {
    return locationVersionMap;
  }

  /**
   * Return only the blocks that are created in the most recent version.
   *
   * @return the list of blocks that are created in the latest version.
   */
  public List<OmKeyLocationInfo> getBlocksLatestVersionOnly() {
    return new ArrayList<>(locationVersionMap.get(version));
  }

  public long getVersion() {
    return version;
  }

  /**
   * Use this expensive method only when absolutely needed!
   * It creates a new list so it is not an O(1) operation.
   * Use getLocationLists() instead.
   * @return a list of OmKeyLocationInfo
   */
  public List<OmKeyLocationInfo> getLocationList() {
    return locationVersionMap.values().stream().flatMap(List::stream)
        .collect(Collectors.toList());
  }

  public Collection<List<OmKeyLocationInfo>> getLocationLists() {
    return locationVersionMap.values();
  }

  public long getLocationListCount() {
    return locationVersionMap.values().stream().mapToLong(List::size).sum();
  }

  @Deprecated
  public List<OmKeyLocationInfo> getLocationList(Long versionToFetch) {
    return new ArrayList<>(locationVersionMap.get(versionToFetch));
  }

  public KeyLocationList getProtobuf(boolean ignorePipeline,
      int clientVersion) {
    KeyLocationList.Builder builder = KeyLocationList.newBuilder()
        .setVersion(version).setIsMultipartKey(isMultipartKey);
    List<OzoneManagerProtocolProtos.KeyLocation> keyLocationList =
        new ArrayList<>();
    for (List<OmKeyLocationInfo> locationList : locationVersionMap.values()) {
      for (OmKeyLocationInfo keyInfo : locationList) {
        keyLocationList.add(keyInfo.getProtobuf(ignorePipeline, clientVersion));
      }
    }
    return  builder.addAllKeyLocations(keyLocationList).build();
  }

  public static OmKeyLocationInfoGroup getFromProtobuf(
      KeyLocationList keyLocationList) {
    return new OmKeyLocationInfoGroup(
        keyLocationList.getVersion(),
        keyLocationList.getKeyLocationsList().stream()
            .map(OmKeyLocationInfo::getFromProtobuf)
            .collect(Collectors.groupingBy(
                OmKeyLocationInfo::getCreateVersion)),
        keyLocationList.getIsMultipartKey()
    );
  }

  /**
   * Given a new block location, generate a new version list based upon this
   * one.
   *
   * @param newLocationList a list of new location to be added.
   * @return newly generated OmKeyLocationInfoGroup
   */
  OmKeyLocationInfoGroup generateNextVersion(
      List<OmKeyLocationInfo> newLocationList) {
    Map<Long, List<OmKeyLocationInfo>> newMap = new HashMap<>();
    newMap.put(version + 1, new ArrayList<>(newLocationList));
    return new OmKeyLocationInfoGroup(version + 1, newMap);
  }

  void appendNewBlocks(List<OmKeyLocationInfo> newLocationList) {
    List<OmKeyLocationInfo> locationList = locationVersionMap.get(version);
    for (OmKeyLocationInfo info : newLocationList) {
      info.setCreateVersion(version);
      locationList.add(info);
    }
  }

  void removeBlocks(long versionToRemove) {
    locationVersionMap.remove(versionToRemove);
  }

  void addAll(long versionToAdd, List<OmKeyLocationInfo> locationInfoList) {
    locationVersionMap.putIfAbsent(versionToAdd, new ArrayList<>());
    List<OmKeyLocationInfo> list = locationVersionMap.get(versionToAdd);
    list.addAll(locationInfoList);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("version:").append(version).append(' ');
    sb.append("isMultipartKey:").append(isMultipartKey).append(' ');
    for (List<OmKeyLocationInfo> kliList : locationVersionMap.values()) {
      for (OmKeyLocationInfo kli: kliList) {
        sb.append("conID ").append(kli.getContainerID());
        sb.append(' ');
        sb.append("locID ").append(kli.getLocalID());
        sb.append(' ');
        sb.append("bcsID ").append(kli.getBlockCommitSequenceId());
        sb.append(" || ");
      }
    }
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OmKeyLocationInfoGroup that = (OmKeyLocationInfoGroup) o;
    return version == that.version && isMultipartKey == that.isMultipartKey
        && Objects.equal(locationVersionMap, that.locationVersionMap);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(version, locationVersionMap, isMultipartKey);
  }
}
