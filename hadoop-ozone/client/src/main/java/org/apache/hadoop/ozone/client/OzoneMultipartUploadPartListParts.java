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

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;

/**
 * Class that represents Multipart upload List parts response.
 */
public class OzoneMultipartUploadPartListParts {

  private ReplicationConfig replicationConfig;

  //When a list is truncated, this element specifies the last part in the list,
  // as well as the value to use for the part-number-marker request parameter
  // in a subsequent request.
  private int nextPartNumberMarker;
  // Indicates whether the returned list of parts is truncated. A true value
  // indicates that the list was truncated.
  // A list can be truncated if the number of parts exceeds the limit
  // returned in the MaxParts element.
  private boolean truncated;
  private List<PartInfo> partInfoList = new ArrayList<>();

  @Deprecated
  public OzoneMultipartUploadPartListParts(ReplicationType type,
      ReplicationFactor factor,
      int nextMarker, boolean truncate) {
    this.nextPartNumberMarker = nextMarker;
    this.truncated = truncate;
    this.replicationConfig = ReplicationConfig.fromTypeAndFactor(type, factor);
  }

  public OzoneMultipartUploadPartListParts(ReplicationConfig replicationConfig,
                                           int nextMarker, boolean truncate) {
    this.nextPartNumberMarker = nextMarker;
    this.truncated = truncate;
    this.replicationConfig = replicationConfig;
  }

  public void addAllParts(List<PartInfo> partInfos) {
    partInfoList.addAll(partInfos);
  }

  public void addPart(PartInfo partInfo) {
    this.partInfoList.add(partInfo);
  }

  public int getNextPartNumberMarker() {
    return nextPartNumberMarker;
  }

  public boolean isTruncated() {
    return truncated;
  }

  public List<PartInfo> getPartInfoList() {
    return partInfoList;
  }

  @Deprecated
  public ReplicationType getReplicationType() {
    return ReplicationType
            .fromProto(replicationConfig.getReplicationType());
  }

  @Deprecated
  public ReplicationFactor getReplicationFactor() {
    return ReplicationFactor
            .fromProto(ReplicationConfig.getLegacyFactor(replicationConfig));
  }

  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  /**
   * Class that represents each Part information of a multipart upload part.
   */
  public static final class PartInfo {

    private final int partNumber;
    private final String partName;
    private final long modificationTime;
    private final long size;
    private final String eTag;

    public PartInfo(int number, String name, long time, long size,
                    String eTag) {
      this.partNumber = number;
      this.partName = name;
      this.modificationTime = time;
      this.size = size;
      this.eTag = eTag;
    }

    public int getPartNumber() {
      return partNumber;
    }

    public String getPartName() {
      return partName;
    }

    public long getModificationTime() {
      return modificationTime;
    }

    public long getSize() {
      return size;
    }

    public String getETag() {
      return eTag;
    }
  }
}
