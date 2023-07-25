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
package org.apache.hadoop.ozone.om.helpers;

import java.io.IOException;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BasicKeyInfo;

/**
 * Lightweight OmKeyInfo class.
 */
public class BasicOmKeyInfo {

  private String volumeName;
  private String bucketName;
  private String keyName;
  private long dataSize;
  private long creationTime;
  private long modificationTime;
  private ReplicationConfig replicationConfig;
  private boolean isFile;

  @SuppressWarnings("parameternumber")
  public BasicOmKeyInfo(String volumeName, String bucketName, String keyName,
                        long dataSize, long creationTime, long modificationTime,
                        ReplicationConfig replicationConfig, boolean isFile) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.dataSize = dataSize;
    this.creationTime = creationTime;
    this.modificationTime = modificationTime;
    this.replicationConfig = replicationConfig;
    this.isFile = isFile;
  }

  public static BasicOmKeyInfo getFromProtobuf(BasicKeyInfo basicKeyInfo)
          throws IOException {
    if (basicKeyInfo == null) {
      return null;
    }

    String volName = basicKeyInfo.getVolumeName();
    String buckName = basicKeyInfo.getBucketName();
    String keyName = basicKeyInfo.getKeyName();
    long dataSize = basicKeyInfo.getDataSize();
    long creationTime = basicKeyInfo.getCreationTime();
    long modificationTime = basicKeyInfo.getModificationTime();
    ReplicationConfig replicationConfig = ReplicationConfig.fromProto(
            basicKeyInfo.getType(),
            basicKeyInfo.getFactor(),
            basicKeyInfo.getEcReplicationConfig()
    );
    boolean isFile = basicKeyInfo.getIsFile();

    return new BasicOmKeyInfo(volName, buckName, keyName, dataSize,
            creationTime, modificationTime, replicationConfig, isFile);
  }
}
