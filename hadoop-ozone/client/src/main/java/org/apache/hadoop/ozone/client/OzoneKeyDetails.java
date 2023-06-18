/**
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

package org.apache.hadoop.ozone.client;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.ratis.util.function.CheckedSupplier;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A class that encapsulates OzoneKeyLocation.
 */
public class OzoneKeyDetails extends OzoneKey {

  /**
   * A list of block location information to specify replica locations.
   */
  private final List<OzoneKeyLocation> ozoneKeyLocations;

  private final FileEncryptionInfo feInfo;

  private final CheckedSupplier<OzoneInputStream, IOException> contentSupplier;

  /**
   * Constructs OzoneKeyDetails from OmKeyInfo.
   */
  @SuppressWarnings("parameternumber")
  @Deprecated
  public OzoneKeyDetails(String volumeName, String bucketName, String keyName,
                         long size, long creationTime, long modificationTime,
                         List<OzoneKeyLocation> ozoneKeyLocations,
                         ReplicationType type, Map<String, String> metadata,
                         FileEncryptionInfo feInfo, int replicationFactor) {
    super(volumeName, bucketName, keyName, size, creationTime,
        modificationTime, type, replicationFactor);
    this.ozoneKeyLocations = ozoneKeyLocations;
    this.feInfo = feInfo;
    contentSupplier = null;
    this.setMetadata(metadata);
  }


  /**
   * Constructs OzoneKeyDetails from OmKeyInfo.
   */
  @SuppressWarnings("parameternumber")
  public OzoneKeyDetails(String volumeName, String bucketName, String keyName,
      long size, long creationTime, long modificationTime,
      List<OzoneKeyLocation> ozoneKeyLocations,
      ReplicationConfig replicationConfig,
      Map<String, String> metadata,
      FileEncryptionInfo feInfo,
      CheckedSupplier<OzoneInputStream, IOException> contentSupplier) {
    super(volumeName, bucketName, keyName, size, creationTime,
            modificationTime, replicationConfig, metadata);
    this.ozoneKeyLocations = ozoneKeyLocations;
    this.feInfo = feInfo;
    this.contentSupplier = contentSupplier;
  }

  /**
   * Returns the location detail information of the specific Key.
   */
  public List<OzoneKeyLocation> getOzoneKeyLocations() {
    return ozoneKeyLocations;
  }

  public FileEncryptionInfo getFileEncryptionInfo() {
    return feInfo;
  }

  /**
   * Get OzoneInputStream to read the content of the key.
   */
  @JsonIgnore
  public OzoneInputStream getContent() throws IOException {
    return this.contentSupplier.get();
  }
}
