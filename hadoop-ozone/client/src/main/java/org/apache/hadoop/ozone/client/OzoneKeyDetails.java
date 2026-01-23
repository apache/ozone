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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.ratis.util.function.CheckedSupplier;

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
   * The generation of an existing key. This can be used with atomic commits, to
   * ensure the key has not changed since the key details were read.
   */
  private final Long generation;

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
      CheckedSupplier<OzoneInputStream, IOException> contentSupplier,
      boolean isFile, String owner, Map<String, String> tags, Long generation) {
    super(volumeName, bucketName, keyName, size, creationTime,
        modificationTime, replicationConfig, metadata, isFile, owner, tags);
    this.ozoneKeyLocations = ozoneKeyLocations;
    this.feInfo = feInfo;
    this.contentSupplier = contentSupplier;
    this.generation = generation;
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
                         CheckedSupplier<OzoneInputStream, IOException> contentSupplier,
                         boolean isFile, String owner, Map<String, String> tags) {
    this(volumeName, bucketName, keyName, size, creationTime,
        modificationTime, ozoneKeyLocations, replicationConfig, metadata, feInfo, contentSupplier,
        isFile, owner, tags, null);
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

  public Long getGeneration() {
    return generation;
  }

  /**
   * Get OzoneInputStream to read the content of the key.
   * @return OzoneInputStream
   * @throws IOException
   */
  @JsonIgnore
  public OzoneInputStream getContent() throws IOException {
    return this.contentSupplier.get();
  }
}
