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

package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;

/**
 * Encapsulates the low level key info.
 */
public class KeyObjectDBInfo extends ObjectDBInfo {
  /** volume name from om db. */
  @JsonProperty("volumeName")
  private String volumeName;
  @JsonProperty("bucketName")
  private String bucketName;
  @JsonProperty("keyName")
  private String keyName;
  @JsonProperty("dataSize")
  private long dataSize;
  @JsonProperty("keyLocationVersions")
  private List<OmKeyLocationInfoGroup> keyLocationVersions;
  @JsonProperty("replicationConfig")
  private ReplicationConfig replicationConfig;
  @JsonProperty("encInfo")
  private FileEncryptionInfo encInfo;

  /**
   * Support OFS use-case to identify if the key is a file or a directory.
   */
  private boolean isFile;

  /**
   * Represents leaf node name. This also will be used when the keyName is
   * created on a FileSystemOptimized(FSO) bucket. For example, the user given
   * keyName is "a/b/key1" then the fileName stores "key1".
   */
  private String fileName;

  public KeyObjectDBInfo() {

  }

  public KeyObjectDBInfo(OmKeyInfo omKeyInfo) {
    super.setName(omKeyInfo.getKeyName());
    super.setCreationTime(omKeyInfo.getCreationTime());
    super.setModificationTime(omKeyInfo.getModificationTime());
    super.setAcls(AclMetadata.fromOzoneAcls(omKeyInfo.getAcls()));
    super.setMetadata(omKeyInfo.getMetadata());
    this.setVolumeName(omKeyInfo.getVolumeName());
    this.setBucketName(omKeyInfo.getBucketName());
    this.setKeyName(omKeyInfo.getKeyName());
    this.setDataSize(omKeyInfo.getDataSize());
    this.setKeyLocationVersions(omKeyInfo.getKeyLocationVersions());
    this.setReplicationConfig(omKeyInfo.getReplicationConfig());
    this.setEncInfo(omKeyInfo.getFileEncryptionInfo());
    this.setFileName(omKeyInfo.getFileName());
    this.setFile(omKeyInfo.isFile());
  }

  public String getVolumeName() {
    return volumeName;
  }

  public void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public String getKeyName() {
    return keyName;
  }

  public void setKeyName(String keyName) {
    this.keyName = keyName;
  }

  public long getDataSize() {
    return dataSize;
  }

  public void setDataSize(long dataSize) {
    this.dataSize = dataSize;
  }

  public List<OmKeyLocationInfoGroup> getKeyLocationVersions() {
    return keyLocationVersions;
  }

  public void setKeyLocationVersions(
      List<OmKeyLocationInfoGroup> keyLocationVersions) {
    this.keyLocationVersions = keyLocationVersions;
  }

  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  public void setReplicationConfig(ReplicationConfig replicationConfig) {
    this.replicationConfig = replicationConfig;
  }

  public boolean isFile() {
    return isFile;
  }

  public void setFile(boolean file) {
    isFile = file;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public FileEncryptionInfo getEncInfo() {
    return encInfo;
  }

  public void setEncInfo(FileEncryptionInfo encInfo) {
    this.encInfo = encInfo;
  }
}
