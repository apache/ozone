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

import java.io.IOException;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;

/**
 * This class is used exclusively in FSO buckets..
 */
public final class OmFSOFile {

  private String volumeName;
  private String bucketName;
  private String keyName;
  private OMMetadataManager omMetadataManager;

  private String fileName;
  private long volumeId;
  private long bucketId;
  private long parentID;

  @SuppressWarnings("checkstyle:parameternumber")
  private OmFSOFile(String volumeName, String bucketName, String keyName, 
      OMMetadataManager omMetadataManager, String fileName,
      long volumeId, long bucketId, long parentID) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.omMetadataManager = omMetadataManager;

    this.fileName = fileName;
    this.volumeId = volumeId;
    this.bucketId = bucketId;
    this.parentID = parentID;
  }

  /**
   * Builder class for OmFSOFile.
   */
  public static class Builder {
    private String volumeName;
    private String bucketName;
    private String keyName;
    private OMMetadataManager omMetadataManager;
    private String errMsg;

    public Builder() {
      this.errMsg = null;
    }

    public Builder setVolumeName(String volumeName) {
      this.volumeName = volumeName;
      return this;
    }

    public Builder setBucketName(String bucketName) {
      this.bucketName = bucketName;
      return this;
    }

    public Builder setKeyName(String keyName) {
      this.keyName = keyName;
      return this;
    }

    public Builder setOmMetadataManager(OMMetadataManager omMetadataManager) {
      this.omMetadataManager = omMetadataManager;
      return this;
    }

    public Builder setErrMsg(String errMsg) {
      this.errMsg = errMsg;
      return this;
    }

    public OmFSOFile build() throws IOException {
      String fileName = OzoneFSUtils.getFileName(this.keyName);
      final long volumeId = omMetadataManager.getVolumeId(this.volumeName);
      final long bucketId = omMetadataManager.getBucketId(this.volumeName, this.bucketName);
      long parentID = OMFileRequest
          .getParentID(volumeId, bucketId, this.keyName,
          this.omMetadataManager, this.errMsg);

      return new OmFSOFile(volumeName, bucketName, keyName, 
          omMetadataManager, fileName, volumeId, bucketId, parentID);
    }
  }

  public String getVolumeName() {
    return this.volumeName;
  }

  public String getBucketName() {
    return this.bucketName;
  }

  public String getKeyName() {
    return this.keyName;
  }

  public OMMetadataManager getOmMetadataManager() {
    return this.omMetadataManager;
  }

  public String getFileName() {
    return this.fileName;
  }

  public long getVolumeId() {
    return this.volumeId;
  }

  public long getBucketId() {
    return this.bucketId;
  }

  public long getParentID() {
    return this.parentID;
  }

  public String getOpenFileName(long clientID) {
    return omMetadataManager.getOpenFileName(this.volumeId, this.bucketId,
        this.parentID, this.fileName, clientID);
  }

  public String getOzonePathKey() {
    return omMetadataManager.getOzonePathKey(this.volumeId, this.bucketId,
        this.parentID, this.fileName);
  }
}
