/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * This class help to get metadata keys.
 */
public final class OmGetKey {

  private String volumeName;
  private String bucketName;
  private String keyName;
  private OMMetadataManager omMetadataManager;
  private long clientID;
  private String errMsg;

  private String fileName;
  private Iterator<Path> pathComponents;
  private long volumeId;
  private long bucketId;
  private long parentID;


  @SuppressWarnings("checkstyle:parameternumber")
  private OmGetKey(String volumeName, String bucketName, String keyName, 
      OMMetadataManager omMetadataManager, long clientID, String errMsg, 
      String fileName, Iterator<Path> pathComponents, long volumeId,
      long bucketId, long parentID) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.omMetadataManager = omMetadataManager;
    this.clientID = clientID;
    this.errMsg = errMsg;

    this.fileName = fileName;
    this.pathComponents = pathComponents;
    this.volumeId = volumeId;
    this.bucketId = bucketId;
    this.parentID = parentID;
  }

  /**
   * Builder class for OmGetKey.
   */
  public static class Builder {
    private String volumeName;
    private String bucketName;
    private String keyName;
    private OMMetadataManager omMetadataManager;
    private long clientID;
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

    public Builder setClientID(long clientID) {
      this.clientID = clientID;
      return this;
    }

    public Builder setErrMsg(String errMsg) {
      this.errMsg = errMsg;
      return this;
    }

    public OmGetKey build() throws IOException {
      String fileName = OzoneFSUtils.getFileName(this.keyName);
      Iterator<Path> pathComponents = Paths.get(this.keyName).iterator();
      final long volumeId = omMetadataManager.getVolumeId(this.volumeName);
      final long bucketId = omMetadataManager.getBucketId(this.volumeName,
          this.bucketName);
      long parentID = OMFileRequest
          .getParentID(volumeId, bucketId, pathComponents, this.keyName,
          this.omMetadataManager, this.errMsg);

      return new OmGetKey(volumeName, bucketName, keyName, 
          omMetadataManager, clientID, errMsg, fileName, pathComponents, 
          volumeId, bucketId, parentID);
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

  public long getClientID() {
    return this.clientID;
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

  public String getOpenKey() {
    return omMetadataManager.getOpenFileName(this.volumeId, this.bucketId,
        this.parentID, this.fileName, this.clientID);
  }

  public String getOzonePathKey() {
    return omMetadataManager.getOzonePathKey(this.volumeId, this.bucketId,
        this.parentID, this.fileName);
  }
}
