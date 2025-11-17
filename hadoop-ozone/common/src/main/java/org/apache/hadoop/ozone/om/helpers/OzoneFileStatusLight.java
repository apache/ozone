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

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneFileStatusProtoLight;

/**
 * Lightweight OzoneFileStatus class.
 */
public class OzoneFileStatusLight {

  private BasicOmKeyInfo keyInfo;

  private boolean isDirectory;

  private long blockSize;

  public OzoneFileStatusLight() {
    isDirectory = true;
  }

  public OzoneFileStatusLight(BasicOmKeyInfo keyInfo,
                              long blockSize, boolean isDirectory) {
    this.keyInfo = keyInfo;
    this.isDirectory = isDirectory;
    this.blockSize = blockSize;
  }

  public BasicOmKeyInfo getKeyInfo() {
    return keyInfo;
  }

  public long getBlockSize() {
    return blockSize;
  }

  public String getTrimmedName() {
    String keyName = keyInfo.getKeyName();
    if (keyName.endsWith(OZONE_URI_DELIMITER)) {
      return keyName.substring(0, keyName.length() - 1);
    } else {
      return keyName;
    }
  }

  public String getPath() {
    if (keyInfo == null) {
      return OZONE_URI_DELIMITER;
    } else {
      String path = OZONE_URI_DELIMITER + keyInfo.getKeyName();
      if (path.endsWith(OZONE_URI_DELIMITER)) {
        return path.substring(0, path.length() - 1);
      } else {
        return path;
      }
    }
  }

  public boolean isDirectory() {
    if (keyInfo == null) {
      return true;
    }
    return isDirectory;
  }

  public boolean isFile() {
    return !isDirectory();
  }

  public OzoneFileStatusProtoLight getProtobuf() {
    OzoneFileStatusProtoLight.Builder builder =
        OzoneFileStatusProtoLight.newBuilder()
            .setBlockSize(blockSize)
            .setIsDirectory(isDirectory);
    //key info can be null for the fake root entry.
    if (keyInfo != null) {
      builder.setBasicKeyInfo(keyInfo.getProtobuf())
          .setVolumeName(keyInfo.getVolumeName())
          .setBucketName(keyInfo.getBucketName());
    }
    return builder.build();
  }

  public static OzoneFileStatusLight getFromProtobuf(
      OzoneFileStatusProtoLight status) throws IOException {
    return new OzoneFileStatusLight(
        BasicOmKeyInfo.getFromProtobuf(status.getVolumeName(),
            status.getBucketName(), status.getBasicKeyInfo()),
        status.getBlockSize(),
        status.getIsDirectory());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OzoneFileStatusLight)) {
      return false;
    }
    OzoneFileStatusLight that = (OzoneFileStatusLight) o;
    return isDirectory == that.isDirectory &&
        blockSize == that.blockSize &&
        getTrimmedName().equals(that.getTrimmedName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTrimmedName());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append('{');
    if (keyInfo == null) {
      sb.append("<root>");
    } else {
      sb.append(getTrimmedName());
      if (isDirectory) {
        sb.append(" (dir)");
      }
    }
    sb.append('}');
    return sb.toString();
  }

  public static OzoneFileStatusLight fromOzoneFileStatus(
      OzoneFileStatus ozoneFileStatus) {
    BasicOmKeyInfo basicKeyInfo =
        BasicOmKeyInfo.fromOmKeyInfo(ozoneFileStatus.getKeyInfo());
    return new OzoneFileStatusLight(basicKeyInfo,
        ozoneFileStatus.getBlockSize(), ozoneFileStatus.isDirectory());
  }
}
