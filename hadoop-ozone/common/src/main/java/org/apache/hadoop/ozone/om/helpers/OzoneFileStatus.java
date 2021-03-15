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

import java.util.Objects;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneFileStatusProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneFileStatusProto.Builder;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

/**
 * File Status of the Ozone Key.
 */
public class OzoneFileStatus {

  private static final long serialVersionUID = 1L;

  /**
   * The key info object for files. Leave null for the root directory.
   */
  private OmKeyInfo keyInfo;

  private boolean isDirectory;

  private long blockSize;

  public OzoneFileStatus() {
    isDirectory = true;
  }

  public OzoneFileStatus(OmKeyInfo keyInfo,
      long blockSize, boolean isDirectory) {
    this.keyInfo = keyInfo;
    this.isDirectory = isDirectory;
    this.blockSize = blockSize;
  }

  public OmKeyInfo getKeyInfo() {
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

  public OzoneFileStatusProto getProtobuf(int clientVersion) {
    Builder builder = OzoneFileStatusProto.newBuilder()
        .setBlockSize(blockSize)
        .setIsDirectory(isDirectory);
    //key info can be null for the fake root entry.
    if (keyInfo != null) {
      builder.setKeyInfo(keyInfo.getProtobuf(clientVersion));
    }
    return builder.build();
  }

  public static OzoneFileStatus getFromProtobuf(OzoneFileStatusProto status) {
    return new OzoneFileStatus(
        OmKeyInfo.getFromProtobuf(status.getKeyInfo()),
        status.getBlockSize(),
        status.getIsDirectory());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OzoneFileStatus)) {
      return false;
    }
    OzoneFileStatus that = (OzoneFileStatus) o;
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
    sb.append("{");
    if (keyInfo == null) {
      sb.append("<root>");
    } else {
      sb.append(getTrimmedName());
      if (isDirectory) {
        sb.append(" (dir)");
      }
    }
    sb.append("}");
    return sb.toString();
  }

}
