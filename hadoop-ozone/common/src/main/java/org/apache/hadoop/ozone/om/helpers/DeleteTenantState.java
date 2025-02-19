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

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteTenantResponse;

/**
 * A class that encapsulates DeleteTenantResponse protobuf message.
 */
public class DeleteTenantState {

  /**
   * Volume name associated to the deleted tenant.
   */
  private final String volumeName;

  /**
   * Reference count remaining of the volume associated to the deleted tenant.
   */
  private final long volRefCount;

  public DeleteTenantState(String volumeName, long volRefCount) {
    this.volumeName = volumeName;
    this.volRefCount = volRefCount;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public long getVolRefCount() {
    return volRefCount;
  }

  public static DeleteTenantState fromProtobuf(DeleteTenantResponse resp) {
    return new DeleteTenantState(resp.getVolumeName(), resp.getVolRefCount());
  }

  public DeleteTenantResponse getProtobuf() {
    return DeleteTenantResponse.newBuilder()
        .setVolumeName(volumeName)
        .setVolRefCount(volRefCount)
        .build();
  }

  public static DeleteTenantState.Builder newBuilder() {
    return new DeleteTenantState.Builder();
  }

  /**
   * Builder for TenantDeleted.
   */
  public static final class Builder {
    private String volumeName;
    private long volRefCount;

    private Builder() {
    }

    public Builder setVolumeName(String volumeName) {
      this.volumeName = volumeName;
      return this;
    }

    public Builder setVolRefCount(long volRefCount) {
      this.volRefCount = volRefCount;
      return this;
    }

    public DeleteTenantState build() {
      return new DeleteTenantState(volumeName, volRefCount);
    }
  }
}
