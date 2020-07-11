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
package org.apache.hadoop.ozone.protocol.commands;

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMStopDataNodeResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMStopDataNodeResponseProto.ErrorCode;


/**
 * Response to Stop Datanode call.
 */
public class StopDataNodeCommand {
  private ErrorCode error;

  public StopDataNodeCommand(final ErrorCode error) {
    this.error = error;
  }

  /**
   * Returns a new builder.
   *
   * @return - Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Returns ErrorCode.
   *
   * @return - ErrorCode
   */
  public ErrorCode getError() {
    return error;
  }

  /**
   * Gets the protobuf message of this object.
   *
   * @return A protobuf message.
   */
  public SCMStopDataNodeResponseProto getProtoBufMessage() {
    SCMStopDataNodeResponseProto.Builder builder = SCMStopDataNodeResponseProto
        .newBuilder().setErrorCode(this.error);
    return builder.build();
  }

  /**
   * A builder class to verify all values are sane.
   */
  public static class Builder {
    private ErrorCode error;

    /**
     * Sets Error code.
     *
     * @param errorCode - error code
     * @return Builder
     */
    public Builder setErrorCode(ErrorCode errorCode) {
      this.error = errorCode;
      return this;
    }

    /**
     * Build the command object.
     *
     * @return StopDataNodeCommand
     */
    public StopDataNodeCommand build() {
      return new StopDataNodeCommand(this.error);
    }
  }
}
