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

package org.apache.hadoop.ozone.debug.logs.container.utils;

/**
 *Holds information about a container.
 */

public final class DatanodeContainerInfo {

  private final long containerId;
  private final String datanodeId;
  private final String timestamp;
  private final String state;
  private final long bcsid;
  private final String errorMessage;
  private final String logLevel;
  private final int indexValue;

  private DatanodeContainerInfo(Builder builder) {
    this.containerId = builder.containerId;
    this.datanodeId = builder.datanodeId;
    this.timestamp = builder.timestamp;
    this.state = builder.state;
    this.bcsid = builder.bcsid;
    this.errorMessage = builder.errorMessage;
    this.logLevel = builder.logLevel;
    this.indexValue = builder.indexValue;
  }

  /**
   * Builder for DatanodeContainerInfo.
   */

  public static class Builder {
    private long containerId;
    private String datanodeId;
    private String timestamp;
    private String state;
    private long bcsid;
    private String errorMessage;
    private String logLevel;
    private int indexValue;

    public Builder setContainerId(long containerId) {
      this.containerId = containerId;
      return this;
    }

    public Builder setDatanodeId(String datanodeId) {
      this.datanodeId = datanodeId;
      return this;
    }

    public Builder setTimestamp(String timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Builder setState(String state) {
      this.state = state;
      return this;
    }

    public Builder setBcsid(long bcsid) {
      this.bcsid = bcsid;
      return this;
    }

    public Builder setErrorMessage(String errorMessage) {
      this.errorMessage = errorMessage;
      return this;
    }

    public Builder setLogLevel(String logLevel) {
      this.logLevel = logLevel;
      return this;
    }

    public Builder setIndexValue(int indexValue) {
      this.indexValue = indexValue;
      return this;
    }

    public DatanodeContainerInfo build() {
      return new DatanodeContainerInfo(this);
    }
  }

  public long getContainerId() {
    return containerId;
  }

  public String getDatanodeId() {
    return datanodeId;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public String getState() {
    return state;
  }

  public long getBcsid() {
    return bcsid;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public String getLogLevel() {
    return logLevel;
  }

  public int getIndexValue() {
    return indexValue;
  }
}

