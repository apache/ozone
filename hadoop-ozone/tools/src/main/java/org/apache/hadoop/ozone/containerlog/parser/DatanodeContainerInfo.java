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

package org.apache.hadoop.ozone.containerlog.parser;

/**
 *Holds information about a container.
 */

public class DatanodeContainerInfo {

  private String timestamp;
  private String state;
  private long bcsid;
  private String errorMessage;
  private String logLevel;
  private int indexValue;

  public DatanodeContainerInfo() {
  }
  public DatanodeContainerInfo(String timestamp, String state, long bcsid, String errorMessage,
                               String logLevel, int indexValue) {
    this.timestamp = timestamp;
    this.state = state;
    this.bcsid = bcsid;
    this.errorMessage = errorMessage;
    this.logLevel = logLevel;
    this.indexValue = indexValue;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public long getBcsid() {
    return bcsid;
  }

  public void setBcsid(long bcsid) {
    this.bcsid = bcsid;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public String getLogLevel() {
    return logLevel;
  }

  public void setLogLevel(String logLevel) {
    this.logLevel = logLevel;
  }

  public int getIndexValue() {
    return indexValue;
  }

  public void setIndexValue(int indexValue) {
    this.indexValue = indexValue;
  }

}
