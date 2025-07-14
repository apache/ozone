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

import java.util.ArrayList;
import java.util.List;

/**
 * Client side representation of DBUpdates.
 */
public class DBUpdates {

  private List<byte[]> dataList = new ArrayList<>();

  private long currentSequenceNumber = -1;

  private long latestSequenceNumber = -1L;

  private boolean isDBUpdateSuccess = true;

  public DBUpdates() {
    this.dataList = new ArrayList<>();
  }

  public DBUpdates(List<byte[]> data) {
    this.dataList = new ArrayList<>(data);
  }

  public void addWriteBatch(byte[] data, long sequenceNumber) {
    dataList.add(data);
    if (currentSequenceNumber < sequenceNumber) {
      currentSequenceNumber = sequenceNumber;
    }
  }

  public List<byte[]> getData() {
    return dataList;
  }

  public void setCurrentSequenceNumber(long sequenceNumber) {
    this.currentSequenceNumber = sequenceNumber;
  }

  public long getCurrentSequenceNumber() {
    return currentSequenceNumber;
  }

  public void setLatestSequenceNumber(long sequenceNumber) {
    this.latestSequenceNumber = sequenceNumber;
  }

  public long getLatestSequenceNumber() {
    return latestSequenceNumber;
  }

  public boolean isDBUpdateSuccess() {
    return isDBUpdateSuccess;
  }

  public void setDBUpdateSuccess(boolean dbUpdateSuccess) {
    this.isDBUpdateSuccess = dbUpdateSuccess;
  }
}
