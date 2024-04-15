/*
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
package org.apache.hadoop.ozone.recon.api.types;

/**
 * Wrapper object for statistics of records of a page in API response.
 */
public class Stats {
  /**
   * Total count of the keys.
   */
  private long totalCount;

  /** last key sent. */
  private String lastKey;

  /**
   * limit the number of records to return in API response.
   */
  private long limit;

  /**
   * counter to track the number of records added in API response.
   */
  private long currentCount;

  public Stats(long limit) {
    this.limit = limit;
  }

  public long getTotalCount() {
    return totalCount;
  }

  public void setTotalCount(long totalCount) {
    this.totalCount = totalCount;
  }

  public String getLastKey() {
    return lastKey;
  }

  public void setLastKey(String lastKey) {
    this.lastKey = lastKey;
  }

  public long getLimit() {
    return limit;
  }

  public void setLimit(long limit) {
    this.limit = limit;
  }

  public long getCurrentCount() {
    return currentCount;
  }

  public void setCurrentCount(long currentCount) {
    this.currentCount = currentCount;
  }
}
