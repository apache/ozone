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

package org.apache.hadoop.ozone.recon.tasks;

/**
 * A class used to encapsulate a single OM DB update event.
 * Currently only PUT and DELETE are supported.
 * @param <K> Type of Key.
 * @param <V> Type of Value.
 */
public final class OMDBUpdateEvent<K, V> {

  private final OMDBUpdateAction action;
  private final String table;
  private final K updatedKey;
  private final V updatedValue;
  private final long sequenceNumber;

  private OMDBUpdateEvent(OMDBUpdateAction action,
                          String table,
                          K updatedKey,
                          V updatedValue,
                          long sequenceNumber) {
    this.action = action;
    this.table = table;
    this.updatedKey = updatedKey;
    this.updatedValue = updatedValue;
    this.sequenceNumber = sequenceNumber;
  }

  public OMDBUpdateAction getAction() {
    return action;
  }

  public String getTable() {
    return table;
  }

  public K getKey() {
    return updatedKey;
  }

  public V getValue() {
    return updatedValue;
  }

  public long getSequenceNumber() {
    return sequenceNumber;
  }

  /**
   * Builder used to construct an OM DB Update event.
   * @param <K1> Key type.
   * @param <V1> Value type.
   */
  public static class OMUpdateEventBuilder<K1, V1> {

    private OMDBUpdateAction action;
    private String table;
    private K1 updatedKey;
    private V1 updatedValue;
    private long lastSequenceNumber;

    OMUpdateEventBuilder setAction(OMDBUpdateAction omdbUpdateAction) {
      this.action = omdbUpdateAction;
      return this;
    }

    OMUpdateEventBuilder setTable(String tableName) {
      this.table = tableName;
      return this;
    }

    OMUpdateEventBuilder setKey(K1 key) {
      this.updatedKey = key;
      return this;
    }

    OMUpdateEventBuilder setValue(V1 value) {
      this.updatedValue = value;
      return this;
    }

    OMUpdateEventBuilder setSequenceNumber(long sequenceNumber) {
      this.lastSequenceNumber = sequenceNumber;
      return this;
    }

    /**
     * Build an OM update event.
     * @return OMDBUpdateEvent
     */
    public OMDBUpdateEvent build() {
      return new OMDBUpdateEvent<K1, V1>(
          action,
          table,
          updatedKey,
          updatedValue,
          lastSequenceNumber);
    }
  }

  /**
   * Supported Actions - PUT, DELETE.
   */
  public enum OMDBUpdateAction {
    PUT, DELETE
  }
}
