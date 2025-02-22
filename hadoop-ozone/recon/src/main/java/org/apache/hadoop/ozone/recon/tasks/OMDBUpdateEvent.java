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

package org.apache.hadoop.ozone.recon.tasks;

import java.util.Objects;

/**
 * A class used to encapsulate a single OM DB update event.
 * Currently PUT, DELETE and UPDATE are supported.
 * @param <KEY> Type of Key.
 * @param <VALUE> Type of Value.
 */
public final class OMDBUpdateEvent<KEY, VALUE> {

  private final OMDBUpdateAction action;
  private final String table;
  private final KEY updatedKey;
  private final VALUE updatedValue;
  private final VALUE oldValue;
  private final long sequenceNumber;

  private OMDBUpdateEvent(OMDBUpdateAction action,
                          String table,
                          KEY updatedKey,
                          VALUE updatedValue,
                          VALUE oldValue,
                          long sequenceNumber) {
    this.action = action;
    this.table = table;
    this.updatedKey = updatedKey;
    this.updatedValue = updatedValue;
    this.oldValue = oldValue;
    this.sequenceNumber = sequenceNumber;
  }

  public OMDBUpdateAction getAction() {
    return action;
  }

  public String getTable() {
    return table;
  }

  public KEY getKey() {
    return updatedKey;
  }

  public VALUE getValue() {
    return updatedValue;
  }

  public VALUE getOldValue() {
    return oldValue;
  }

  public long getSequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OMDBUpdateEvent that = (OMDBUpdateEvent) o;
    return this.updatedKey.equals(that.updatedKey) &&
        this.table.equals(that.table) &&
        this.action.equals(that.action);
  }

  @Override
  public int hashCode() {
    return Objects.hash(updatedKey, table, action);
  }

  /**
   * Builder used to construct an OM DB Update event.
   * @param <KEY> Key type.
   * @param <VALUE> Value type.
   */
  public static class OMUpdateEventBuilder<KEY, VALUE> {

    private OMDBUpdateAction action;
    private String table;
    private KEY updatedKey;
    private VALUE oldValue;
    private VALUE updatedValue;
    private long lastSequenceNumber;

    OMUpdateEventBuilder setAction(OMDBUpdateAction omdbUpdateAction) {
      this.action = omdbUpdateAction;
      return this;
    }

    OMUpdateEventBuilder setTable(String tableName) {
      this.table = tableName;
      return this;
    }

    OMUpdateEventBuilder setKey(KEY key) {
      this.updatedKey = key;
      return this;
    }

    OMUpdateEventBuilder setValue(VALUE value) {
      this.updatedValue = value;
      return this;
    }

    OMUpdateEventBuilder setOldValue(VALUE value) {
      this.oldValue = value;
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
      return new OMDBUpdateEvent<KEY, VALUE>(
          action,
          table,
          updatedKey,
          updatedValue,
          oldValue,
          lastSequenceNumber);
    }
  }

  /**
   * Supported Actions - PUT, DELETE.
   */
  public enum OMDBUpdateAction {
    PUT, DELETE, UPDATE
  }
}
