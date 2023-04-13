/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.recon.api.types;

/**
 * Last X units of time used to get Audit logs.
 */
public enum LastXUnit {
  TWENTY_FOUR_HOUR("Last 24 hours", "24H"),
  SEVEN_DAYS("Last 7 days", "7D"),
  NINETY_DAYS("Last 90 days", "90D");

  private final String description;

  private final String value;

  LastXUnit(String description, String value) {
    this.description = description;
    this.value = value;
  }

  public static LastXUnit getType(String value) {
    for (LastXUnit lastXUnit : LastXUnit.values()) {
      if (value.equalsIgnoreCase(lastXUnit.getValue())) {
        return lastXUnit;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return description;
  }

  public String getValue() {
    return value;
  }
}
