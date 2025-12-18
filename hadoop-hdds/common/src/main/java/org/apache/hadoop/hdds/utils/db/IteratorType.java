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

package org.apache.hadoop.hdds.utils.db;

/**
 * The iterator type.
 */
public enum IteratorType {

  /**
   * Neither read key nor value.
   */
  NEITHER(0),
  /**
   * Read key only.
   */
  KEY_ONLY(1),
  /**
   * Read value only.
   */
  VALUE_ONLY(2),
  /**
   * Read both key and value.
   */
  KEY_AND_VALUE(3);

  private final int mask;

  IteratorType(int mask) {
    this.mask = mask;
  }

  public boolean readKey() {
    return (this.mask & KEY_ONLY.mask) != 0;
  }

  public boolean readValue() {
    return (this.mask & VALUE_ONLY.mask) != 0;
  }
}
