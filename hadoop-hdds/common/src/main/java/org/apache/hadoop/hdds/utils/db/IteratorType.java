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
  NEITHER,
  /**
   * Read key only.
   */
  KEY_ONLY,
  /**
   * Read value only.
   */
  VALUE_ONLY,
  /**
   * Read both key and value.
   */
  KEY_AND_VALUE;

  public boolean readKey() {
    return (this.ordinal() & KEY_ONLY.ordinal()) != 0;
  }

  public boolean readValue() {
    return (this.ordinal() & VALUE_ONLY.ordinal()) != 0;
  }
}
