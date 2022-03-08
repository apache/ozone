/*
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
package org.apache.hadoop.hdds;

public interface ComponentVersion {

  int version();

  String description();

  /**
   * Method to compare the actual component version with the one provided by
   * the other side of the communication.
   * @param actual the actual component version of the server runtime
   *               (NOTE: this is not the server's but the other party's
   *               version number in the current runtime!)
   * @param other the provided component version of the other component
   * @return a negative value if the actual version is greater, 0 if the
   *         two versions are equal, a positive value if the other version is
   *         greater.
   * @throws ClassCastException if the two provided component version instance
   *                            are not instances of the same class.
   */
  default int compare(ComponentVersion actual, ComponentVersion other) {
    if (actual.getClass() != other.getClass()) {
      throw new ClassCastException();
    }
    return (other.version() & Integer.MAX_VALUE) - actual.version();
  }
}
