/**
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

package org.apache.hadoop.ipc_;

import org.apache.hadoop.security.UserGroupInformation;

/**
 * Interface which allows extracting information necessary to
 * create schedulable identity strings.
 */
public interface Schedulable {
  public UserGroupInformation getUserGroupInformation();

  /**
   * This is overridden only in {@link Server.Call}.
   * The CallerContext field will be used to carry information
   * about the user in cases where UGI proves insufficient.
   * Any other classes that might try to use this method,
   * will get an UnsupportedOperationException.
   *
   * @return an instance of CallerContext if method
   * is overridden else get an UnsupportedOperationException
   */
  default CallerContext getCallerContext() {
    throw new UnsupportedOperationException("Invalid operation.");
  }

  int getPriorityLevel();
}
