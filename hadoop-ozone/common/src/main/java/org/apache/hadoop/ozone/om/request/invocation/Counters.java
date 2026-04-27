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

package org.apache.hadoop.ozone.om.request.invocation;

class Counters {
  /**
   * Counter for retries.
   */
  private int retries;
  /**
   * Counter for method invocation has been failed over.
   */
  private int failovers;

  boolean isZeros() {
    return retries == 0 && failovers == 0;
  }

  public int getRetries() {
    return retries;
  }

  public int getFailovers() {
    return failovers;
  }

  public void incRetries() {
    retries++;
  }

  public void incFailovers() {
    failovers++;
  }

}
