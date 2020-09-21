/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.cache;

/**
 * Type like : LRU, LRU2(multi-level), MFU etc..
 */
public enum CachePolicy {

  DIR_LRU("DIR_LRU"),

  DIR_NOCACHE("DIR_NOCACHE"); // disable dir cache

  CachePolicy(String cachePolicy) {
    this.policy = cachePolicy;
  }

  private String policy;

  public String getPolicy() {
    return policy;
  }

  public static CachePolicy getPolicy(String policyStr) {
    for (CachePolicy policy : CachePolicy.values()) {
      if (policyStr.equalsIgnoreCase(policy.getPolicy())) {
        return policy;
      }
    }
    // defaulting to NO_CACHE
    return DIR_NOCACHE;
  }
}
